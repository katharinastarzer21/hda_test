import gevent.monkey
gevent.monkey.patch_all()

import os, json, random, time, logging, gevent
from locust import HttpUser, task, between
from locust.env import Environment
from locust.log import setup_logging
from otel_push import record, flush

HDA_URL = os.environ.get("HDA_URL", "https://dev.hda.eodchosting.eu").rstrip("/")

ZARR_PATH = "/collections/S2-L2A-C1/T33UWP/indices/time/.zarray"
TIF_PATH  = ("/collections/SENTINEL1_SIG0_20M/V1M2R3/EQUI7_EU020M/E048N015T3"
             "/SIG0_20260412T171426__VV_A015_E048N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif")

# separate, known-size (~257 MB) file for the full-object download task — full
# GETs on TIF_PATH would also work, but its size is unconfirmed and this task
# is heavy enough already that we want a number to reason about bandwidth with.
FULL_TIF_PATH = os.environ.get(
    "FULL_TIF_PATH",
    "/collections/sen2like/2025/06/11/EU010M_E049N015T1_20250611T101041_B04.tif",
)

# pool of large files for the full-download task, one picked at random per
# request — simulates many different users pulling different big objects
# instead of everyone hammering the exact same (possibly cached) file.
# TIF_PATH's real size is unconfirmed; treat it with the same caution as
# FULL_TIF_PATH's confirmed ~257 MB until it's checked.
FULL_DOWNLOAD_PATHS = [
    p.strip() for p in os.environ.get(
        "FULL_DOWNLOAD_PATHS", f"{FULL_TIF_PATH},{TIF_PATH}"
    ).split(",") if p.strip()
]

# stage ramp-up config, all overridable via env vars for quick local test runs
VU_START     = int(os.environ.get("VU_START", 5))
VU_STEP      = int(os.environ.get("VU_STEP", 10))
VU_MAX       = int(os.environ.get("VU_MAX", 200))
SPAWN_RATE   = int(os.environ.get("SPAWN_RATE", 10))
STAGE_SECS   = int(os.environ.get("STAGE_SECS", 90))
WARMUP_SECS  = int(os.environ.get("WARMUP_SECS", 15))  # excluded from stats, connection setup etc.

# actual data transfer sizes for the GeoTIFF read tasks, not just headers.
# tile-sized = one COG overview/tile read; chunk-sized = a larger windowed pull.
RANGE_TILE_BYTES  = int(os.environ.get("RANGE_TILE_BYTES", 256 * 1024))
RANGE_CHUNK_BYTES = int(os.environ.get("RANGE_CHUNK_BYTES", 4 * 1024 * 1024))

# breakpoint = one of these breached, N stages in a row (to filter noise)
ERROR_RATE_THRESHOLD = float(os.environ.get("ERROR_RATE_THRESHOLD", 0.05))
P95_THRESHOLD_SECS   = float(os.environ.get("P95_THRESHOLD_SECS", 5.0))
BREACH_STAGES_TO_CONFIRM = int(os.environ.get("BREACH_STAGES_TO_CONFIRM", 2))

# a stage whose aggregate RPS falls this far below the best RPS seen so far
# is a saturation signal in its own right — err rate and p95 can both still
# look fine while the server is already doing less total work under more load.
THROUGHPUT_DROP_THRESHOLD = float(os.environ.get("THROUGHPUT_DROP_THRESHOLD", 0.15))

RESULTS_JSON = os.environ.get("RESULTS_JSON", "perf_results.json")

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)


class HdaUser(HttpUser):
    host = HDA_URL
    # was between(3, 8): each VU barely made a request a minute, so raising VU
    # count mostly added idle users instead of concurrent load. Shrinking this
    # is what actually makes VUs hammer the target simultaneously.
    wait_time = between(0.1, 0.5)

    @task(2)
    def get_zarr(self):
        self.client.get(ZARR_PATH, name="GET_zarray")

    @task(1)
    def head_geotiff(self):
        # cheap header-only check, kept for comparison against the real reads below
        self.client.head(TIF_PATH, name="HEAD_tif")

    @task(3)
    def get_geotiff_tile(self):
        # single COG tile/overview read — the dominant real-world access pattern
        self.client.get(
            TIF_PATH,
            headers={"Range": f"bytes=0-{RANGE_TILE_BYTES - 1}"},
            name="GET_tif_range_tile",
        )

    @task(1)
    def get_geotiff_chunk(self):
        # larger windowed pull, to see actual throughput degrade under load
        self.client.get(
            TIF_PATH,
            headers={"Range": f"bytes=0-{RANGE_CHUNK_BYTES - 1}"},
            name="GET_tif_range_chunk",
        )

    @task(1)
    def get_geotiff_full(self):
        # no Range header at all — a real full-object download, picking a
        # different file per request so this models many different users
        # pulling different big objects rather than everyone hammering one
        # (possibly cached) file. Weight is 1 of 8 total, so roughly VU_count/8
        # of these can be in flight at once at the top of the ramp: at
        # VU_MAX=295 that's ~37 concurrent 250+ MB downloads at a time.
        self.client.get(random.choice(FULL_DOWNLOAD_PATHS), name="GET_tif_full")


def collect_stage_stats(env):
    # pull current numbers out of locust's stats entries, one dict per endpoint
    stats = {}
    for (name, _method), entry in env.stats.entries.items():
        if name in ("", "Aggregated"):
            continue
        error_types = {
            f"{err.method} {err.name}: {err.error}"[:120]: err.occurrences
            for err in env.stats.errors.values()
            if err.name == name
        }
        rps = entry.total_rps
        avg_size = entry.avg_content_length or 0
        stats[name] = {
            "p50": (entry.get_response_time_percentile(0.50) or 0) / 1000,
            "p90": (entry.get_response_time_percentile(0.90) or 0) / 1000,
            "p95": (entry.get_response_time_percentile(0.95) or 0) / 1000,
            "p99": (entry.get_response_time_percentile(0.99) or 0) / 1000,
            "rps": rps,
            "err": entry.fail_ratio,
            "num_requests": entry.num_requests,
            "num_failures": entry.num_failures,
            "throughput_mbps": (rps * avg_size) / 1_000_000,
            "error_types": error_types,
        }
    return stats


def stage_breaches(stage_stats, total_rps, peak_total_rps, peak_total_rps_vu):
    # returns list of reasons if this stage broke a threshold, empty list if all good
    reasons = []
    for endpoint, s in stage_stats.items():
        if s["err"] > ERROR_RATE_THRESHOLD:
            reasons.append(f"{endpoint}: error_rate={s['err']:.1%} > {ERROR_RATE_THRESHOLD:.0%}")
        if s["p95"] > P95_THRESHOLD_SECS:
            reasons.append(f"{endpoint}: p95={s['p95']:.2f}s > {P95_THRESHOLD_SECS}s")

    # peak_total_rps is 0 until a first stage has been recorded, nothing to compare yet
    if peak_total_rps > 0:
        drop_ratio = (peak_total_rps - total_rps) / peak_total_rps
        if drop_ratio > THROUGHPUT_DROP_THRESHOLD:
            reasons.append(
                f"throughput regression: total_rps={total_rps:.1f} is {drop_ratio:.0%} "
                f"below peak={peak_total_rps:.1f} rps (seen at {peak_total_rps_vu} VUs)"
            )
    return reasons


def push_metrics(all_stages, breakpoint_info):
    baseline = all_stages.get(VU_START, {})
    now = time.time()
    for vu_count, stats in all_stages.items():
        for endpoint, s in stats.items():
            base_p95 = baseline.get(endpoint, {}).get("p95")
            ratio = (s["p95"] / base_p95) if base_p95 else 1.0
            record(
                {
                    "eodc_e2e_perf_p50_seconds": s["p50"],
                    "eodc_e2e_perf_p95_seconds": s["p95"],
                    "eodc_e2e_perf_p99_seconds": s["p99"],
                    "eodc_e2e_perf_rps": s["rps"],
                    "eodc_e2e_perf_throughput_mbps": s["throughput_mbps"],
                    "eodc_e2e_perf_error_rate": s["err"],
                    "eodc_e2e_perf_vus": float(vu_count),
                    "eodc_e2e_perf_slowdown_ratio": ratio,
                    "eodc_e2e_perf_last_run_timestamp": now,
                },
                {"service": "hda", "endpoint": endpoint, "vus": str(vu_count)},
            )
    if breakpoint_info:
        record({"eodc_e2e_perf_breakpoint_vus": float(breakpoint_info["vus"])}, {"service": "hda"})
    flush()


def write_results(all_stages, stage_meta, breakpoint_info):
    # called after every stage, not just at the end, so a mid-ramp crash
    # (either the target service or the runner) still leaves the stages
    # measured so far on disk instead of losing the whole run.
    with open(RESULTS_JSON, "w") as f:
        json.dump(
            {
                "target": HDA_URL,
                "timestamp": time.time(),
                "config": {
                    "vu_start": VU_START,
                    "vu_step": VU_STEP,
                    "vu_max": VU_MAX,
                    "stage_secs": STAGE_SECS,
                    "warmup_secs": WARMUP_SECS,
                    "error_rate_threshold": ERROR_RATE_THRESHOLD,
                    "p95_threshold_secs": P95_THRESHOLD_SECS,
                },
                "breakpoint": breakpoint_info,
                "stages": all_stages,
                "stage_meta": stage_meta,
            },
            f,
            indent=2,
        )


def main():
    setup_logging("INFO")
    env = Environment(user_classes=[HdaUser])
    env.create_local_runner()

    all_stages = {}
    stage_meta = {}
    breach_streak = 0
    breakpoint_info = None
    vu_count = VU_START
    peak_total_rps = 0.0
    peak_total_rps_vu = None

    while vu_count <= VU_MAX:
        log.info("Stage %d VUs — %ds (%ds warmup excluded)", vu_count, STAGE_SECS, WARMUP_SECS)
        stage_started_at = time.time()
        env.runner.start(vu_count, spawn_rate=SPAWN_RATE)

        gevent.sleep(WARMUP_SECS)
        env.stats.reset_all()  # drop warmup requests, start measuring clean
        measure_started_at = time.time()

        gevent.sleep(max(STAGE_SECS - WARMUP_SECS, 1))
        env.runner.stop()
        measured_secs = time.time() - measure_started_at
        gevent.sleep(1)

        stage_stats = collect_stage_stats(env)
        all_stages[vu_count] = stage_stats
        total_rps = sum(s["rps"] for s in stage_stats.values())
        stage_meta[vu_count] = {
            "measured_secs": measured_secs,
            "wall_secs": time.time() - stage_started_at,
            "total_requests": sum(s["num_requests"] for s in stage_stats.values()),
            "total_failures": sum(s["num_failures"] for s in stage_stats.values()),
            "total_rps": total_rps,
        }

        log.info(
            "  stage took %.1fs measured (%.1fs incl. warmup+spawn) — %d requests, %d failures, %.1f total rps",
            stage_meta[vu_count]["measured_secs"], stage_meta[vu_count]["wall_secs"],
            stage_meta[vu_count]["total_requests"], stage_meta[vu_count]["total_failures"], total_rps,
        )
        for name, s in stage_stats.items():
            log.info(
                "  %-20s  p50=%.3fs  p95=%.3fs  p99=%.3fs  rps=%.1f  thrpt=%.2fMB/s  err=%.1f%%",
                name, s["p50"], s["p95"], s["p99"], s["rps"], s["throughput_mbps"], s["err"] * 100,
            )

        reasons = stage_breaches(stage_stats, total_rps, peak_total_rps, peak_total_rps_vu)
        if total_rps > peak_total_rps:
            peak_total_rps = total_rps
            peak_total_rps_vu = vu_count
        if reasons:
            breach_streak += 1
            log.warning("Stage %d VUs breached thresholds (%d/%d): %s",
                        vu_count, breach_streak, BREACH_STAGES_TO_CONFIRM, "; ".join(reasons))
        else:
            breach_streak = 0

        if breach_streak >= BREACH_STAGES_TO_CONFIRM and breakpoint_info is None:
            first_breach_vu = vu_count - VU_STEP * (BREACH_STAGES_TO_CONFIRM - 1)
            breakpoint_info = {
                "vus": first_breach_vu,
                "confirmed_at_vus": vu_count,
                "reasons": reasons,
            }
            log.error("BREAKPOINT confirmed at %d VUs (confirmed at %d VUs): %s",
                       first_breach_vu, vu_count, "; ".join(reasons))
            write_results(all_stages, stage_meta, breakpoint_info)
            break

        write_results(all_stages, stage_meta, breakpoint_info)
        vu_count += VU_STEP

    push_metrics(all_stages, breakpoint_info)
    log.info("Results written to %s", RESULTS_JSON)

    env.runner.quit()
    log.info("Done.")


if __name__ == "__main__":
    main()
