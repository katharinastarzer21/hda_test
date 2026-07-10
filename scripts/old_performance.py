import gevent.monkey
gevent.monkey.patch_all()

import os, time, logging, gevent
from locust import HttpUser, task, between
from locust.env import Environment
from locust.log import setup_logging
from otel_push import record, flush

HDA_URL    = os.environ.get("HDA_URL", "https://data.eodc.eu").rstrip("/")
VU_STAGES  = [5, 10, 25]
STAGE_SECS = 60

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)


class HdaUser(HttpUser):
    host = HDA_URL
    wait_time = between(3, 8)

    @task
    def get_zarr(self):
        self.client.get(
            "/collections/S2-L2A-C1/T33UWP/indices/time/.zarray",
            name="GET_zarray",
        )

    @task
    def get_geotiff(self):
        self.client.head(
            "/collections/SENTINEL1_SIG0_20M/V1M2R3/EQUI7_EU020M/E048N015T3"
            "/SIG0_20260412T171426__VV_A015_E048N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif",
            name="HEAD_tif",
        )


def push_metrics(all_stages):
    baseline = all_stages.get(VU_STAGES[0], {})
    now      = time.time()

    for vu_count, stats in all_stages.items():
        for endpoint, s in stats.items():
            ratio = s["p95"] / baseline.get(endpoint, {}).get("p95", s["p95"]) if baseline.get(endpoint, {}).get("p95") else 1.0
            record(
                {"eodc_e2e_perf_p95_seconds":        s["p95"],
                 "eodc_e2e_perf_p50_seconds":        s["p50"],
                 "eodc_e2e_perf_rps":                s["rps"],
                 "eodc_e2e_perf_error_rate":         s["err"],
                 "eodc_e2e_perf_vus":                float(vu_count),
                 "eodc_e2e_perf_slowdown_ratio":     ratio,
                 "eodc_e2e_perf_last_run_timestamp": now},
                {"service": "hda", "endpoint": endpoint, "vus": str(vu_count)},
            )
            log.info("staged  vu=%2d  endpoint=%-20s  p50=%.3fs  p95=%.3fs  rps=%.1f  slowdown=%.2fx",
                     vu_count, endpoint, s["p50"], s["p95"], s["rps"], ratio)

    flush()


def main():
    setup_logging("INFO")
    env = Environment(user_classes=[HdaUser])
    env.create_local_runner()

    all_stages = {}
    for vu_count in VU_STAGES:
        log.info("Stage %d VUs — %ds", vu_count, STAGE_SECS)
        env.stats.reset_all()
        env.runner.start(vu_count, spawn_rate=10)
        gevent.sleep(STAGE_SECS)
        env.runner.stop()
        gevent.sleep(1)

        all_stages[vu_count] = {
            name: {"p95": (entry.get_response_time_percentile(0.95) or 0) / 1000,
                   "p50": (entry.get_response_time_percentile(0.50) or 0) / 1000,
                   "rps": entry.total_rps,
                   "err": entry.fail_ratio}
            for (_, name), entry in env.stats.entries.items()
            if name not in ("", "Aggregated")
        }
        for name, s in all_stages[vu_count].items():
            log.info("  %-20s  p50=%.3fs  p95=%.3fs  rps=%.1f  err=%.1f%%",
                     name, s["p50"], s["p95"], s["rps"], s["err"] * 100)

    push_metrics(all_stages)
    env.runner.quit()
    log.info("Done.")


if __name__ == "__main__":
    main()