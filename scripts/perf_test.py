import gevent.monkey
gevent.monkey.patch_all()

import os, json, random, resource, time, logging, gevent
from urllib.parse import urlparse
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

# pool of large files shared by every data-bearing task (HEAD, range tile,
# range chunk, full download) — one picked at random per request. Using a
# single fixed file per task type let a cache warm up after the first ~85 VUs
# worth of requests in one run (repeat requests kept hitting the exact same
# object), which masked real backend behavior for the rest of that ramp.
# Spreading requests across distinct objects keeps that from happening.
#
# The 59 paths below were discovered via the STAC API (eodag.dev.services.
# eodc.eu/collections -> per-collection /items), pulled from 15 different
# collections (cop_marine ocean models, NASA VIIRS), and every single one was
# verified with a real request (curl -sL, following redirects) to return
# HTTP 200 before being added here — of ~119 candidates pulled from the
# catalog, 31 were 404 (stale/broken hrefs), 17 were 202 "ingestion pending"
# (same async pattern as the NISAR file we skipped earlier), and 12 were 500
# (broken downloadLink routes); only working ones made this list. Sizes span
# ~73 KB to ~185 MB. TIF_PATH/FULL_TIF_PATH's sizes are separately confirmed
# (unconfirmed / ~257 MB respectively) and kept for continuity with earlier
# runs. Everything here redirects (302) to object storage on other hosts —
# requests follows that itself, including for Range/HEAD requests.
_STAC_DISCOVERED_PATHS = [
    '/collections/data/cop_marine/GLOBAL_ANALYSISFORECAST_BGC_001_028/GLOBAL_ANALYSIS_FORECAST_BIO_001_028_coordinates/native',
    '/collections/data/cop_marine/GLOBAL_ANALYSISFORECAST_BGC_001_028/GLOBAL_ANALYSIS_FORECAST_BIO_001_028_mask/native',
    '/collections/data/cop_marine/BALTICSEA_MULTIYEAR_PHY_003_011/BAL-MYP-NEMO_PHY-DailyMeans-19930105/native',
    '/collections/data/cop_marine/BALTICSEA_MULTIYEAR_PHY_003_011/BAL-MYP-NEMO_PHY-DailyMeans-19930104/native',
    '/collections/data/cop_marine/BALTICSEA_MULTIYEAR_PHY_003_011/BAL-MYP-NEMO_PHY-DailyMeans-19930106/native',
    '/collections/data/cop_marine/BALTICSEA_MULTIYEAR_PHY_003_011/BAL-MYP-NEMO_PHY-DailyMeans-19930102/native',
    '/collections/data/cop_marine/BALTICSEA_MULTIYEAR_PHY_003_011/BAL-MYP-NEMO_PHY-DailyMeans-19930101/native',
    '/collections/data/cop_marine/BALTICSEA_ANALYSISFORECAST_PHY_003_006/BAL-NEMO_PHY-MonthlyMeans-202411/native',
    '/collections/data/cop_marine/BALTICSEA_ANALYSISFORECAST_PHY_003_006/BAL-NEMO_PHY-MonthlyMeans-202410/native',
    '/collections/data/cop_marine/BALTICSEA_ANALYSISFORECAST_PHY_003_006/BAL-NEMO_PHY-MonthlyMeans-202406/native',
    '/collections/data/cop_marine/ARCTIC_ANALYSISFORECAST_PHY_002_001/20210709_hr-metno-MODEL-topaz5-ARC-b20210712-fv02.0/native',
    '/collections/data/cop_marine/ARCTIC_ANALYSISFORECAST_PHY_002_001/20210707_hr-metno-MODEL-topaz5-ARC-b20210712-fv02.0/native',
    '/collections/data/cop_marine/BALTICSEA_ANALYSISFORECAST_PHY_003_006/BAL-NEMO_PHY-MonthlyMeans-202408/native',
    '/collections/data/cop_marine/ARCTIC_ANALYSISFORECAST_PHY_002_001/20210705_hr-metno-MODEL-topaz5-ARC-b20210712-fv02.0/native',
    '/collections/data/cop_marine/ARCTIC_ANALYSISFORECAST_PHY_002_001/20210708_hr-metno-MODEL-topaz5-ARC-b20210712-fv02.0/native',
    '/collections/data/cop_marine/ARCTIC_ANALYSISFORECAST_PHY_002_001/20210706_hr-metno-MODEL-topaz5-ARC-b20210712-fv02.0/native',
    '/collections/data/cop_marine/BALTICSEA_MULTIYEAR_PHY_003_011/BAL-MYP-NEMO_PHY-DailyMeans-19930103/native',
    '/collections/data/cop_marine/ARCTIC_ANALYSISFORECAST_PHY_002_001/20210710_hr-metno-MODEL-topaz5-ARC-b20210712-fv02.0/native',
    '/collections/data/cop_marine/BALTICSEA_ANALYSISFORECAST_PHY_003_006/BAL-NEMO_PHY-MonthlyMeans-202409/native',
    '/collections/data/cop_marine/BALTICSEA_ANALYSISFORECAST_PHY_003_006/BAL-NEMO_PHY-MonthlyMeans-202407/native',
    '/collections/data/cop_marine/GLOBAL_ANALYSISFORECAST_BGC_001_028/mercatorbiomer4v2r1_global_mean_optics_20231130/native',
    '/collections/data/cop_marine/MEDSEA_ANALYSISFORECAST_PHY_006_013/MED-MFC_006_013_mask_bathy/native',
    '/collections/data/cop_marine/MEDSEA_ANALYSISFORECAST_PHY_006_013/MED-MFC_006_013_coordinates/native',
    '/collections/data/cop_marine/MEDSEA_ANALYSISFORECAST_PHY_006_013/MED-MFC_006_013_mdt/native',
    '/collections/data/cop_marine/MEDSEA_MULTIYEAR_PHY_006_004/MED-MFC_006_004_mdt/native',
    '/collections/data/cop_marine/GLOBAL_ANALYSISFORECAST_BGC_001_028/mercatorbiomer4v2r1_global_mean_optics_20231129/native',
    '/collections/data/cop_marine/MEDSEA_MULTIYEAR_PHY_006_004/MED-MFC_006_004_coordinates/native',
    '/collections/data/cop_marine/NWSHELF_ANALYSISFORECAST_PHY_004_013/metoffice_foam1_amm15_NWS_BED_b20240714_dm20240712/native',
    '/collections/data/cop_marine/NWSHELF_ANALYSISFORECAST_PHY_004_013/metoffice_foam1_amm15_NWS_BED_b20240716_dm20240714/native',
    '/collections/data/cop_marine/MEDSEA_MULTIYEAR_PHY_006_004/MED-MFC_006_004_mask_bathy/native',
    '/collections/data/cop_marine/NWSHELF_ANALYSISFORECAST_PHY_004_013/metoffice_foam1_amm15_NWS_BED_b20240715_dm20240713/native',
    '/collections/data/cop_marine/GLOBAL_MULTIYEAR_WAV_001_032/WAVERYSV1_bathymeter/native',
    '/collections/data/cop_marine/NWSHELF_MULTIYEAR_PHY_004_009/metoffice_foam1_amm7_NWS_BED_dm19930102/native',
    '/collections/data/cop_marine/NWSHELF_ANALYSISFORECAST_PHY_004_013/metoffice_foam1_amm15_NWS_BED_b20240717_dm20240715/native',
    '/collections/data/cop_marine/NWSHELF_ANALYSISFORECAST_PHY_004_013/metoffice_foam1_amm15_NWS_BED_b20240718_dm20240716/native',
    '/collections/data/cop_marine/NWSHELF_MULTIYEAR_PHY_004_009/metoffice_foam1_amm7_NWS_BED_dm19930104/native',
    '/collections/data/cop_marine/NWSHELF_MULTIYEAR_PHY_004_009/metoffice_foam1_amm7_NWS_BED_dm19930103/native',
    '/collections/data/cop_marine/NWSHELF_ANALYSISFORECAST_PHY_004_013/metoffice_foam1_amm15_NWS_BED_b20240719_dm20240717/native',
    '/collections/data/cop_marine/NWSHELF_MULTIYEAR_PHY_004_009/metoffice_foam1_amm7_NWS_BED_dm19930105/native',
    '/collections/data/cop_marine/NWSHELF_MULTIYEAR_PHY_004_009/metoffice_foam1_amm7_NWS_BED_dm19930106/native',
    '/collections/data/cop_marine/GLOBAL_ANALYSISFORECAST_BGC_001_028/mercatorbiomer4v2r1_global_mean_optics_20231202/native',
    '/collections/data/cop_marine/NWSHELF_MULTIYEAR_PHY_004_009/metoffice_foam1_amm7_NWS_BED_dm19930101/native',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026177.h06v03.002.2026185070600/VJ215A2H.A2026177.h06v03.002.2026185070600.h5',
    '/collections/data/cop_marine/MEDSEA_MULTIYEAR_PHY_006_004/19890101_y-CMCC--PSAL-MFSe3r1-MED-b20220901_re-sv01.00/native',
    '/collections/data/cop_marine/MEDSEA_MULTIYEAR_PHY_006_004/19880101_y-CMCC--PSAL-MFSe3r1-MED-b20220901_re-sv01.00/native',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026177.h00v10.002.2026185071658/VJ215A2H.A2026177.h00v10.002.2026185071658.h5',
    '/collections/data/cop_marine/GLOBAL_ANALYSISFORECAST_BGC_001_028/mercatorbiomer4v2r1_global_mean_optics_20231201/native',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026177.h12v07.002.2026185071750/VJ215A2H.A2026177.h12v07.002.2026185071750.h5',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026177.h03v07.002.2026185071751/VJ215A2H.A2026177.h03v07.002.2026185071751.h5',
    '/collections/data/cop_marine/MEDSEA_MULTIYEAR_PHY_006_004/19870101_y-CMCC--PSAL-MFSe3r1-MED-b20220901_re-sv01.00/native',
    '/collections/data/cop_marine/GLOBAL_MULTIYEAR_PHY_001_030/mercatorglorys12v1_gl12_mean_19930101_R19930106/native',
    '/collections/data/cop_marine/GLOBAL_MULTIYEAR_PHY_001_030/mercatorglorys12v1_gl12_mean_19930102_R19930106/native',
    '/collections/data/cop_marine/GLOBAL_MULTIYEAR_PHY_001_030/mercatorglorys12v1_gl12_mean_19930103_R19930106/native',
    '/collections/data/cop_marine/GLOBAL_MULTIYEAR_PHY_001_030/mercatorglorys12v1_gl12_mean_19930104_R19930106/native',
    '/collections/data/cop_marine/GLOBAL_MULTIYEAR_PHY_001_030/mercatorglorys12v1_gl12_mean_19930105_R19930106/native',
    '/collections/data/cop_marine/GLOBAL_MULTIYEAR_PHY_001_030/mercatorglorys12v1_gl12_mean_19930106_R19930113/native',
    '/collections/data/cop_marine/MEDSEA_ANALYSISFORECAST_PHY_006_013/20231101_d-CMCC--RFVL-MFSeas10-MEDATL-b20250531_an-sv11.00/native',
    '/collections/data/cop_marine/MEDSEA_ANALYSISFORECAST_PHY_006_013/20231103_d-CMCC--RFVL-MFSeas10-MEDATL-b20250531_an-sv11.00/native',
    '/collections/data/cop_marine/MEDSEA_ANALYSISFORECAST_PHY_006_013/20231102_d-CMCC--RFVL-MFSeas10-MEDATL-b20250531_an-sv11.00/native',
]
_DEFAULT_DATA_FILE_PATHS = ",".join([FULL_TIF_PATH, TIF_PATH] + _STAC_DISCOVERED_PATHS)
DATA_FILE_PATHS = [
    p.strip() for p in os.environ.get(
        "DATA_FILE_PATHS", _DEFAULT_DATA_FILE_PATHS
    ).split(",") if p.strip()
]

# stage ramp-up config, all overridable via env vars for quick local test runs
VU_START     = int(os.environ.get("VU_START", 5))
VU_STEP      = int(os.environ.get("VU_STEP", 10))
VU_MAX       = int(os.environ.get("VU_MAX", 200))
SPAWN_RATE   = int(os.environ.get("SPAWN_RATE", 10))
STAGE_SECS   = int(os.environ.get("STAGE_SECS", 90))
WARMUP_SECS  = int(os.environ.get("WARMUP_SECS", 15))  # excluded from stats, connection setup etc.

# soak-test mode: instead of ramping VU_START->VU_MAX in VU_STEP increments,
# re-test a fixed list of VU counts (e.g. the breakpoint region found by a
# previous ramp) for much longer per stage. 90s stages are enough to see a
# breakpoint during a ramp, but too short to tell stable saturation apart
# from a transient cache/connection-pool/backend blip at one point in time.
# e.g. SOAK_VUS=430,455,480,505,530,555 SOAK_STAGE_SECS=600
SOAK_VUS = [int(v.strip()) for v in os.environ.get("SOAK_VUS", "").split(",") if v.strip()]
SOAK_STAGE_SECS = int(os.environ.get("SOAK_STAGE_SECS", 600))

# actual data transfer sizes for the GeoTIFF read tasks, not just headers.
# tile-sized = one COG overview/tile read; chunk-sized = a larger windowed pull.
RANGE_TILE_BYTES  = int(os.environ.get("RANGE_TILE_BYTES", 256 * 1024))
RANGE_CHUNK_BYTES = int(os.environ.get("RANGE_CHUNK_BYTES", 4 * 1024 * 1024))

# breakpoint = one of these breached, N stages in a row (to filter noise).
# Only ERROR_RATE_THRESHOLD and THROUGHPUT_DROP_THRESHOLD actually stop the
# ramp — both are close to objective (real failures; real saturation, i.e.
# the server doing less total work despite more concurrent load). p95 latency
# climbs smoothly with load rather than cliff-breaking, so "breakpoint" from a
# fixed p95 bar was really just "wherever a continuously rising curve crosses
# a number nobody had signed off on as the actual acceptable limit" — that
# call belongs to whoever owns the SLA, not to this script. p95 is still
# measured, logged, and charted every stage; it's just not a stop condition.
ERROR_RATE_THRESHOLD = float(os.environ.get("ERROR_RATE_THRESHOLD", 0.05))
P95_THRESHOLD_SECS   = float(os.environ.get("P95_THRESHOLD_SECS", 5.0))
FULL_DOWNLOAD_P95_THRESHOLD_SECS = float(os.environ.get("FULL_DOWNLOAD_P95_THRESHOLD_SECS", 60.0))
BREACH_STAGES_TO_CONFIRM = int(os.environ.get("BREACH_STAGES_TO_CONFIRM", 2))

# a stage whose aggregate RPS falls this far below the best RPS seen so far
# is a saturation signal in its own right — err rate and p95 can both still
# look fine while the server is already doing less total work under more load.
THROUGHPUT_DROP_THRESHOLD = float(os.environ.get("THROUGHPUT_DROP_THRESHOLD", 0.15))

RESULTS_JSON = os.environ.get("RESULTS_JSON", "perf_results.json")

# "e2e" (default): the normal mixed task set below, following redirects all
# the way to S3 and downloading real bytes. "redirect_only": a separate user
# class that only measures how fast the origin (dev.hda.eodchosting.eu)
# issues its 302 and stops there — never touches S3. Run these as two
# SEPARATE invocations, not mixed into one: mixing them back into one task
# set would just reintroduce the same "shared weight dilutes the signal"
# problem that made task frequency (not file size) the real driver of the
# GET_tif_range_tile breakpoint. Confirmed via curl that ZARR_PATH is served
# directly (200, no redirect) — architecturally different from the
# DATA_FILE_PATHS pool, so it's excluded from the redirect_only check.
TASK_MODE = os.environ.get("TASK_MODE", "e2e")

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)

# structured failure detail for tracing whether errors originate at the
# origin (dev.hda.eodchosting.eu resolution) or downstream (S3, once
# redirected) — status codes and Locust's own aggregated error_types alone
# can't distinguish that, since both show up as the same "404" either way.
# Capped so a bad run with many failures doesn't blow up perf_results.json.
ERROR_SAMPLE_LIMIT = int(os.environ.get("ERROR_SAMPLE_LIMIT", 500))
_error_samples = []


def _record_error_sample(path, response=None, exc=None):
    if len(_error_samples) >= ERROR_SAMPLE_LIMIT:
        return
    sample = {"path": path, "time": time.time()}
    if exc is not None:
        sample["exception"] = f"{type(exc).__name__}: {exc}"
    if response is not None:
        url = getattr(response, "url", None)
        history = getattr(response, "history", None) or []
        sample["status"] = getattr(response, "status_code", None)
        sample["final_url"] = url
        sample["final_host"] = urlparse(url).netloc if url else None
        sample["redirected"] = bool(history)
        if history:
            sample["redirect_chain"] = [
                {
                    "status": getattr(r, "status_code", None),
                    "url": getattr(r, "url", None),
                    "location": r.headers.get("Location") if hasattr(r, "headers") else None,
                }
                for r in history
            ]
    _error_samples.append(sample)


class HdaUser(HttpUser):
    host = HDA_URL
    # was between(3, 8): each VU barely made a request a minute, so raising VU
    # count mostly added idle users instead of concurrent load. Shrinking this
    # is what actually makes VUs hammer the target simultaneously.
    wait_time = between(0.1, 0.5)

    def _traced_request(self, method, path, name, headers=None, consume_body=False):
        # every task routes through here so failures are captured uniformly
        # (see _record_error_sample) — not just the download-heavy ones.
        # consume_body=True also gets the stream+discard treatment: reading
        # the whole body into memory is fine for KB-sized responses, but
        # GET_tif_full alone is 250+ MB, and with enough in flight at once
        # (more likely than its raw task weight suggests, since it's also
        # the slowest task) that's a real way to push a CI runner into memory
        # pressure as VU count climbs. catch_response=True still times the
        # full transfer either way — the request isn't "done" until the loop
        # below finishes for streamed requests — and still reports non-2xx
        # as a failure, same as plain self.client.get() would.
        request_fn = self.client.get if method == "GET" else self.client.head
        try:
            with request_fn(
                path, name=name, headers=headers, stream=consume_body, catch_response=True
            ) as response:
                if response.status_code >= 400:
                    response.failure(f"status {response.status_code}")
                    _record_error_sample(path, response=response)
                    return
                if consume_body:
                    for _ in response.iter_content(chunk_size=1024 * 1024):
                        pass
        except Exception as exc:
            # connection-level failures (dropped connections, timeouts) —
            # Locust's catch_response usually absorbs these into its own
            # failure accounting without raising, but if one does propagate,
            # record what we know (path + exception) before letting it go so
            # existing behavior/stats aren't changed by adding this tracing.
            _record_error_sample(path, exc=exc)
            raise

    @task(2)
    def get_zarr(self):
        self._traced_request("GET", ZARR_PATH, "GET_zarray")

    @task(1)
    def head_geotiff(self):
        # cheap header-only check, kept for comparison against the real reads below
        self._traced_request("HEAD", random.choice(DATA_FILE_PATHS), "HEAD_tif")

    @task(3)
    def get_geotiff_tile(self):
        # single COG tile/overview read — the dominant real-world access pattern
        self._traced_request(
            "GET", random.choice(DATA_FILE_PATHS), "GET_tif_range_tile",
            headers={"Range": f"bytes=0-{RANGE_TILE_BYTES - 1}"},
        )

    @task(1)
    def get_geotiff_chunk(self):
        # larger windowed pull, to see actual throughput degrade under load
        self._traced_request(
            "GET", random.choice(DATA_FILE_PATHS), "GET_tif_range_chunk",
            headers={"Range": f"bytes=0-{RANGE_CHUNK_BYTES - 1}"}, consume_body=True,
        )

    @task(1)
    def get_geotiff_full(self):
        # no Range header at all — a real full-object download, picking a
        # different file per request so this models many different users
        # pulling different big objects rather than everyone hammering one
        # (possibly cached) file. Weight is 1 of 8 total, so roughly VU_count/8
        # of these can be in flight at once at the top of the ramp: at
        # VU_MAX=295 that's ~37 concurrent 250+ MB downloads at a time.
        self._traced_request("GET", random.choice(DATA_FILE_PATHS), "GET_tif_full", consume_body=True)


class RedirectOnlyUser(HttpUser):
    # isolates the origin's own resolve-and-redirect step from the actual
    # S3 transfer that follows it. Every DATA_FILE_PATHS entry is confirmed
    # (via curl) to redirect (302) rather than serve bytes directly — this
    # class never follows that redirect, so 100% of its load lands on
    # whatever the origin's own bottleneck is (proxy, metadata lookup, auth),
    # uncontaminated by S3's completely separate performance characteristics.
    host = HDA_URL
    wait_time = between(0.1, 0.5)

    @task
    def check_redirect(self):
        path = random.choice(DATA_FILE_PATHS)
        with self.client.get(
            path, name="REDIRECT_only", allow_redirects=False, catch_response=True
        ) as response:
            if response.status_code >= 400:
                response.failure(f"status {response.status_code}")
                _record_error_sample(path, response=response)
            elif response.status_code not in (301, 302, 303, 307, 308):
                # not necessarily wrong — some paths might be served directly —
                # but worth knowing about since every path here was previously
                # confirmed to redirect; flag it as a failure so it's visible.
                response.failure(f"expected a redirect, got status {response.status_code}")
                _record_error_sample(path, response=response)


ACTIVE_USER_CLASS = RedirectOnlyUser if TASK_MODE == "redirect_only" else HdaUser


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
    # returns list of reasons if this stage broke a threshold, empty list if all good.
    # p95 is deliberately NOT checked here — see the comment by P95_THRESHOLD_SECS.
    # It's still in every stage's stats/log/report for whoever wants to apply
    # their own SLA bar to the curve; it just doesn't stop the ramp itself.
    reasons = []
    for endpoint, s in stage_stats.items():
        if s["err"] > ERROR_RATE_THRESHOLD:
            reasons.append(f"{endpoint}: error_rate={s['err']:.1%} > {ERROR_RATE_THRESHOLD:.0%}")

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


def write_results(all_stages, stage_meta, breakpoint_info, stage_secs, soak_mode):
    # called after every stage, not just at the end, so a mid-ramp crash
    # (either the target service or the runner) still leaves the stages
    # measured so far on disk instead of losing the whole run.
    with open(RESULTS_JSON, "w") as f:
        json.dump(
            {
                "target": HDA_URL,
                "timestamp": time.time(),
                "task_mode": TASK_MODE,
                "soak_mode": soak_mode,
                "config": {
                    "vu_start": VU_START,
                    "vu_step": VU_STEP,
                    "vu_max": VU_MAX,
                    "soak_vus": SOAK_VUS or None,
                    "stage_secs": stage_secs,
                    "warmup_secs": WARMUP_SECS,
                    "error_rate_threshold": ERROR_RATE_THRESHOLD,
                    "throughput_drop_threshold": THROUGHPUT_DROP_THRESHOLD,
                    "p95_threshold_secs": P95_THRESHOLD_SECS,
                    "full_download_p95_threshold_secs": FULL_DOWNLOAD_P95_THRESHOLD_SECS,
                },
                "breakpoint": breakpoint_info,
                "stages": all_stages,
                "stage_meta": stage_meta,
                "error_samples": _error_samples,
            },
            f,
            indent=2,
        )


def main():
    setup_logging("INFO")
    env = Environment(user_classes=[ACTIVE_USER_CLASS])
    log.info("TASK_MODE=%s -> using %s", TASK_MODE, ACTIVE_USER_CLASS.__name__)
    env.create_local_runner()

    all_stages = {}
    stage_meta = {}
    breach_streak = 0
    breach_streak_reasons = []  # reasons per stage of the CURRENT streak, oldest first
    breakpoint_info = None
    peak_total_rps = 0.0
    peak_total_rps_vu = None
    last_clean_vu = None       # last stage with zero breach reasons
    recommended_safe_vus = None  # last clean stage seen before the *first ever* breach

    soak_mode = bool(SOAK_VUS)
    if soak_mode:
        vu_sequence = SOAK_VUS
        stage_secs = SOAK_STAGE_SECS
        log.info("Soak-test mode: fixed VU list %s, %ds/stage — runs the whole "
                  "list regardless of breaches, to see stable vs. transient behavior",
                  SOAK_VUS, SOAK_STAGE_SECS)
    else:
        vu_sequence = list(range(VU_START, VU_MAX + 1, VU_STEP))
        stage_secs = STAGE_SECS

    for vu_count in vu_sequence:
        log.info("Stage %d VUs — %ds (%ds warmup excluded)", vu_count, stage_secs, WARMUP_SECS)
        stage_started_at = time.time()
        env.runner.start(vu_count, spawn_rate=SPAWN_RATE)

        gevent.sleep(WARMUP_SECS)
        env.stats.reset_all()  # drop warmup requests, start measuring clean
        measure_started_at = time.time()

        gevent.sleep(max(stage_secs - WARMUP_SECS, 1))
        env.runner.stop()
        measured_secs = time.time() - measure_started_at
        gevent.sleep(1)

        stage_stats = collect_stage_stats(env)
        all_stages[vu_count] = stage_stats
        total_rps = sum(s["rps"] for s in stage_stats.values())
        # ru_maxrss is cumulative peak-so-far (not per-stage) on Linux, but
        # still shows which stage first pushed memory to a new high — the
        # thing we actually want to see if the process gets killed under load.
        peak_rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        stage_meta[vu_count] = {
            "measured_secs": measured_secs,
            "wall_secs": time.time() - stage_started_at,
            "total_requests": sum(s["num_requests"] for s in stage_stats.values()),
            "total_failures": sum(s["num_failures"] for s in stage_stats.values()),
            "total_rps": total_rps,
            "peak_rss_mb": peak_rss_mb,
        }

        log.info(
            "  stage took %.1fs measured (%.1fs incl. warmup+spawn) — %d requests, %d failures, "
            "%.1f total rps, %.0fMB peak RSS so far",
            stage_meta[vu_count]["measured_secs"], stage_meta[vu_count]["wall_secs"],
            stage_meta[vu_count]["total_requests"], stage_meta[vu_count]["total_failures"],
            total_rps, peak_rss_mb,
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
            if recommended_safe_vus is None:
                # first time anything has ever breached — the last clean stage
                # before this is "tested this much concurrency with zero
                # issues," a defensible headroom number distinct from the
                # eventual (confirmed) breakpoint further up the ramp.
                recommended_safe_vus = last_clean_vu
            breach_streak += 1
            breach_streak_reasons.append((vu_count, reasons))
            log.warning("Stage %d VUs breached thresholds (%d/%d): %s",
                        vu_count, breach_streak, BREACH_STAGES_TO_CONFIRM, "; ".join(reasons))
        else:
            breach_streak = 0
            breach_streak_reasons = []
            last_clean_vu = vu_count

        if breach_streak >= BREACH_STAGES_TO_CONFIRM and breakpoint_info is None:
            # reasons from the stage where the streak actually STARTED, not
            # from whichever stage happened to confirm it (those can differ —
            # e.g. VU 530 breached on a 6.0s p95 but the confirming stage at
            # 555 only re-breached at 5.1s; reporting 555's number next to
            # 530's label was the exact mismatch this fixes).
            first_breach_vu, first_breach_reasons = breach_streak_reasons[0]
            breakpoint_info = {
                "vus": first_breach_vu,
                "confirmed_at_vus": vu_count,
                "reasons": first_breach_reasons,
                "confirming_stage_reasons": reasons,
                "recommended_safe_vus": recommended_safe_vus,
            }
            log.error(
                "BREAKPOINT confirmed at %d VUs (confirmed at %d VUs): %s "
                "(recommended safe operating capacity: %s VUs)",
                first_breach_vu, vu_count, "; ".join(first_breach_reasons), recommended_safe_vus,
            )
            write_results(all_stages, stage_meta, breakpoint_info, stage_secs, soak_mode)
            if not soak_mode:
                # a ramp stops once it has its answer; a soak run's whole point
                # is to sit in this region for a while, so it keeps going
                # through the rest of the fixed VU list even past this point.
                break

        write_results(all_stages, stage_meta, breakpoint_info, stage_secs, soak_mode)

    push_metrics(all_stages, breakpoint_info)
    log.info("Results written to %s", RESULTS_JSON)

    env.runner.quit()
    log.info("Done.")


if __name__ == "__main__":
    main()
