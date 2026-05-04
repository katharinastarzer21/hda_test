"""
Availability probes for HDA and HDA-Go.

Probes:
  hda     — HEAD on a known Zarr array metadata file
  hda_go  — HEAD on a known GeoTIFF file

Env vars:
  HDA_URL     Base URL (e.g. https://data.eodc.eu)
  E2E_ENV     prod or dev
  OTEL_ENDPOINT
  OTEL_API_KEY
"""

import os, time, logging, requests
from otel_push import record, flush

HDA_URL = os.environ.get("HDA_URL", "https://data.eodc.eu").rstrip("/")
TIMEOUT = 20

HDA_PATH    = "/collections/S2-L2A-C1/T33UWP/indices/time/.zarray"
HDA_GO_PATH = "/collections/SENTINEL1_SIG0_20M/V1M2R3/EQUI7_EU020M/E048N015T3/SIG0_20260412T171426__VV_A015_E048N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif"

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)


def probe(url):
    t0 = time.perf_counter()
    try:
        r = requests.head(url, timeout=TIMEOUT, allow_redirects=True)
        return r.status_code, time.perf_counter() - t0
    except Exception:
        return 0, time.perf_counter() - t0


def push(probe_name, success, duration, status):
    record(
        {"eodc_e2e_probe_success":            int(success),
         "eodc_e2e_probe_duration_seconds":   duration,
         "eodc_e2e_probe_http_status":        float(status),
         "eodc_e2e_probe_last_run_timestamp": time.time()},
        {"service": "hda", "probe": probe_name},
    )


def ok(status):
    return 200 <= status < 300


def run():
    all_ok = True

    # HDA probe
    status, dur = probe(f"{HDA_URL}{HDA_PATH}")
    result = ok(status)
    log.info("hda     %s  http=%d  %.0fms", "OK" if result else "FAIL", status, dur * 1000)
    push("hda", result, dur, status)
    all_ok = all_ok and result

    # HDA-Go probe
    status, dur = probe(f"{HDA_URL}{HDA_GO_PATH}")
    result = ok(status)
    log.info("hda_go  %s  http=%d  %.0fms", "OK" if result else "FAIL", status, dur * 1000)
    push("hda_go", result, dur, status)
    all_ok = all_ok and result

    flush()
    return all_ok


if __name__ == "__main__":
    success = run()
    if not success:
        raise SystemExit(1)
