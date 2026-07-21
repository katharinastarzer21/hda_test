"""Small ramp test for HDA's Airflow-backed on-demand ingestion path
(FederatedEodagRouter: NASA VIIRS/NISAR/OPERA etc.) — does cold-start
ingestion work, and does polling recognize readiness correctly.

Usage: python ingestion_ramp_smoke_test.py
"""
import gevent.monkey
gevent.monkey.patch_all()

import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urljoin

import requests
from gevent.lock import Semaphore
from gevent.pool import Pool

HDA_URL = os.environ.get("HDA_URL", "https://dev.hda.eodchosting.eu").rstrip("/")
STAC_URL = os.environ.get("HDA_STAC_URL", "https://eodag.dev.services.eodc.eu").rstrip("/")

RAMP_LEVELS = [int(v.strip()) for v in os.environ.get("RAMP_LEVELS", "1,3,5").split(",") if v.strip()]

ACCEPTABLE_INGEST_WAIT_SECS = float(os.environ.get("ACCEPTABLE_INGEST_WAIT_SECS", 180))
MAX_INGEST_WAIT_SECS = float(os.environ.get("MAX_INGEST_WAIT_SECS", 300))
POLL_INTERVAL_SECS = float(os.environ.get("POLL_INTERVAL_SECS", 15))
DOWNLOAD_TIMEOUT_SECS = float(os.environ.get("DOWNLOAD_TIMEOUT_SECS", 300))
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "/tmp/hda_ingestion_ramp_test")

# KEEP_DOWNLOADS_SAMPLE>0 keeps that many of the FIRST successful downloads
# on disk (under KEEP_DOWNLOADS_DIR) for spot checks; 0 deletes all of them.
KEEP_DOWNLOADS_SAMPLE = int(os.environ.get("KEEP_DOWNLOADS_SAMPLE", 0))
KEEP_DOWNLOADS_DIR = os.environ.get("KEEP_DOWNLOADS_DIR", os.path.join(DOWNLOAD_DIR, "kept"))
_kept_downloads_count = 0
_kept_downloads_lock = Semaphore()

_DEFAULT_TARGET_COLLECTIONS = ",".join([
    "VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002",
    "NISAR_L1_RSLC_BETA_V1_1",
    "NISAR_L2_GCOV_BETA_V1_1",
    "OPERA_L2_RTC-S1_V1",
])
TARGET_COLLECTIONS = [
    c.strip() for c in os.environ.get("TARGET_COLLECTIONS", _DEFAULT_TARGET_COLLECTIONS).split(",")
    if c.strip()
]
ITEMS_PER_COLLECTION = int(os.environ.get("ITEMS_PER_COLLECTION", 40))
MAX_STAC_PAGES = int(os.environ.get("MAX_STAC_PAGES", 50))
STAC_WINDOW_DAYS = int(os.environ.get("STAC_WINDOW_DAYS", 7))

RESULTS_JSON = os.environ.get("RESULTS_JSON", "ingestion_ramp_results.json")

# for collections not in the STAC catalog (e.g. SENTINEL1_SIG0_20M) — used
# verbatim instead of STAC discovery, cycled to fill the ramp's needs.
STATIC_ASSET_PATHS = [
    p.strip() for p in os.environ.get("STATIC_ASSET_PATHS", "").split(",") if p.strip()
]

SEEN_CACHE_FILE = os.environ.get("SEEN_CACHE_FILE", "ingestion_ramp_seen.json")

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)


def load_seen():
    if not os.path.isfile(SEEN_CACHE_FILE):
        return set()
    with open(SEEN_CACHE_FILE) as f:
        return set(json.load(f))


def save_seen(seen):
    with open(SEEN_CACHE_FILE, "w") as f:
        json.dump(sorted(seen), f, indent=2)


def fetch_fresh_pool(seen, min_needed=0):
    # Uses `datetime` windows, not `limit`/`next` pagination: for NASA/CMR
    # collections (eodc_cmr_nasa_nisar's CMRSearch plugin), eodag core stores
    # limit/next_page_token only on PreparedSearch attributes the plugin
    # never reads, so every call silently returns the same first 20 items
    # regardless of limit or next. `datetime` isn't affected by that bug and
    # reaches CMR as a real filter, so distinct time windows are the only
    # reliable way to get fresh items.
    pool = []
    now = datetime.now(timezone.utc)
    for cid in TARGET_COLLECTIONS:
        collection_fresh = []
        windows_tried = 0
        window_end = now

        while windows_tried < MAX_STAC_PAGES:
            window_start = window_end - timedelta(days=STAC_WINDOW_DAYS)
            params = {
                "limit": ITEMS_PER_COLLECTION,
                "datetime": f"{window_start.strftime('%Y-%m-%dT%H:%M:%SZ')}/"
                            f"{window_end.strftime('%Y-%m-%dT%H:%M:%SZ')}",
            }
            try:
                resp = requests.get(f"{STAC_URL}/collections/{cid}/items", params=params, timeout=60)
                resp.raise_for_status()
            except requests.RequestException as exc:
                log.warning("Could not fetch items for collection %s (window %s): %s",
                            cid, params["datetime"], exc)
                break

            data = resp.json()
            windows_tried += 1
            for item in data.get("features", []):
                for asset_name, asset in item.get("assets", {}).items():
                    href = asset.get("href")
                    if not href:
                        continue
                    parsed = urlparse(urljoin(STAC_URL, href))
                    if HDA_URL not in f"{parsed.scheme}://{parsed.netloc}":
                        continue
                    if parsed.path in seen:
                        continue
                    collection_fresh.append(parsed.path)
                    break  # one asset per item — siblings share an ingestion job

            window_end = window_start

            if min_needed and len(collection_fresh) >= min_needed:
                break

        log.info("  %s: %d fresh asset paths found (%d time window(s) queried)",
                  cid, len(collection_fresh), windows_tried)
        pool.extend(collection_fresh)

    random.shuffle(pool)
    return pool


def _maybe_keep_download(tmp_path, rec, path):
    global _kept_downloads_count
    if KEEP_DOWNLOADS_SAMPLE <= 0 or rec.get("outcome") != "success":
        return False
    with _kept_downloads_lock:
        if _kept_downloads_count >= KEEP_DOWNLOADS_SAMPLE:
            return False
        _kept_downloads_count += 1
        os.makedirs(KEEP_DOWNLOADS_DIR, exist_ok=True)
        kept_path = os.path.join(KEEP_DOWNLOADS_DIR, path.lstrip("/").replace("/", "__"))
        os.replace(tmp_path, kept_path)
        rec["kept_path"] = kept_path
        return True


def attempt_one(path, tag):
    url = f"{HDA_URL}{path}"
    rec = {"path": path, "tag": tag, "started_at": time.time()}
    session = requests.Session()

    waited = 0.0
    poll_count = 0
    saw_ingesting = False
    ready = False
    first_status = None

    while True:
        poll_count += 1
        try:
            # allow_redirects=False: only reads the origin's own status
            # (202/302/200), never transits to S3 during polling.
            resp = session.get(url, timeout=30, allow_redirects=False)
        except requests.RequestException as exc:
            rec["outcome"] = f"poll_exception: {exc}"
            rec["waited_secs"] = waited
            rec["poll_count"] = poll_count
            log.info("[%s] %s poll #%d -> exception: %s", tag, path, poll_count, exc)
            return rec

        if first_status is None:
            first_status = resp.status_code

        if resp.status_code in (200, 302):
            ready = True
            log.info("[%s] %s poll #%d -> %d, ready after %.0fs wait",
                      tag, path, poll_count, resp.status_code, waited)
            break
        if resp.status_code == 202:
            saw_ingesting = True
            log.info("[%s] %s poll #%d -> 202 still ingesting (%.0fs waited so far)",
                      tag, path, poll_count, waited)
        else:
            rec["outcome"] = f"poll_unexpected_status_{resp.status_code}"
            rec["waited_secs"] = waited
            rec["poll_count"] = poll_count
            log.warning("[%s] %s poll #%d -> unexpected status %d",
                        tag, path, poll_count, resp.status_code)
            return rec

        if waited + POLL_INTERVAL_SECS > MAX_INGEST_WAIT_SECS:
            break
        time.sleep(POLL_INTERVAL_SECS)
        waited += POLL_INTERVAL_SECS

    rec["waited_secs"] = waited
    rec["poll_count"] = poll_count
    rec["first_status"] = first_status
    rec["saw_ingesting"] = saw_ingesting
    rec["cache_state"] = "cold" if saw_ingesting else "warm"
    rec["within_acceptable_wait"] = waited <= ACCEPTABLE_INGEST_WAIT_SECS

    if not ready:
        rec["outcome"] = "ingestion_timeout"
        log.warning("[%s] %s gave up after %.0fs, never left 202", tag, path, waited)
        return rec

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    tmp_path = os.path.join(DOWNLOAD_DIR, f"dl_{os.getpid()}_{time.time_ns()}.bin")
    dl_started = time.time()
    try:
        with session.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT_SECS) as resp:
            if resp.status_code >= 400:
                rec["outcome"] = f"download_failed_status_{resp.status_code}"
                return rec
            expected_size = resp.headers.get("Content-Length")
            expected_size = int(expected_size) if expected_size else None
            bytes_written = 0
            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
                    bytes_written += len(chunk)
        rec["download_secs"] = time.time() - dl_started
        rec["bytes_downloaded"] = bytes_written
        rec["expected_size"] = expected_size
        size_ok = expected_size is None or bytes_written == expected_size
        rec["outcome"] = "success" if size_ok else "size_mismatch"
        log.info("[%s] %s downloaded %d bytes in %.1fs (%s)",
                  tag, path, bytes_written, rec["download_secs"], rec["outcome"])
    except requests.RequestException as exc:
        rec["outcome"] = f"download_exception: {exc}"
        log.warning("[%s] %s download failed: %s", tag, path, exc)
    finally:
        if os.path.exists(tmp_path) and not _maybe_keep_download(tmp_path, rec, path):
            os.remove(tmp_path)

    rec["total_secs"] = time.time() - rec["started_at"]
    return rec


def run_level(n, paths):
    pool = Pool(n)
    greenlets = [
        pool.spawn(attempt_one, path, f"L{n}-{i+1}/{n}")
        for i, path in enumerate(paths)
    ]
    pool.join(timeout=MAX_INGEST_WAIT_SECS + DOWNLOAD_TIMEOUT_SECS + 30)
    results = []
    for g in greenlets:
        if g.ready() and g.value is not None:
            results.append(g.value)
        else:
            results.append({"path": None, "outcome": "join_timeout_never_returned"})
    return results


def summarize(n, results):
    succeeded = [r for r in results if r.get("outcome") == "success"]
    cold = [r for r in results if r.get("cache_state") == "cold"]
    warm = [r for r in results if r.get("cache_state") == "warm"]
    cold_succeeded = [r for r in cold if r.get("outcome") == "success"]
    timed_out = [r for r in results if r.get("outcome") == "ingestion_timeout"]
    other_failed = len(results) - len(succeeded) - len(timed_out)

    cold_ingest_waits = [r["waited_secs"] for r in cold_succeeded]
    download_secs = [r["download_secs"] for r in succeeded if "download_secs" in r]

    return {
        "attempted": n,
        "succeeded": len(succeeded),
        "cold_count": len(cold),
        "warm_count": len(warm),
        "cold_succeeded": len(cold_succeeded),
        "cold_succeeded_within_acceptable_wait": sum(
            1 for r in cold_succeeded if r.get("within_acceptable_wait")
        ),
        "timed_out_waiting_for_ingestion": len(timed_out),
        "other_failures": other_failed,
        "avg_cold_ingest_wait_secs": (
            sum(cold_ingest_waits) / len(cold_ingest_waits) if cold_ingest_waits else None
        ),
        "max_cold_ingest_wait_secs": max(cold_ingest_waits) if cold_ingest_waits else None,
        "avg_download_secs": sum(download_secs) / len(download_secs) if download_secs else None,
        "max_download_secs": max(download_secs) if download_secs else None,
        "results": results,
    }


def write_results(levels):
    with open(RESULTS_JSON, "w") as f:
        json.dump(
            {
                "target": HDA_URL,
                "timestamp": time.time(),
                "config": {
                    "ramp_levels": RAMP_LEVELS,
                    "acceptable_ingest_wait_secs": ACCEPTABLE_INGEST_WAIT_SECS,
                    "max_ingest_wait_secs": MAX_INGEST_WAIT_SECS,
                    "poll_interval_secs": POLL_INTERVAL_SECS,
                    "download_timeout_secs": DOWNLOAD_TIMEOUT_SECS,
                    "target_collections": TARGET_COLLECTIONS,
                },
                "levels": levels,
            },
            f,
            indent=2,
        )


def main():
    log.info("Ingestion ramp smoke test against %s", HDA_URL)
    log.info("Ramp levels: %s", RAMP_LEVELS)

    seen = load_seen()
    needed = sum(RAMP_LEVELS)

    if STATIC_ASSET_PATHS:
        log.info("STATIC_ASSET_PATHS set (%d path(s)) — skipping STAC discovery entirely, "
                  "cycling this list to fill all %d attempts.", len(STATIC_ASSET_PATHS), needed)
        pool = [STATIC_ASSET_PATHS[i % len(STATIC_ASSET_PATHS)] for i in range(needed)]
    else:
        pool = fetch_fresh_pool(seen, min_needed=needed)
        log.info("%d fresh (never-before-used) candidate asset paths available, %d needed for this ramp",
                  len(pool), needed)
        if len(pool) < needed:
            log.warning(
                "Not enough fresh assets for the full ramp (%d < %d) — later stages will run "
                "with fewer VUs than requested, or delete %s to reuse older assets.",
                len(pool), needed, SEEN_CACHE_FILE,
            )

    levels = {}
    for n in RAMP_LEVELS:
        stage_paths, pool = pool[:n], pool[n:]
        if not stage_paths:
            log.warning("Stage %d VUs: no fresh assets left, skipping.", n)
            continue
        actual_n = len(stage_paths)
        log.info("Stage %d VUs (%d fresh assets available for it)...", n, actual_n)

        results = run_level(actual_n, stage_paths)
        if not STATIC_ASSET_PATHS:
            seen.update(p for p in stage_paths)
            save_seen(seen)

        summary = summarize(actual_n, results)
        levels[n] = summary
        write_results(levels)

        log.info(
            "  Stage %d: %d/%d succeeded (%d cold-start, %d already-warm) | "
            "%d timed out still-ingesting | %d other failures | "
            "avg cold ingest wait %.1fs | avg download %.1fs",
            n, summary["succeeded"], actual_n, summary["cold_count"], summary["warm_count"],
            summary["timed_out_waiting_for_ingestion"], summary["other_failures"],
            summary["avg_cold_ingest_wait_secs"] or 0.0, summary["avg_download_secs"] or 0.0,
        )

        if summary["succeeded"] == 0 and n > RAMP_LEVELS[0]:
            log.warning("Zero successes at %d concurrent — stopping the ramp early.", n)
            break

    log.info("Done. Results written to %s (seen-asset cache: %s)", RESULTS_JSON, SEEN_CACHE_FILE)


if __name__ == "__main__":
    main()
