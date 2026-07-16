"""
Small, careful ramp test for HDA's on-demand "ingestion" path (the
Airflow-backed FederatedEodagRouter: NASA VIIRS/NISAR/OPERA, cop_cds
timeseries, etc. — the collections where a click has to wait for a redirect,
an Airflow DAG run, and only then serves the real file).

Run this BEFORE perf_test.py / ingestion_download_test.py at real concurrency.
Its only job is to answer, at a small and slow scale (default 1 -> 3 -> 5
concurrent downloads): does the cold-start path work at all right now, and
does the client-side polling actually recognize the transition when it does?

Two bugs in ingestion_download_test.py motivated a fresh script rather than
patching that one in place:

  1. Its poll step is `session.get(url, timeout=30)` with no `stream=True`.
     That is fine while the backend answers 202. But once the asset becomes
     ready, the backend replies with a 302 redirect straight to a presigned
     S3 URL (see federated_eodag.py get_asset()), requests follows it
     transparently, and the SAME "just checking status" call is now a full,
     non-streamed download of the real object with only a 30s timeout. For
     multi-GB NISAR/OPERA assets that reliably blows up mid-transfer. This
     is not hypothetical — ingestion_results_small.json has a poll_exception
     reading "IncompleteRead(7568408576 bytes read, 4880285696 more
     expected)" at waited_secs=0.0: ingestion had actually finished, the test
     just filed the real success as a generic failure. This script's poll
     step uses allow_redirects=False and never follows past the origin's own
     response — 202 (still ingesting), 302 (ready, federated/Airflow path),
     or 200 (ready, direct-serve collections that never go through Airflow
     at all) are all visible from the origin alone, with zero bytes of body
     ever read during polling. The real transfer happens exactly once, in a
     separate streamed request, only after a poll has confirmed readiness.

  2. Its asset pool is a static list hardcoded in the script. The first time
     any entry is hit, ingestion completes and that S3 object is warm
     forever — every later run, and every later/higher-concurrency stage
     within the very same run, increasingly draws already-warm files with no
     way to tell warm and cold apart (see concurrency level 5 in that same
     results file: avg_ingest_wait_secs 0.0 across the board — not because
     ingestion got fast, because everything there was already warm). This
     script pulls a fresh item pool from the STAC API right before running,
     samples WITHOUT replacement so one stage can't reuse another's assets,
     and additionally remembers every path it has ever used in a small local
     cache file so re-running this script during debugging doesn't silently
     retest the same warm handful. Every result is tagged "cold" (saw a 202
     before becoming ready) or "warm" (ready on the very first poll) so the
     two are never averaged together.

This deliberately does NOT try to distinguish "Airflow never got the
trigger" from "Airflow is just slow" — it can't see Airflow directly. What it
DOES give you: for every attempt, first_status, poll_count, time-to-ready,
and download time, so a run where several assets sit at 202 for the full
MAX_INGEST_WAIT_SECS without ever transitioning is visible as exactly that,
separately from assets that transition promptly.

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

# ramp is deliberately small and ascending, not the wide sweep perf_test.py
# does — the point is a sanity check, not a breakpoint search.
RAMP_LEVELS = [int(v.strip()) for v in os.environ.get("RAMP_LEVELS", "1,3,5").split(",") if v.strip()]

# same split as ingestion_download_test.py: ACCEPTABLE is "still a fine user
# experience", MAX is "give up, this attempt failed".
ACCEPTABLE_INGEST_WAIT_SECS = float(os.environ.get("ACCEPTABLE_INGEST_WAIT_SECS", 180))
MAX_INGEST_WAIT_SECS = float(os.environ.get("MAX_INGEST_WAIT_SECS", 300))
POLL_INTERVAL_SECS = float(os.environ.get("POLL_INTERVAL_SECS", 15))
DOWNLOAD_TIMEOUT_SECS = float(os.environ.get("DOWNLOAD_TIMEOUT_SECS", 300))
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "/tmp/hda_ingestion_ramp_test")

# every successful download is deleted right after its size-check by default
# (same reasoning as ingestion_download_test.py — don't fill the disk at high
# concurrency). Set KEEP_DOWNLOADS_SAMPLE>0 to keep that many of the FIRST
# successful downloads on disk (under KEEP_DOWNLOADS_DIR) for manual spot
# checks, without keeping every single one at higher VU counts.
KEEP_DOWNLOADS_SAMPLE = int(os.environ.get("KEEP_DOWNLOADS_SAMPLE", 0))
KEEP_DOWNLOADS_DIR = os.environ.get("KEEP_DOWNLOADS_DIR", os.path.join(DOWNLOAD_DIR, "kept"))
_kept_downloads_count = 0
_kept_downloads_lock = Semaphore()

# collections known to go through the Airflow-backed FederatedEodagRouter
# (cold-start pattern) — override with TARGET_COLLECTIONS to add direct-serve
# collections too; those will just come back tagged "warm" every time, which
# is itself a useful baseline to compare against.
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
# safety bound on how many STAC pages to walk per collection while paginating
# for fresh assets — real collections can have millions of items, so without
# this a min_needed that's simply unreachable (e.g. every remaining item's
# assets are already in the seen-cache) would loop forever instead of giving
# up with whatever it found.
MAX_STAC_PAGES = int(os.environ.get("MAX_STAC_PAGES", 50))
# size, in days, of each backward-stepping datetime window used to pull a
# fresh slice of items — see fetch_fresh_pool() for why datetime windows are
# used instead of limit/next-link pagination.
STAC_WINDOW_DAYS = int(os.environ.get("STAC_WINDOW_DAYS", 7))

RESULTS_JSON = os.environ.get("RESULTS_JSON", "ingestion_ramp_results.json")

# escape hatch for collections that aren't in the STAC catalog at all (e.g.
# SENTINEL1_SIG0_20M — a real, working DirectFilepathRouter collection, but
# its provider isn't in EODAG_PROVIDERS_WHITELIST so fetch_fresh_pool() has
# no STAC entry point to query). When set, this is used verbatim instead of
# STAC discovery, cycled to fill however many attempts the ramp needs.
# Reusing the same path(s) repeatedly is fine for direct-serve collections
# specifically — there's no cold/warm S3-staging state to bias, unlike the
# NASA/CMR-backed ones this script was originally built for.
STATIC_ASSET_PATHS = [
    p.strip() for p in os.environ.get("STATIC_ASSET_PATHS", "").split(",") if p.strip()
]

# persists across runs so re-running this script while debugging doesn't
# keep re-selecting (and re-"proving fast") the same already-warmed handful
# of assets STAC happens to return first.
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
    # mirrors search_collection.py's approach (STAC -> per-collection items
    # -> asset hrefs) but scoped to TARGET_COLLECTIONS and filtered against
    # the seen-cache so this always returns genuinely untested paths.
    #
    # Does NOT use the STAC "next" link for pagination — for NASA/CMR-backed
    # collections routed through eodc_cmr_nasa_nisar's CMRSearch plugin
    # (eodag_cmr.py), eodag core pops `limit`/`next_page_token(_key)` out of
    # kwargs and stores them only as PreparedSearch attributes
    # (prep.limit/prep.next_page_token), but the plugin reads them back out
    # of a plain params dict that never receives them (it checks
    # getattr(prep, "kwargs", {}), which doesn't exist on PreparedSearch) —
    # so every call silently falls back to the hardcoded default of 20 items
    # with no real search-after cursor forwarded to CMR, regardless of
    # `limit=` or of following `next`. Confirmed live: limit=80 still only
    # returns 20 items, sortby=-datetime returns the identical 20, and the
    # `next` link (present/absent inconsistently) is a dead end either way.
    #
    # `datetime` range filters, by contrast, are NOT stripped by core and do
    # reach CMR as a real temporal filter — so distinct time windows are the
    # only lever that currently returns different items. We step backward in
    # STAC_WINDOW_DAYS-sized windows from "now" until enough fresh assets are
    # collected or MAX_STAC_PAGES windows have been tried.
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
                        continue  # not an HDA-served asset, not relevant here
                    if parsed.path in seen:
                        continue
                    collection_fresh.append(parsed.path)

            window_end = window_start  # step further back in time next iteration

            if min_needed and len(collection_fresh) >= min_needed:
                break

        log.info("  %s: %d fresh asset paths found (%d time window(s) queried)",
                  cid, len(collection_fresh), windows_tried)
        pool.extend(collection_fresh)

    random.shuffle(pool)
    return pool


def _maybe_keep_download(tmp_path, rec, path):
    # keeps only the first KEEP_DOWNLOADS_SAMPLE successful downloads across
    # the whole run (not per-stage) so a KEEP_DOWNLOADS_SAMPLE=3 at VU=20
    # doesn't accidentally keep 3 per stage x N stages. Returns True if this
    # file was kept (caller must not delete tmp_path in that case).
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
            # allow_redirects=False is the whole point: this reads only the
            # origin's own status (202 / 302 / 200), zero response body,
            # never transits to S3 during polling. See module docstring.
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
        log.warning("[%s] %s gave up after %.0fs, never left 202 — possible trigger "
                    "failure, see AirflowIngestionClient.trigger_ingestion", tag, path, waited)
        return rec

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    tmp_path = os.path.join(DOWNLOAD_DIR, f"dl_{os.getpid()}_{time.time_ns()}.bin")
    dl_started = time.time()
    try:
        # the ONE place a response body is ever read — default
        # allow_redirects=True here so this follows the 302 to S3 for real.
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
            log.warning("Zero successes at %d concurrent — stopping the ramp early, "
                        "no point testing higher concurrency until this is understood.", n)
            break

    log.info("Done. Results written to %s (seen-asset cache: %s)", RESULTS_JSON, SEEN_CACHE_FILE)


if __name__ == "__main__":
    main()
