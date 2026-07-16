# HDA ingestion/perf test suite — findings (2026-07-16)

Retrospective on ~3 days spent trying to get concurrency testing of HDA's
on-demand "ingestion" path (Airflow-backed `FederatedEodagRouter`: NASA
VIIRS/NISAR/OPERA, cop_cds timeseries) working reliably, plus a review of
the rest of the `hda_test` scripts. Target: `dev.hda.eodchosting.eu`.

**Bottom line up front:** the 3-day delay was not the server failing to handle
load — it was two real bugs in our own test code (misreading successes as
crashes, and silently re-testing already-warmed files instead of fresh ones),
one real backend bug (a swallowed Airflow-trigger failure with no client-visible
signal), one dead-end red herring (an expired cert that turned out to be
irrelevant to the actual traffic path), and one genuine infra outage. Once the
test-side bugs were fixed, the server itself behaved cleanly and predictably at
every concurrency level tried so far — see Results below.

## Why this took 3 days — the actual causal chain

1. **`ingestion_download_test.py`'s poll step reads response bodies it
   shouldn't.** `session.get(url, timeout=30)` with no `stream=True` is fine
   while the backend answers 202. The moment an asset becomes ready, that
   same "just checking status" call transparently follows the 302 redirect
   to S3 and starts pulling the real object — with only a 30s timeout. For
   multi-GB NISAR/OPERA assets this reliably blows up mid-transfer.
   Confirmed in `ingestion_results_small.json`: a NISAR `.h5` poll logged
   `poll_exception: IncompleteRead(7568408576 bytes read, 4880285696 more
   expected)` at `waited_secs=0.0` — ingestion had actually succeeded, and
   the test filed the real success as a generic crash. This is the literal
   "Airflow ran through fine, the code just didn't see it" symptom.

2. **Static, hardcoded asset lists in both `ingestion_download_test.py` and
   `perf_test.py` go warm forever.** The first time any path is hit,
   ingestion completes and the S3 object stays warm permanently. Every
   later run — and every later/higher-concurrency *stage within the same
   run* — increasingly draws already-warm files with no way to tell warm
   and cold apart (e.g. concurrency level 5 in
   `ingestion_results_small.json`: `avg_ingest_wait_secs: 0.0` across the
   board — not because ingestion got fast, because everything there was
   already warm). `perf_test.py`'s own code comments show this exact
   failure mode was already hit once before (a single fixed file per task
   warmed after ~85 VUs) and "fixed" by widening to a 59-path pool — but
   that pool is still finite and hits the same wall eventually, just later.

3. **A real, currently-live backend bug** in `http-data-access`
   (`AirflowIngestionClient.trigger_ingestion`,
   `src/http_data_access/utils/airflow_ingestion_client.py`): it re-triggers
   Airflow on *every* 202 poll (not just the first), re-authenticates via a
   shared token file (`hda.env`) on every call, and wraps the whole POST in
   a bare `except Exception: logger.warning(...)` with the comment
   "IMPORTANT: do not fail access path". A failed trigger (auth race,
   Airflow timeout, network blip) is completely invisible to any client —
   the caller just sees another 202, indistinguishable from "still queued".

4. **A red herring that cost real debugging time:** the TLS cert on
   `airflow-v3.dev.services.eodc.eu` genuinely is expired
   (`notAfter=Jun 15 11:23:36 2026 GMT`) — confirmed via `openssl s_client`
   and a direct `requests.post(..., verify=True)` call, which does fail
   with `SSLCertVerificationError` from an external vantage point. But a
   live OPERA cold-start trigger through the actual HDA backend succeeded
   30s later regardless — the real HDA→Airflow traffic apparently doesn't
   route through the same externally-visible hostname/cert. Lesson: verify
   from the same vantage point the real traffic takes before trusting a
   plausible-looking lead.

5. **`hda-config`'s `dev/collections/` has no committed YAML for VIIRS at
   all** (only `NISAR_L1_RSLC_BETA_V1_1.yaml`, `NISAR_L2_GCOV_BETA_V1_1.yaml`,
   `OPERA_L2_RTC-S1_V1.yaml`, `S2_MSI_L2A.yaml` exist under `dev/`; VIIRS
   only has a `prod/` config). Whatever config the live dev server actually
   uses for VIIRS isn't tracked in git anywhere findable.

6. **STAC API pagination is silently broken for every NASA/CMR-backed
   collection.** Root-caused by reading `eodc_cmr_nasa_nisar/eodag_cmr.py`:
   eodag core (`eodag/api/core.py:1831-1836`) pops `limit` /
   `next_page_token(_key)` out of kwargs and stores them only as
   `PreparedSearch.limit` / `PreparedSearch.next_page_token` attributes.
   The CMR plugin never reads those attributes — it reads
   `params.get("limit")` / `params.get("cmr_search_after")` from a plain
   dict that's never populated with them (`getattr(prep, "kwargs", {})`
   always returns `{}`, since `PreparedSearch` has no `.kwargs`). Net
   effect: every listing call silently falls back to the hardcoded default
   of **20 items**, with no real search-after cursor ever forwarded to CMR
   — regardless of `limit=` or of following the STAC `next` link (which
   itself appears/disappears inconsistently — comparing returned-count vs
   requested-limit, not tracking a real cursor — and is a dead end either
   way). Meanwhile `numberMatched` (e.g. 73,586,682 for `OPERA_L2_RTC-S1_V1`)
   comes from CMR's real `CMR-Hits` header via a completely different code
   path, so it's wildly disconnected from what's actually retrievable.
   `datetime` range filters are *not* stripped by core and do work as a
   real temporal filter — the only currently-working lever to get a
   different slice of items for these collections.

7. **The dev server was genuinely down** for part of this testing — plain
   `nginx 502 Bad Gateway`, upstream unreachable, confirmed via `curl` even
   against a previously-known-good direct-serve path. Real infra outage,
   not a testing-code issue — but worth a trivial health-check step before
   any deeper debugging next time, so an outage doesn't get mistaken for a
   code/logic problem.

8. **No small-scale "does this even work" gate existed before jumping to
   full concurrency.** Every existing script (`perf_test.py`,
   `ingestion_download_test.py`) goes straight into a real concurrency
   sweep, so any of the above issues only ever surfaced buried inside noisy
   20+ VU aggregate stats, never as a clean, isolated, single-item signal.

## File-by-file findings

**`perf_test.py`** — well-built overall (incremental result writes,
redirect-chain-aware error sampling, deliberate breakpoint logic that
excludes p95 as a stop condition). One real gap: `DATA_FILE_PATHS` mixes
fast direct-serve collections (`cop_marine`) with Airflow cold-start ones
(VIIRS/NISAR/OPERA) in the same weighted task set, and a 202 here is
recorded as an immediate hard failure with no poll/retry. At `VU_MAX=800`
against a finite ~61-path pool, a collision against a still-cold NASA asset
looks identical to real server overload in the error-rate stat — risk of a
false breakpoint that's really just catalog randomness, not capacity.
Recommend splitting the pool: keep only fast/warm-by-design collections in
the RPS ramp; leave ingestion-pattern collections to the dedicated
ingestion scripts. `RedirectOnlyUser` (`allow_redirects=False`) is good
prior art, worth reusing directly rather than re-deriving elsewhere.

**`ingestion_single_test.py`** — good minimal debug tool, but carries the
*identical* poll bug as `ingestion_download_test.py` (a non-streamed
`session.get` used as a status check, line ~42). Ironic given its purpose
is debugging that exact script — likely copy-pasted before the bug was
understood.

**`test_hda_performance.py` / `test_hda_availability.py`** — fine as
lightweight legacy health checks (weekly / every-20-min cadence).
`test_hda_performance.py` still uses `wait_time = between(3, 8)`, the exact
setting `perf_test.py`'s own comments say was found to barely generate
load — presumably intentional for a gentle recurring check, worth
confirming it wasn't just never revisited.

**`old_performance.py`** — appears to be dead code: near-identical to
`test_hda_performance.py`, not referenced by any
`.github/workflows/*.yml`. Candidate for deletion.

**`report_generator.py` / `ingestion_report_generator.py`** — solid, no
bugs found. Nice touches: total-RPS overlay to reveal saturation,
host/redirect-aware error grouping, path-wrapping for long file names in
PDF tables.

**`otel_push.py`** — fine; one fragility worth knowing: `flush()` calls
`_provider.shutdown()`, so any `record()` after `flush()` in the same
process would push into an already-shut-down provider. Not currently
triggered anywhere, just a footgun for future edits.

**`cleanup_pushgateway.py`** — simple, correct, fails fast on missing env
vars (appropriate for a manual utility).

**`search_collection.py`** — fine; deliberately shallow (`limit=2` per
collection), so it doesn't hit the CMR pagination bug itself — it was
never trying to get many items per collection in the first place.

**CI workflows** — `perf_test_new.yml`'s one-off cron (`0 4 13 7 *`) has no
year field, so it will silently refire every July 13th unless removed.
`perf_test_legacy.yml` (weekly) and `perf_test_new.yml` (`VU_MAX=800`
one-off) risk confusion over which is authoritative going forward.

## What's already fixed (`scripts/ingestion_ramp_smoke_test.py`)

New script directly addresses root causes #1, #2, and #8:

- Polls with `allow_redirects=False` and never reads a response body until
  readiness is confirmed — the poll step can no longer accidentally become
  a giant download.
- Draws assets from a fresh STAC pool per run, samples without replacement
  across stages, and persists a `seen`-cache (`ingestion_ramp_seen.json`)
  across runs — every result is tagged `cold` (saw a 202 first) or `warm`
  (ready on first poll) so the two are never averaged together.
- Paginates via `datetime`-range windows (the only lever that survives the
  CMR pagination bug, #6) instead of `limit`/`next`-link, which are both
  silently broken for these collections.
- Deliberately small, ascending ramp (default `1,3,5`) as the missing gate
  from #8, before scaling to `perf_test.py` / `ingestion_download_test.py`.
- Optional `KEEP_DOWNLOADS_SAMPLE` to keep a bounded number of real
  downloaded files on disk for manual spot-checking, without risking
  unbounded disk growth at higher VU counts.

Root causes #3–#7 are backend/infra-owned, not fixable from this repo:
#3 and #6 are worth raising with whoever owns `http-data-access` and
`eodag-server`/`eodc_cmr_nasa_nisar` respectively, since both affect more
than just this test suite.

`scripts/ingestion_ramp_report_generator.py` was added alongside it, mirroring
`ingestion_report_generator.py`'s PDF layout (outcome breakdown, ingestion-wait
and download-time charts vs. concurrency, per-level table) but adapted to this
script's cold/warm-tagged JSON schema:

```
python scripts/ingestion_ramp_report_generator.py opera_ramp_results_3.json result.pdf
```

## Confirmed-clean result (2026-07-16, OPERA_L2_RTC-S1_V1, post-fix)

Ramp `1,5,10,15,20` VUs, all genuinely cold-start, zero failures:

| VUs | Succeeded | Avg ingest wait | Max ingest wait | Avg download |
|----:|----------:|-----------------:|-----------------:|--------------:|
|   1 |       1/1 |             30.0s |             30.0s |         0.43s |
|   5 |       5/5 |             33.0s |             45.0s |         0.34s |
|  10 |     10/10 |             42.0s |             45.0s |         0.30s |
|  15 |     15/15 |             45.0s |             45.0s |         0.45s |
|  20 |     20/20 |             51.0s |             75.0s |         0.78s |

Ingest wait grows gently with concurrency (30s → 51s avg); no sign of
saturation yet at 20 concurrent cold-starts.

## Stress ramp, bigger steps (2026-07-16, OPERA_L2_RTC-S1_V1, `RAMP_LEVELS=10,20,30,40,50`)

Follow-up ramp at wider steps, per the request to push harder before
concluding the first result generalizes. 150 total attempts, every single one
genuinely cold-start (0 already-warm), **150/150 succeeded, 0 timeouts, 0
other failures**:

| VUs | Succeeded | Avg ingest wait | Max ingest wait | Avg download | Max download |
|----:|----------:|-----------------:|-----------------:|--------------:|--------------:|
|  10 |     10/10 |             39.0s |             45.0s |         0.35s |         0.74s |
|  20 |     20/20 |             49.5s |             75.0s |         0.50s |         1.13s |
|  30 |     30/30 |             60.0s |             90.0s |         0.67s |         1.64s |
|  40 |     40/40 |             68.3s |            105.0s |         0.44s |         1.10s |
|  50 |     50/50 |             80.7s |            135.0s |         0.43s |         1.60s |

Interactive charts (both ramps, hover for exact values):
`opera_ramp_chart.html` artifact · PDF report: `result.pdf`
(generate via `scripts/ingestion_ramp_report_generator.py`).

**Reading this correctly:** unlike the first ramp, this one shows a real,
steady trend, not noise — avg ingest wait grows roughly linearly with VU
count (≈ +10–13s per +10 VUs), and max grows faster (+15–30s per +10 VUs,
reaching 135s at 50 concurrent). This is Airflow/ingestion-side latency
scaling under concurrent trigger load, not a capacity cliff: there is still
zero error rate and zero timeouts even at 50 concurrent, and every attempt
stayed well inside the 180s "acceptable wait" bar. Download time (the actual
S3 transfer, once ready) stays flat and sub-2s throughout — the growth is
entirely in the ingestion/Airflow side, not in data transfer or the HDA
proxy itself.

## Pushing further: 60 → 100 VUs — where the trend actually turns into a signal

Continuation of the ramp above (same collection, same defaults,
`RAMP_LEVELS=60,70,80,90,100`), 350 more attempts, every one genuinely
cold-start:

| VUs | Succeeded | Within acceptable wait (≤180s) | Avg ingest wait | Max ingest wait | Avg download | Max download |
|----:|----------:|--------------------------------:|-----------------:|-----------------:|--------------:|--------------:|
|  60 |     60/60 |                            60/60 |             92.5s |            150.0s |         0.46s |         1.73s |
|  70 |     70/70 |                            70/70 |            104.1s |            180.0s |         0.63s |         2.01s |
|  80 |     80/80 |                          **78/80** |            112.1s |            195.0s |         0.64s |         1.58s |
|  90 |     90/90 |                          **67/90** |            137.5s |            225.0s |         0.51s |         1.52s |
| 100 |   100/100 |                        **79/100** |            133.1s |            240.0s |         0.42s |         1.56s |

**This is the real finding.** Still **zero hard failures and zero timeouts
even at 100 concurrent** — the ingestion path never breaks. But look at the
"within acceptable wait" column: it holds at 100% through 70 VUs, then drops
sharply — 2/80 miss the 180s bar, then 23/90, then 21/100. That's a genuine
capacity signal appearing specifically in the 70→90 VU range, not further
out. Max ingest wait keeps climbing past that point too (195s → 225s → 240s),
still comfortably under the 300s hard timeout, but the user-experience
threshold (180s "acceptable") is where the real degradation shows up first —
exactly the kind of soft-saturation signal `perf_test.py`'s own breakpoint
design philosophy (error rate / throughput-drop, not a hard latency cutoff)
was built to catch, just observed here from the ingestion side instead of
the RPS side. avg wait dips slightly from 90→100 (137.5s → 133.1s) — likely
normal run-to-run variance from which specific assets got drawn, not a real
recovery; the max column's steady climb is the cleaner signal to trust.

**Recommendation:** treat ~70-80 concurrent cold-start OPERA ingestions as
the practical comfort ceiling on this dev environment today — comfortably
below where anything actually fails, but past where a meaningful fraction of
users would notice slower-than-expected waits. Whoever owns the SLA should
set the real bar; this is the data point to set it against.

## OPERA vs. VIIRS vs. NISAR — why timing differs across collections

All three route through the *identical* code path (`FederatedEodagRouter` →
`AirflowIngestionClient` → the same `airflow_ingestion_endpoint`, the same
`eodag_data_access` DAG). The timing difference is not architectural — it's
the size/processing-level of the underlying product:

- **OPERA_L2_RTC-S1_V1** — derived Level-2 backscatter. Small: ~100KB `.h5`
  + ~9MB browse `.png`. Sub-2s downloads throughout every test run so far.
- **VIIRS JPSS2 LAI/FPAR** — coarse 500m vegetation-index tiles, also
  derived/reduced. Small: ~73KB–1.5MB `.h5`.
- **NISAR_L1_RSLC** — Level-1 Range-Doppler Single-Look-Complex SAR data,
  essentially raw-resolution radar, not a reduced product. Real evidence
  already in hand: a poll attempt in `ingestion_results_small.json` logged
  `IncompleteRead(7568408576 bytes read, 4880285696 more expected)` — **~12.4GB
  for one file.** `NISAR_L2_GCOV` (geocoded covariance) is more processed
  than L1_RSLC but still a much heavier product class than OPERA/VIIRS.

Product size affects two separate numbers, both of which scale with it:
ingestion/staging time (the DAG has to move the source product from NASA
Earthdata into EODC's S3) and download time (pure transfer, once ready).
Testing recommendations:

- Don't mix asset types when comparing collections — a run that randomly
  draws either a tiny `_BROWSE.png` or a multi-GB `.h5` for the same
  collection makes cross-collection comparison noisy. An `ASSET_SUFFIX_FILTER`
  option (test only `.png` across collections for an apples-to-apples
  ingest-latency comparison, separately from `.h5` for real transfer
  bandwidth) would clean this up if this becomes a recurring need.
- **Disk-space warning for NISAR specifically**: the script always writes
  the full download to a temp file before verifying its size, regardless of
  `KEEP_DOWNLOADS_SAMPLE` — testing NISAR `.h5` at concurrency N needs
  `N × ~12GB` of free space in `DOWNLOAD_DIR` at once. Start with
  `RAMP_LEVELS=1` only, same bootstrapping approach used for OPERA, before
  ever trying real concurrency on NISAR.
- Raise `DOWNLOAD_TIMEOUT_SECS` well above the 300s default for NISAR — not
  enough for a multi-GB transfer on most connections.

## SIG0 — not missing, just not in the STAC catalog at all

The colleague's "test SIG0" request couldn't be satisfied by browsing the
catalog because **it genuinely isn't there** — confirmed by paginating the
full STAC `/collections` listing (43 collections total, `search_collection.py`
follows the same `next`-link pagination pattern): no `sig0`/`sigma` match
anywhere.

Root cause, traced into `eodag-server/resources/`:
- `collections.yml:746` defines `SENTINEL1_SIG0_20M` with a real title/
  description/keywords — it exists as generic collection metadata.
- But `providers.yml:937-938` associates it with a specific provider, and
  the STAC service's `Dockerfile:21` sets
  `EODAG_PROVIDERS_WHITELIST=cop_dataspace,nasa,cop_cds,cop_ads,cop_ghsl,
  cop_marine,cop_dataspace_s3,eodc_topo4eo` — SIG0's provider isn't in that
  list, so it's filtered out of everything the STAC catalog exposes, even
  though the collection is fully defined.
- Separately, `hda-config/dev/collections/sigma0.yaml` (filename) defines
  `collection_name: SENTINEL1_SIG0_20M` (the actual ID) with
  `router_type: DirectFilepathRouter` — files are served straight from
  `/eodc/products/eodc.eu/S1_CSAR_IWGRDH/SIG0` on disk. **No Airflow, no S3
  staging, no cold-start pattern at all** — every request is always
  instantly available. (`AI4SAR_SIG0` is a private/restricted sibling of the
  same product, same router type.)

Practical consequence: `ingestion_ramp_smoke_test.py`'s STAC-based asset
discovery (`fetch_fresh_pool`) cannot find SIG0 assets — there is no STAC
entry point for this collection at all, and there's no filesystem or admin
API access from here to enumerate real file paths either
(`http_data_access/routers/admin.py` only has collection/token CRUD, no
file listing). The only confirmed-real SIG0 path available is the one
already hardcoded in `perf_test.py`/`old_performance.py`:
`/collections/SENTINEL1_SIG0_20M/V1M2R3/EQUI7_EU020M/E048N015T3/
SIG0_20260412T171426__VV_A015_E048N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif`.
Since this collection has no cold/warm distinction (always served instantly
from disk), reusing that one file across every concurrent request in a ramp
is methodologically valid here — unlike the NASA collections, there's no
warm-cache bias risk to worry about. It does mean the test measures raw
concurrent file-serving performance on one object/filesystem path, not
variety across many distinct files the way the OPERA ramps did.
