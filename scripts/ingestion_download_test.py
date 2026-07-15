"""
Separate, purpose-built test for HDA's on-demand "ingestion" assets — the
ones that don't serve immediately but respond with:
    202 {"detail": "Asset not available; ingestion has been requested"}
and only become downloadable once the backend has staged them.

This does NOT fit the RPS-ramp model in perf_test.py: that measures sustained
throughput on assets that respond fast. Here, each attempt is a single,
long-running flow (poll until ready -> download the whole file -> verify the
size -> delete it) and the real question is "how many of these can run at
once before some start failing or timing out" — a concurrency ceiling, not
a requests/second number.

Deletes every downloaded file immediately after verifying it (success or
failure) so this is safe to run in GitHub Actions without filling the
runner's disk, even at high concurrency.

Usage: python ingestion_download_test.py
"""
import gevent.monkey
gevent.monkey.patch_all()

import os, sys, json, random, time, logging
import requests
from gevent.pool import Pool

HDA_URL = os.environ.get("HDA_URL", "https://dev.hda.eodchosting.eu").rstrip("/")

# two different numbers on purpose, same reasoning as the main perf test's
# p95 SLA vs. hard timeout split: ACCEPTABLE is "still a good user experience"
# (2-3 min is fine for a cold, on-demand asset); MAX is "give up entirely,
# this attempt failed" — a generous safety margin above ACCEPTABLE so slow
# but still-working ingestion isn't counted as a failure outright, just
# flagged as slower than ideal.
ACCEPTABLE_INGEST_WAIT_SECS = float(os.environ.get("ACCEPTABLE_INGEST_WAIT_SECS", 180))
MAX_INGEST_WAIT_SECS = float(os.environ.get("MAX_INGEST_WAIT_SECS", 300))
POLL_INTERVAL_SECS = float(os.environ.get("POLL_INTERVAL_SECS", 10))
# separate budget for the actual transfer once the asset is ready — these are
# real satellite products, can be sizeable, and network conditions vary
DOWNLOAD_TIMEOUT_SECS = float(os.environ.get("DOWNLOAD_TIMEOUT_SECS", 300))
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "/tmp/hda_ingestion_test")

CONCURRENCY_LEVELS = [
    int(v.strip()) for v in os.environ.get("CONCURRENCY_LEVELS", "1,2,5,10,15,20").split(",")
    if v.strip()
]

RESULTS_JSON = os.environ.get("RESULTS_JSON", "ingestion_results.json")

# confirmed via a live scan (curl, following redirects) to actually return the
# 202 "ingestion has been requested" pattern — recently-generated satellite
# products (NISAR, OPERA, VIIRS) and a few climate-data-store timeseries/
# soil-moisture assets. Pulled from 8 collections; every entry here returned
# 202 at scan time, not guessed.
_DEFAULT_INGESTION_PATHS = ",".join([
    '/collections/data/nasa/NISAR_L1_RSLC_BETA_V1_1/NISAR_L1_PR_RSLC_010_164_D_076_2005_QPDH_A_20260120T140558_20260120T140633_X05010_N_F_J_001/NISAR_L1_PR_RSLC_010_164_D_076_2005_QPDH_A_20260120T140558_20260120T140633_X05010_N_F_J_001.png',
    '/collections/data/nasa/NISAR_L1_RSLC_BETA_V1_1/NISAR_L1_PR_RSLC_010_165_D_100_2005_DHDH_M_20260120T155930_20260120T155950_X05010_N_P_J_001/NISAR_L1_PR_RSLC_010_165_D_100_2005_DHDH_M_20260120T155930_20260120T155950_X05010_N_P_J_001.h5',
    '/collections/data/nasa/NISAR_L1_RSLC_BETA_V1_1/NISAR_L1_PR_RSLC_010_164_D_050_0005_NASV_A_20260120T135101_20260120T135129_X05010_N_P_J_001/NISAR_L1_PR_RSLC_010_164_D_050_0005_NASV_A_20260120T135101_20260120T135129_X05010_N_P_J_001.png',
    '/collections/data/nasa/NISAR_L1_RSLC_BETA_V1_1/NISAR_L1_PR_RSLC_010_164_D_072_2005_QPDH_A_20260120T140344_20260120T140418_X05010_N_F_J_001/NISAR_L1_PR_RSLC_010_164_D_072_2005_QPDH_A_20260120T140344_20260120T140418_X05010_N_F_J_001.h5',
    '/collections/data/nasa/NISAR_L1_RSLC_BETA_V1_1/NISAR_L1_PR_RSLC_010_164_D_077_2005_QPDH_A_20260120T140632_20260120T140648_X05010_N_P_J_001/NISAR_L1_PR_RSLC_010_164_D_077_2005_QPDH_A_20260120T140632_20260120T140648_X05010_N_P_J_001.h5',
    '/collections/data/nasa/NISAR_L1_RSLC_BETA_V1_1/NISAR_L1_PR_RSLC_010_164_D_074_2005_QPDH_A_20260120T140451_20260120T140526_X05010_N_F_J_001/NISAR_L1_PR_RSLC_010_164_D_074_2005_QPDH_A_20260120T140451_20260120T140526_X05010_N_F_J_001.h5',
    '/collections/data/nasa/NISAR_L1_RSLC_BETA_V1_1/NISAR_L1_PR_RSLC_010_164_D_048_0005_NASV_A_20260120T135000_20260120T135033_X05010_N_F_J_001/NISAR_L1_PR_RSLC_010_164_D_048_0005_NASV_A_20260120T135000_20260120T135033_X05010_N_F_J_001.png',
    '/collections/data/nasa/NISAR_L1_RSLC_BETA_V1_1/NISAR_L1_PR_RSLC_010_164_D_075_2005_QPDH_A_20260120T140525_20260120T140559_X05010_N_F_J_001/NISAR_L1_PR_RSLC_010_164_D_075_2005_QPDH_A_20260120T140525_20260120T140559_X05010_N_F_J_001.png',
    '/collections/data/nasa/NISAR_L1_RSLC_BETA_V1_1/NISAR_L1_PR_RSLC_010_164_D_049_0005_NASV_A_20260120T135032_20260120T135102_X05010_N_F_J_001/NISAR_L1_PR_RSLC_010_164_D_049_0005_NASV_A_20260120T135032_20260120T135102_X05010_N_F_J_001.h5',
    '/collections/data/nasa/NISAR_L1_RSLC_BETA_V1_1/NISAR_L1_PR_RSLC_010_164_D_073_2005_QPDH_A_20260120T140417_20260120T140452_X05010_N_F_J_001/NISAR_L1_PR_RSLC_010_164_D_073_2005_QPDH_A_20260120T140417_20260120T140452_X05010_N_F_J_001.png',
    '/collections/data/nasa/NISAR_L1_RSLC_BETA_V1_1/NISAR_L1_PR_RSLC_010_164_D_047_0005_NASV_A_20260120T134930_20260120T135001_X05010_N_F_J_001/NISAR_L1_PR_RSLC_010_164_D_047_0005_NASV_A_20260120T134930_20260120T135001_X05010_N_F_J_001.h5',
    '/collections/data/nasa/NISAR_L1_RSLC_BETA_V1_1/NISAR_L1_PR_RSLC_010_164_D_046_0005_NASV_A_20260120T134856_20260120T134931_X05010_N_F_J_001/NISAR_L1_PR_RSLC_010_164_D_046_0005_NASV_A_20260120T134856_20260120T134931_X05010_N_F_J_001.png',
    '/collections/data/nasa/NISAR_L1_RSLC_BETA_V1_1/NISAR_L1_PR_RSLC_010_164_A_040_0005_NASV_A_20260120T134533_20260120T134609_X05010_N_F_J_001/NISAR_L1_PR_RSLC_010_164_A_040_0005_NASV_A_20260120T134533_20260120T134609_X05010_N_F_J_001.png',
    '/collections/data/nasa/NISAR_L1_RSLC_BETA_V1_1/NISAR_L1_PR_RSLC_010_164_A_041_0005_NASV_A_20260120T134608_20260120T134638_X05010_N_F_J_001/NISAR_L1_PR_RSLC_010_164_A_041_0005_NASV_A_20260120T134608_20260120T134638_X05010_N_F_J_001.png',
    '/collections/data/nasa/NISAR_L1_RSLC_BETA_V1_1/NISAR_L1_PR_RSLC_010_164_A_045_0005_NASV_A_20260120T134813_20260120T134857_X05010_N_F_J_001/NISAR_L1_PR_RSLC_010_164_A_045_0005_NASV_A_20260120T134813_20260120T134857_X05010_N_F_J_001.h5',
    '/collections/data/nasa/NISAR_L2_GCOV_BETA_V1_1/NISAR_L2_PR_GCOV_010_165_D_100_2005_DHDH_M_20260120T155930_20260120T155950_X05010_N_P_J_001/NISAR_L2_PR_GCOV_010_165_D_100_2005_DHDH_M_20260120T155930_20260120T155950_X05010_N_P_J_001.png',
    '/collections/data/nasa/NISAR_L1_RSLC_BETA_V1_1/NISAR_L1_PR_RSLC_010_164_A_043_0005_NASV_A_20260120T134709_20260120T134739_X05010_N_F_J_001/NISAR_L1_PR_RSLC_010_164_A_043_0005_NASV_A_20260120T134709_20260120T134739_X05010_N_F_J_001.png',
    '/collections/data/nasa/NISAR_L1_RSLC_BETA_V1_1/NISAR_L1_PR_RSLC_010_164_A_042_0005_NASV_A_20260120T134637_20260120T134710_X05010_N_F_J_001/NISAR_L1_PR_RSLC_010_164_A_042_0005_NASV_A_20260120T134637_20260120T134710_X05010_N_F_J_001.h5',
    '/collections/data/nasa/NISAR_L2_GCOV_BETA_V1_1/NISAR_L2_PR_GCOV_010_164_D_076_2005_QPDH_A_20260120T140558_20260120T140633_X05010_N_F_J_001/NISAR_L2_PR_GCOV_010_164_D_076_2005_QPDH_A_20260120T140558_20260120T140633_X05010_N_F_J_001.h5',
    '/collections/data/nasa/NISAR_L2_GCOV_BETA_V1_1/NISAR_L2_PR_GCOV_010_164_D_072_2005_QPDH_A_20260120T140344_20260120T140418_X05010_N_F_J_001/NISAR_L2_PR_GCOV_010_164_D_072_2005_QPDH_A_20260120T140344_20260120T140418_X05010_N_F_J_001.h5',
    '/collections/data/nasa/NISAR_L2_GCOV_BETA_V1_1/NISAR_L2_PR_GCOV_010_164_D_071_2005_QPDH_A_20260120T140310_20260120T140345_X05010_N_F_J_001/NISAR_L2_PR_GCOV_010_164_D_071_2005_QPDH_A_20260120T140310_20260120T140345_X05010_N_F_J_001.png',
    '/collections/data/nasa/NISAR_L2_GCOV_BETA_V1_1/NISAR_L2_PR_GCOV_010_164_D_073_2005_QPDH_A_20260120T140417_20260120T140452_X05010_N_F_J_001/NISAR_L2_PR_GCOV_010_164_D_073_2005_QPDH_A_20260120T140417_20260120T140452_X05010_N_F_J_001.png',
    '/collections/data/nasa/NISAR_L2_GCOV_BETA_V1_1/NISAR_L2_PR_GCOV_010_164_D_046_0005_NASV_A_20260120T134856_20260120T134931_X05010_N_F_J_001/NISAR_L2_PR_GCOV_010_164_D_046_0005_NASV_A_20260120T134856_20260120T134931_X05010_N_F_J_001.h5',
    '/collections/data/nasa/NISAR_L2_GCOV_BETA_V1_1/NISAR_L2_PR_GCOV_010_164_D_048_0005_NASV_A_20260120T135000_20260120T135033_X05010_N_F_J_001/NISAR_L2_PR_GCOV_010_164_D_048_0005_NASV_A_20260120T135000_20260120T135033_X05010_N_F_J_001.h5',
    '/collections/data/nasa/NISAR_L2_GCOV_BETA_V1_1/NISAR_L2_PR_GCOV_010_164_A_044_0005_NASV_A_20260120T134738_20260120T134814_X05010_N_F_J_001/NISAR_L2_PR_GCOV_010_164_A_044_0005_NASV_A_20260120T134738_20260120T134814_X05010_N_F_J_001.png',
    '/collections/data/nasa/NISAR_L2_GCOV_BETA_V1_1/NISAR_L2_PR_GCOV_010_164_A_043_0005_NASV_A_20260120T134709_20260120T134739_X05010_N_F_J_001/NISAR_L2_PR_GCOV_010_164_A_043_0005_NASV_A_20260120T134709_20260120T134739_X05010_N_F_J_001.png',
    '/collections/data/nasa/NISAR_L2_GCOV_BETA_V1_1/NISAR_L2_PR_GCOV_010_164_A_045_0005_NASV_A_20260120T134813_20260120T134857_X05010_N_F_J_001/NISAR_L2_PR_GCOV_010_164_A_045_0005_NASV_A_20260120T134813_20260120T134857_X05010_N_F_J_001.h5',
    '/collections/data/nasa/NISAR_L2_GCOV_BETA_V1_1/NISAR_L2_PR_GCOV_010_164_D_077_2005_QPDH_A_20260120T140632_20260120T140648_X05010_N_P_J_001/NISAR_L2_PR_GCOV_010_164_D_077_2005_QPDH_A_20260120T140632_20260120T140648_X05010_N_P_J_001.h5',
    '/collections/data/nasa/NISAR_L2_GCOV_BETA_V1_1/NISAR_L2_PR_GCOV_010_164_D_075_2005_QPDH_A_20260120T140525_20260120T140559_X05010_N_F_J_001/NISAR_L2_PR_GCOV_010_164_D_075_2005_QPDH_A_20260120T140525_20260120T140559_X05010_N_F_J_001.png',
    '/collections/data/nasa/NISAR_L2_GCOV_BETA_V1_1/NISAR_L2_PR_GCOV_010_164_D_070_2005_QPDH_A_20260120T140236_20260120T140311_X05010_N_F_J_001/NISAR_L2_PR_GCOV_010_164_D_070_2005_QPDH_A_20260120T140236_20260120T140311_X05010_N_F_J_001.h5',
    '/collections/data/nasa/NISAR_L2_GCOV_BETA_V1_1/NISAR_L2_PR_GCOV_010_164_D_047_0005_NASV_A_20260120T134930_20260120T135001_X05010_N_F_J_001/NISAR_L2_PR_GCOV_010_164_D_047_0005_NASV_A_20260120T134930_20260120T135001_X05010_N_F_J_001.png',
    '/collections/data/nasa/NISAR_L2_GCOV_BETA_V1_1/NISAR_L2_PR_GCOV_010_164_D_050_0005_NASV_A_20260120T135101_20260120T135129_X05010_N_P_J_001/NISAR_L2_PR_GCOV_010_164_D_050_0005_NASV_A_20260120T135101_20260120T135129_X05010_N_P_J_001.png',
    '/collections/data/nasa/NISAR_L2_GCOV_BETA_V1_1/NISAR_L2_PR_GCOV_010_164_D_049_0005_NASV_A_20260120T135032_20260120T135102_X05010_N_F_J_001/NISAR_L2_PR_GCOV_010_164_D_049_0005_NASV_A_20260120T135032_20260120T135102_X05010_N_F_J_001.h5',
    '/collections/data/nasa/NISAR_L2_GCOV_BETA_V1_1/NISAR_L2_PR_GCOV_010_164_A_042_0005_NASV_A_20260120T134637_20260120T134710_X05010_N_F_J_001/NISAR_L2_PR_GCOV_010_164_A_042_0005_NASV_A_20260120T134637_20260120T134710_X05010_N_F_J_001.h5',
    '/collections/data/nasa/NISAR_L2_GCOV_BETA_V1_1/NISAR_L2_PR_GCOV_010_164_A_040_0005_NASV_A_20260120T134533_20260120T134609_X05010_N_F_J_001/NISAR_L2_PR_GCOV_010_164_A_040_0005_NASV_A_20260120T134533_20260120T134609_X05010_N_F_J_001.h5',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166572-IW3_20260705T013707Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166572-IW3_20260705T013707Z_20260715T021510Z_S1C_30_v1.0.h5',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166573-IW2_20260705T013709Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166573-IW2_20260705T013709Z_20260715T021510Z_S1C_30_v1.0_BROWSE.png',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166572-IW2_20260705T013706Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166572-IW2_20260705T013706Z_20260715T021510Z_S1C_30_v1.0.h5',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166573-IW1_20260705T013708Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166573-IW1_20260705T013708Z_20260715T021510Z_S1C_30_v1.0.h5',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166573-IW3_20260705T013710Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166573-IW3_20260705T013710Z_20260715T021510Z_S1C_30_v1.0_BROWSE.png',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166572-IW1_20260705T013705Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166572-IW1_20260705T013705Z_20260715T021510Z_S1C_30_v1.0.h5',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166574-IW1_20260705T013711Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166574-IW1_20260705T013711Z_20260715T021510Z_S1C_30_v1.0.h5',
    '/collections/data/nasa/NISAR_L1_RSLC_BETA_V1_1/NISAR_L1_PR_RSLC_010_164_A_044_0005_NASV_A_20260120T134738_20260120T134814_X05010_N_F_J_001/NISAR_L1_PR_RSLC_010_164_A_044_0005_NASV_A_20260120T134738_20260120T134814_X05010_N_F_J_001.png',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166571-IW3_20260705T013705Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166571-IW3_20260705T013705Z_20260715T021510Z_S1C_30_v1.0.h5',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166570-IW3_20260705T013702Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166570-IW3_20260705T013702Z_20260715T021510Z_S1C_30_v1.0_BROWSE.png',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166570-IW2_20260705T013701Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166570-IW2_20260705T013701Z_20260715T021510Z_S1C_30_v1.0_BROWSE.png',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166569-IW3_20260705T013659Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166569-IW3_20260705T013659Z_20260715T021510Z_S1C_30_v1.0.h5',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166571-IW2_20260705T013704Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166571-IW2_20260705T013704Z_20260715T021510Z_S1C_30_v1.0_BROWSE.png',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166569-IW2_20260705T013658Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166569-IW2_20260705T013658Z_20260715T021510Z_S1C_30_v1.0_BROWSE.png',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166568-IW3_20260705T013656Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166568-IW3_20260705T013656Z_20260715T021510Z_S1C_30_v1.0.h5',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166569-IW1_20260705T013657Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166569-IW1_20260705T013657Z_20260715T021510Z_S1C_30_v1.0_BROWSE.png',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166567-IW3_20260705T013654Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166567-IW3_20260705T013654Z_20260715T021510Z_S1C_30_v1.0_BROWSE.png',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166571-IW1_20260705T013703Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166571-IW1_20260705T013703Z_20260715T021510Z_S1C_30_v1.0_BROWSE.png',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166568-IW1_20260705T013654Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166568-IW1_20260705T013654Z_20260715T021510Z_S1C_30_v1.0_BROWSE.png',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166568-IW2_20260705T013655Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166568-IW2_20260705T013655Z_20260715T021510Z_S1C_30_v1.0.h5',
    '/collections/data/cop_dataspace_s3/S2_MSI_L2A/S2B_MSIL2A_20260715T044659_N0512_R076_T50XNH_20260715T063750/MSK_CLDPRB_20m',
    '/collections/data/cop_dataspace_s3/S2_MSI_L1C/S2B_MSIL1C_20260715T044659_N0512_R076_T50WME_20260715T063720/MSK_DETFOO_B06',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h29v03.002.2026195145100/VJ215A2H.A2026185.h29v03.002.2026195145100.1.jpg',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h06v03.002.2026195145009/VJ215A2H.A2026185.h06v03.002.2026195145009.1.jpg',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h00v10.002.2026195145152/VJ215A2H.A2026185.h00v10.002.2026195145152.h5',
    '/collections/data/cop_dataspace_s3/S2_MSI_L1C/S2B_MSIL1C_20260715T044659_N0512_R076_T47VMH_20260715T063720/GENERAL_QUALITY.xml',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h35v10.002.2026195145140/VJ215A2H.A2026185.h35v10.002.2026195145140.1.jpg',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h31v13.002.2026195145128/VJ215A2H.A2026185.h31v13.002.2026195145128.h5',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h28v14.002.2026195145159/VJ215A2H.A2026185.h28v14.002.2026195145159.h5',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h01v11.002.2026195145133/VJ215A2H.A2026185.h01v11.002.2026195145133.1.jpg',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h28v03.002.2026195145224/VJ215A2H.A2026185.h28v03.002.2026195145224.1.jpg',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h02v10.002.2026195145250/VJ215A2H.A2026185.h02v10.002.2026195145250.h5',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h35v08.002.2026195145240/VJ215A2H.A2026185.h35v08.002.2026195145240.h5',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h07v03.002.2026195145226/VJ215A2H.A2026185.h07v03.002.2026195145226.1.jpg',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h02v09.002.2026195145226/VJ215A2H.A2026185.h02v09.002.2026195145226.1.jpg',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h09v02.002.2026195145249/VJ215A2H.A2026185.h09v02.002.2026195145249.1.jpg',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h03v10.002.2026195145253/VJ215A2H.A2026185.h03v10.002.2026195145253.h5',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h00v09.002.2026195145251/VJ215A2H.A2026185.h00v09.002.2026195145251.1.jpg',
    '/collections/data/cop_dataspace_s3/S2_MSI_L2A/S2B_MSIL2A_20260715T044659_N0512_R076_T47VMK_20260715T064941/MTD_MSIL2A.xml',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h35v09.002.2026195145255/VJ215A2H.A2026185.h35v09.002.2026195145255.1.jpg',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h33v08.002.2026195145256/VJ215A2H.A2026185.h33v08.002.2026195145256.h5',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h11v08.002.2026195145242/VJ215A2H.A2026185.h11v08.002.2026195145242.h5',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h26v02.002.2026195145229/VJ215A2H.A2026185.h26v02.002.2026195145229.h5',
    '/collections/data/cop_cds/reanalysis-era5-land-timeseries/reanalysis-era5-land-timeseries_16.4_48.2_2025-01-01_2026-06-01/2m_temperature',
    '/collections/data/cop_cds/reanalysis-era5-land-timeseries/reanalysis-era5-land-timeseries_16.4_48.3_2025-01-01_2026-06-01/surface_thermal_radiation_downwards',
    '/collections/data/cop_cds/reanalysis-era5-land-timeseries/reanalysis-era5-land-timeseries_16.3_48.2_2025-01-01_2026-06-01/2m_dewpoint_temperature',
    '/collections/data/cop_cds/reanalysis-era5-land-timeseries/reanalysis-era5-land-timeseries_16.3_48.3_2025-01-01_2026-06-01/soil_temperature_level_3',
    '/collections/data/cop_cds/reanalysis-era5-land-timeseries/reanalysis-era5-land-timeseries_16.5_48.2_2025-01-01_2026-06-01/volumetric_soil_water_level_1',
    '/collections/data/cop_cds/reanalysis-era5-land-timeseries/reanalysis-era5-land-timeseries_16.3_48.1_2025-01-01_2026-06-01/10m_u_component_of_wind',
    '/collections/data/cop_cds/reanalysis-era5-land-timeseries/reanalysis-era5-land-timeseries_16.5_48.3_2025-01-01_2026-06-01/volumetric_soil_water_level_1',
    '/collections/data/cop_cds/satellite-soil-moisture/satellite-soil-moisture_2025-01-01_2026-06-01_daily_v202505_icdr/freeze_thaw_classification',
    '/collections/data/cop_cds/reanalysis-era5-land-timeseries/reanalysis-era5-land-timeseries_16.4_48.1_2025-01-01_2026-06-01/volumetric_soil_water_level_3',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h27v09.002.2026195145255/VJ215A2H.A2026185.h27v09.002.2026195145255.h5',
    '/collections/data/cop_cds/reanalysis-era5-land-timeseries/reanalysis-era5-land-timeseries_16.5_48.1_2025-01-01_2026-06-01/soil_temperature_level_4',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166570-IW1_20260705T013700Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166570-IW1_20260705T013700Z_20260715T021510Z_S1C_30_v1.0.h5',
])
INGESTION_PATHS = [
    p.strip() for p in os.environ.get("INGESTION_PATHS", _DEFAULT_INGESTION_PATHS).split(",")
    if p.strip()
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)


def attempt_one(path):
    # single end-to-end flow for one simulated user: poll until the asset is
    # ready, download the whole thing for real, verify the byte count, then
    # delete it — never leaves a file behind, success or failure.
    url = f"{HDA_URL}{path}"
    record = {"path": path, "started_at": time.time()}
    session = requests.Session()

    waited = 0.0
    ready = False
    saw_ingesting = False
    while waited <= MAX_INGEST_WAIT_SECS:
        try:
            resp = session.get(url, timeout=30)
        except requests.RequestException as exc:
            record["outcome"] = f"poll_exception: {exc}"
            record["waited_secs"] = waited
            return record
        if resp.status_code == 200:
            ready = True
            break
        if resp.status_code == 202:
            saw_ingesting = True
        else:
            record["outcome"] = f"poll_unexpected_status_{resp.status_code}"
            record["waited_secs"] = waited
            return record
        time.sleep(POLL_INTERVAL_SECS)
        waited += POLL_INTERVAL_SECS

    record["waited_secs"] = waited
    record["saw_ingesting"] = saw_ingesting
    record["within_acceptable_wait"] = waited <= ACCEPTABLE_INGEST_WAIT_SECS

    if not ready:
        record["outcome"] = "ingestion_timeout"
        return record

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    tmp_path = os.path.join(DOWNLOAD_DIR, f"dl_{os.getpid()}_{time.time_ns()}.bin")
    dl_started = time.time()
    try:
        with session.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT_SECS) as resp:
            if resp.status_code >= 400:
                record["outcome"] = f"download_failed_status_{resp.status_code}"
                return record
            expected_size = resp.headers.get("Content-Length")
            expected_size = int(expected_size) if expected_size else None
            bytes_written = 0
            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
                    bytes_written += len(chunk)
        record["download_secs"] = time.time() - dl_started
        record["bytes_downloaded"] = bytes_written
        record["expected_size"] = expected_size
        size_ok = expected_size is None or bytes_written == expected_size
        record["outcome"] = "success" if size_ok else "size_mismatch"
    except requests.RequestException as exc:
        record["outcome"] = f"download_exception: {exc}"
    finally:
        # always clean up — this is the whole point: verify a real download
        # happened, then leave no trace, regardless of what happened above.
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    record["total_secs"] = time.time() - record["started_at"]
    return record


def run_concurrency_level(n):
    pool = Pool(n)
    greenlets = [pool.spawn(attempt_one, random.choice(INGESTION_PATHS)) for _ in range(n)]
    # generous overall join timeout: every greenlet's own worst case is
    # MAX_INGEST_WAIT_SECS + DOWNLOAD_TIMEOUT_SECS: give the whole batch that
    # much plus a margin, rather than killing slow-but-still-working attempts
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
    within_acceptable = sum(1 for r in succeeded if r.get("within_acceptable_wait"))
    slower_than_acceptable = len(succeeded) - within_acceptable
    timed_out = sum(1 for r in results if r.get("outcome") == "ingestion_timeout")
    other_failed = len(results) - len(succeeded) - timed_out
    ingest_waits = [r["waited_secs"] for r in results if "waited_secs" in r]
    download_secs = [r["download_secs"] for r in results if "download_secs" in r]
    return {
        "attempted": n,
        "succeeded": len(succeeded),
        "succeeded_within_acceptable_wait": within_acceptable,
        "succeeded_but_slower_than_acceptable": slower_than_acceptable,
        "timed_out_waiting_for_ingestion": timed_out,
        "other_failures": other_failed,
        "avg_ingest_wait_secs": sum(ingest_waits) / len(ingest_waits) if ingest_waits else None,
        "max_ingest_wait_secs": max(ingest_waits) if ingest_waits else None,
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
                    "acceptable_ingest_wait_secs": ACCEPTABLE_INGEST_WAIT_SECS,
                    "max_ingest_wait_secs": MAX_INGEST_WAIT_SECS,
                    "poll_interval_secs": POLL_INTERVAL_SECS,
                    "download_timeout_secs": DOWNLOAD_TIMEOUT_SECS,
                    "concurrency_levels": CONCURRENCY_LEVELS,
                },
                "levels": levels,
            },
            f,
            indent=2,
        )


def main():
    log.info("Ingestion+download concurrency test against %s", HDA_URL)
    log.info("Concurrency levels: %s | pool of %d ingestion-requiring assets",
              CONCURRENCY_LEVELS, len(INGESTION_PATHS))

    levels = {}
    for n in CONCURRENCY_LEVELS:
        log.info("Testing %d parallel ingestion+download attempts...", n)
        results = run_concurrency_level(n)
        summary = summarize(n, results)
        levels[n] = summary
        write_results(levels)  # incremental, same reasoning as perf_test.py
        log.info(
            "  %d/%d succeeded (%d within %.0fs acceptable wait, %d succeeded but slower), "
            "%d timed out waiting for ingestion, %d other failures "
            "(avg ingest wait %.1fs, avg download %.1fs)",
            summary["succeeded"], n, summary["succeeded_within_acceptable_wait"],
            ACCEPTABLE_INGEST_WAIT_SECS, summary["succeeded_but_slower_than_acceptable"],
            summary["timed_out_waiting_for_ingestion"], summary["other_failures"],
            summary["avg_ingest_wait_secs"] or 0.0, summary["avg_download_secs"] or 0.0,
        )
        if summary["succeeded"] == 0 and n > CONCURRENCY_LEVELS[0]:
            log.warning("Zero successes at %d concurrent — stopping early, no point testing higher", n)
            break

    log.info("Done. Results written to %s", RESULTS_JSON)


if __name__ == "__main__":
    main()
