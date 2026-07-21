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

FULL_TIF_PATH = os.environ.get(
    "FULL_TIF_PATH",
    "/collections/sen2like/2025/06/11/EU010M_E049N015T1_20250611T101041_B04.tif",
)

# every path below verified live (curl -sL) to return 200. Sizes ~73KB-185MB.
# All redirect (302) to object storage on other hosts.
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
    '/collections/data/cop_marine/MEDSEA_MULTIYEAR_PHY_006_004/MED-MFC_006_004_mask_bathy/native',
    '/collections/data/cop_marine/GLOBAL_MULTIYEAR_WAV_001_032/WAVERYSV1_bathymeter/native',
    '/collections/data/cop_marine/NWSHELF_MULTIYEAR_PHY_004_009/metoffice_foam1_amm7_NWS_BED_dm19930102/native',
    '/collections/data/cop_marine/NWSHELF_MULTIYEAR_PHY_004_009/metoffice_foam1_amm7_NWS_BED_dm19930104/native',
    '/collections/data/cop_marine/NWSHELF_MULTIYEAR_PHY_004_009/metoffice_foam1_amm7_NWS_BED_dm19930103/native',
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

_STAC_DISCOVERED_PATHS_ROUND2 = [
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h31v13.002.2026195145128/VJ215A2H.A2026185.h31v13.002.2026195145128.h5',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h00v10.002.2026195145152/VJ215A2H.A2026185.h00v10.002.2026195145152.h5',
    '/collections/data/nasa/VIIRS_JPSS2_Leaf_Area_Index_FPAR_8-Day_L4_Global_500m_SIN_Grid_V002/VJ215A2H.A2026185.h35v08.002.2026195145240/VJ215A2H.A2026185.h35v08.002.2026195145240.h5',
    '/collections/data/nasa/NISAR_L1_RSLC_BETA_V1_1/NISAR_L1_PR_RSLC_010_165_D_100_2005_DHDH_M_20260120T155930_20260120T155950_X05010_N_P_J_001/NISAR_L1_PR_RSLC_010_165_D_100_2005_DHDH_M_20260120T155930_20260120T155950_X05010_N_P_J_001.h5',
    '/collections/data/nasa/NISAR_L1_RSLC_BETA_V1_1/NISAR_L1_PR_RSLC_010_164_A_042_0005_NASV_A_20260120T134637_20260120T134710_X05010_N_F_J_001/NISAR_L1_PR_RSLC_010_164_A_042_0005_NASV_A_20260120T134637_20260120T134710_X05010_N_F_J_001.h5',
    '/collections/data/nasa/NISAR_L1_RSLC_BETA_V1_1/NISAR_L1_PR_RSLC_010_164_A_043_0005_NASV_A_20260120T134709_20260120T134739_X05010_N_F_J_001/NISAR_L1_PR_RSLC_010_164_A_043_0005_NASV_A_20260120T134709_20260120T134739_X05010_N_F_J_001.png',
    '/collections/data/nasa/NISAR_L2_GCOV_BETA_V1_1/NISAR_L2_PR_GCOV_010_164_A_044_0005_NASV_A_20260120T134738_20260120T134814_X05010_N_F_J_001/NISAR_L2_PR_GCOV_010_164_A_044_0005_NASV_A_20260120T134738_20260120T134814_X05010_N_F_J_001.png',
    '/collections/data/nasa/NISAR_L2_GCOV_BETA_V1_1/NISAR_L2_PR_GCOV_010_164_A_045_0005_NASV_A_20260120T134813_20260120T134857_X05010_N_F_J_001/NISAR_L2_PR_GCOV_010_164_A_045_0005_NASV_A_20260120T134813_20260120T134857_X05010_N_F_J_001.h5',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166572-IW3_20260705T013707Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166572-IW3_20260705T013707Z_20260715T021510Z_S1C_30_v1.0.h5',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166569-IW1_20260705T013657Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166569-IW1_20260705T013657Z_20260715T021510Z_S1C_30_v1.0_BROWSE.png',
    '/collections/data/nasa/OPERA_L2_RTC-S1_V1/OPERA_L2_RTC-S1_T078-166571-IW1_20260705T013703Z_20260715T021510Z_S1C_30_v1.0/OPERA_L2_RTC-S1_T078-166571-IW1_20260705T013703Z_20260715T021510Z_S1C_30_v1.0_BROWSE.png',
]
_DEFAULT_DATA_FILE_PATHS = ",".join(
    [FULL_TIF_PATH, TIF_PATH] + _STAC_DISCOVERED_PATHS + _STAC_DISCOVERED_PATHS_ROUND2
)
DATA_FILE_PATHS = [
    p.strip() for p in os.environ.get(
        "DATA_FILE_PATHS", _DEFAULT_DATA_FILE_PATHS
    ).split(",") if p.strip()
]

# SIG0_SAME_FILE=true reuses one fixed file (isolates origin-side load from
# storage-backend load); false (default) picks a different real file per
# request. Compare both at the same VU count to tell the two apart.
_SIG0_TILE_E048N015T3 = [
    f"/collections/SENTINEL1_SIG0_20M/V1M2R3/EQUI7_EU020M/E048N015T3/{f}" for f in [
        "SIG0_20251029T165046__VH_A146_E048N015T3_EU020M_V1M2R3_S1AIWGRDH_TUWIEN.tif",
        "SIG0_20251029T165046__VV_A146_E048N015T3_EU020M_V1M2R3_S1AIWGRDH_TUWIEN.tif",
        "SIG0_20251029T165111__VH_A146_E048N015T3_EU020M_V1M2R3_S1AIWGRDH_TUWIEN.tif",
        "SIG0_20251029T165111__VV_A146_E048N015T3_EU020M_V1M2R3_S1AIWGRDH_TUWIEN.tif",
        "SIG0_20251029T165136__VH_A146_E048N015T3_EU020M_V1M2R3_S1AIWGRDH_TUWIEN.tif",
        "SIG0_20251029T165136__VV_A146_E048N015T3_EU020M_V1M2R3_S1AIWGRDH_TUWIEN.tif",
        "SIG0_20251030T053329__VH_D066_E048N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif",
        "SIG0_20251030T053329__VV_D066_E048N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif",
        "SIG0_20251030T053354__VH_D066_E048N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif",
        "SIG0_20251030T053354__VV_D066_E048N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif",
        "SIG0_20251030T053419__VH_D066_E048N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif",
        "SIG0_20251030T053419__VV_D066_E048N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif",
        "SIG0_20251031T052608__VH_D168_E048N015T3_EU020M_V1M2R3_S1AIWGRDH_TUWIEN.tif",
        "SIG0_20251031T052608__VV_D168_E048N015T3_EU020M_V1M2R3_S1AIWGRDH_TUWIEN.tif",
        "SIG0_20251031T052633__VH_D168_E048N015T3_EU020M_V1M2R3_S1AIWGRDH_TUWIEN.tif",
        "SIG0_20251031T052633__VV_D168_E048N015T3_EU020M_V1M2R3_S1AIWGRDH_TUWIEN.tif",
        "SIG0_20251031T052658__VH_D168_E048N015T3_EU020M_V1M2R3_S1AIWGRDH_TUWIEN.tif",
        "SIG0_20251031T052658__VV_D168_E048N015T3_EU020M_V1M2R3_S1AIWGRDH_TUWIEN.tif",
        "SIG0_20251031T052723__VH_D168_E048N015T3_EU020M_V1M2R3_S1AIWGRDH_TUWIEN.tif",
        "SIG0_20251031T052723__VV_D168_E048N015T3_EU020M_V1M2R3_S1AIWGRDH_TUWIEN.tif",
        "SIG0_20251101T051704__VH_D095_E048N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif",
        "SIG0_20251101T051704__VV_D095_E048N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif",
        "SIG0_20251101T051729__VH_D095_E048N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif",
        "SIG0_20251101T051729__VV_D095_E048N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif",
        "SIG0_20251101T051754__VH_D095_E048N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif",
        "SIG0_20251101T051754__VV_D095_E048N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif",
        "SIG0_20251101T171533__VH_A015_E048N015T3_EU020M_V1M2R3_S1AIWGRDH_TUWIEN.tif",
        "SIG0_20251101T171533__VV_A015_E048N015T3_EU020M_V1M2R3_S1AIWGRDH_TUWIEN.tif",
        "SIG0_20251101T171558__VH_A015_E048N015T3_EU020M_V1M2R3_S1AIWGRDH_TUWIEN.tif",
        "SIG0_20251101T171558__VV_A015_E048N015T3_EU020M_V1M2R3_S1AIWGRDH_TUWIEN.tif",
    ]
]
_SIG0_TILE_E051N015T3 = [
    f"/collections/SENTINEL1_SIG0_20M/V1M2R3/EQUI7_EU020M/E051N015T3/{f}" for f in [
        "SIG0_20251029T045228__VH_D051_E051N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif",
        "SIG0_20251029T045228__VV_D051_E051N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif",
        "SIG0_20251029T045253__VH_D051_E051N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif",
        "SIG0_20251029T045253__VV_D051_E051N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif",
        "SIG0_20251029T045318__VH_D051_E051N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif",
        "SIG0_20251029T045318__VV_D051_E051N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif",
    ]
]
_DEFAULT_SIG0_PATHS = ",".join(_SIG0_TILE_E048N015T3 + _SIG0_TILE_E051N015T3)
SIG0_PATHS = [
    p.strip() for p in os.environ.get("SIG0_PATHS", _DEFAULT_SIG0_PATHS).split(",") if p.strip()
]
SIG0_SAME_FILE = os.environ.get("SIG0_SAME_FILE", "false").strip().lower() == "true"

# "full" (default): whole-object GET. "range": small windowed read instead —
# models opening many files and pulling a small value out of each.
SIG0_ACCESS_MODE = os.environ.get("SIG0_ACCESS_MODE", "full").strip().lower()

VU_START     = int(os.environ.get("VU_START", 5))
VU_STEP      = int(os.environ.get("VU_STEP", 10))
VU_MAX       = int(os.environ.get("VU_MAX", 200))
SPAWN_RATE   = int(os.environ.get("SPAWN_RATE", 10))
STAGE_SECS   = int(os.environ.get("STAGE_SECS", 90))
WARMUP_SECS  = int(os.environ.get("WARMUP_SECS", 15))  # excluded from stats

# soak mode: re-test a fixed VU list for longer per stage instead of ramping.
SOAK_VUS = [int(v.strip()) for v in os.environ.get("SOAK_VUS", "").split(",") if v.strip()]
SOAK_STAGE_SECS = int(os.environ.get("SOAK_STAGE_SECS", 600))

RANGE_TILE_BYTES  = int(os.environ.get("RANGE_TILE_BYTES", 256 * 1024))
RANGE_CHUNK_BYTES = int(os.environ.get("RANGE_CHUNK_BYTES", 4 * 1024 * 1024))

# breakpoint = ERROR_RATE_THRESHOLD or THROUGHPUT_DROP_THRESHOLD breached,
# BREACH_STAGES_TO_CONFIRM stages in a row. p95 is measured/logged but does
# NOT stop the ramp (no objective SLA bar to check it against here).
ERROR_RATE_THRESHOLD = float(os.environ.get("ERROR_RATE_THRESHOLD", 0.05))
P95_THRESHOLD_SECS   = float(os.environ.get("P95_THRESHOLD_SECS", 5.0))
FULL_DOWNLOAD_P95_THRESHOLD_SECS = float(os.environ.get("FULL_DOWNLOAD_P95_THRESHOLD_SECS", 60.0))
BREACH_STAGES_TO_CONFIRM = int(os.environ.get("BREACH_STAGES_TO_CONFIRM", 2))
THROUGHPUT_DROP_THRESHOLD = float(os.environ.get("THROUGHPUT_DROP_THRESHOLD", 0.15))

RESULTS_JSON = os.environ.get("RESULTS_JSON", "perf_results.json")

# TASK_MODE: "e2e" (default, mixed HdaUser traffic) | "redirect_only"
# (RedirectOnlyUser, origin's 302 only) | "sig0_only" (Sig0OnlyUser).
TASK_MODE = os.environ.get("TASK_MODE", "e2e")

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)

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


class TracedRequestMixin:
    def _traced_request(self, method, path, name, headers=None, consume_body=False):
        request_fn = self.client.get if method == "GET" else self.client.head
        try:
            with request_fn(
                path, name=name, headers=headers, stream=consume_body, catch_response=True
            ) as response:
                if response.status_code >= 400:
                    response.failure(f"status {response.status_code}")
                    _record_error_sample(path, response=response)
                    return
                if response.status_code == 202:
                    # ingestion-pending stub, not the real file — count as failure
                    response.failure("status 202 (ingestion pending, not the real file)")
                    _record_error_sample(path, response=response)
                    return
                if consume_body:
                    for _ in response.iter_content(chunk_size=1024 * 1024):
                        pass
        except Exception as exc:
            _record_error_sample(path, exc=exc)
            raise


class HdaUser(TracedRequestMixin, HttpUser):
    host = HDA_URL
    wait_time = between(0.1, 0.5)

    @task(2)
    def get_zarr(self):
        self._traced_request("GET", ZARR_PATH, "GET_zarray")

    @task(1)
    def head_geotiff(self):
        self._traced_request("HEAD", random.choice(DATA_FILE_PATHS), "HEAD_tif")

    @task(3)
    def get_geotiff_tile(self):
        self._traced_request(
            "GET", random.choice(DATA_FILE_PATHS), "GET_tif_range_tile",
            headers={"Range": f"bytes=0-{RANGE_TILE_BYTES - 1}"},
        )

    @task(1)
    def get_geotiff_chunk(self):
        self._traced_request(
            "GET", random.choice(DATA_FILE_PATHS), "GET_tif_range_chunk",
            headers={"Range": f"bytes=0-{RANGE_CHUNK_BYTES - 1}"}, consume_body=True,
        )

    @task(1)
    def get_geotiff_full(self):
        self._traced_request("GET", random.choice(DATA_FILE_PATHS), "GET_tif_full", consume_body=True)


class Sig0OnlyUser(TracedRequestMixin, HttpUser):
    host = HDA_URL
    wait_time = between(0.1, 0.5)

    @task
    def get_sig0(self):
        path = SIG0_PATHS[0] if SIG0_SAME_FILE else random.choice(SIG0_PATHS)
        if SIG0_ACCESS_MODE == "range":
            self._traced_request(
                "GET", path, "GET_sig0_range",
                headers={"Range": f"bytes=0-{RANGE_TILE_BYTES - 1}"},
                consume_body=True,
            )
        else:
            self._traced_request("GET", path, "GET_sig0_full", consume_body=True)


class RedirectOnlyUser(HttpUser):
    # every DATA_FILE_PATHS entry redirects (302) except TIF_PATH (SIG0,
    # DirectFilepathRouter — serves straight from disk, 200 not 302). That
    # one entry always fails here by design; use TASK_MODE=sig0_only for it.
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
                response.failure(f"expected a redirect, got status {response.status_code}")
                _record_error_sample(path, response=response)


ACTIVE_USER_CLASS = {
    "redirect_only": RedirectOnlyUser,
    "sig0_only": Sig0OnlyUser,
}.get(TASK_MODE, HdaUser)


def collect_stage_stats(env):
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
    reasons = []
    for endpoint, s in stage_stats.items():
        if s["err"] > ERROR_RATE_THRESHOLD:
            reasons.append(f"{endpoint}: error_rate={s['err']:.1%} > {ERROR_RATE_THRESHOLD:.0%}")

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
    breach_streak_reasons = []
    breakpoint_info = None
    peak_total_rps = 0.0
    peak_total_rps_vu = None
    last_clean_vu = None
    recommended_safe_vus = None

    soak_mode = bool(SOAK_VUS)
    if soak_mode:
        vu_sequence = SOAK_VUS
        stage_secs = SOAK_STAGE_SECS
        log.info("Soak-test mode: fixed VU list %s, %ds/stage", SOAK_VUS, SOAK_STAGE_SECS)
    else:
        vu_sequence = list(range(VU_START, VU_MAX + 1, VU_STEP))
        stage_secs = STAGE_SECS

    for vu_count in vu_sequence:
        log.info("Stage %d VUs — %ds (%ds warmup excluded)", vu_count, stage_secs, WARMUP_SECS)
        stage_started_at = time.time()
        env.runner.start(vu_count, spawn_rate=SPAWN_RATE)

        gevent.sleep(WARMUP_SECS)
        env.stats.reset_all()
        measure_started_at = time.time()

        gevent.sleep(max(stage_secs - WARMUP_SECS, 1))
        env.runner.stop()
        measured_secs = time.time() - measure_started_at
        gevent.sleep(1)

        stage_stats = collect_stage_stats(env)
        all_stages[vu_count] = stage_stats
        total_rps = sum(s["rps"] for s in stage_stats.values())
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
                break

        write_results(all_stages, stage_meta, breakpoint_info, stage_secs, soak_mode)

    push_metrics(all_stages, breakpoint_info)
    log.info("Results written to %s", RESULTS_JSON)

    env.runner.quit()
    log.info("Done.")


if __name__ == "__main__":
    main()
