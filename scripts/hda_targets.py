"""One representative URL per HDA delivery path (from hda-config/prod + http-data-access).

How data.eodc.eu / dev.hda.eodchosting.eu deliver data:
  APISIX ─┬─ dfr-* routes ──────> nginx-static (127.0.0.1:8099), files from /eodc/products (DirectFilepathRouter, public)
          ├─ /collections/EOPF_ZARR* ─> hda-go (127.0.0.1:8093)
          └─ /* ─────────────────> HDA Python app (127.0.0.1:8098)
                                     ├─ EodagRedirectRouter  -> 302 to an external provider (e.g. CloudFerro S3 for cop_marine)
                                     ├─ FederatedEodagRouter -> 302 presigned URL to EODC Ceph S3 (objects.eodc.eu), or 202 while ingesting
                                     └─ Sentinel*Router / restricted DFR -> served by the app itself (needs login, not covered here)

Each target: (name, path, kind)
  kind "file"     : 200 served directly, measured to the last byte
  kind "status"   : expected error status, measures pure APISIX/nginx overhead
  kind "redirect" : measured twice: the 302 alone ("<name>_302", HDA app) and
                    followed to the storage host ("<name>_follow", the storage)
All of these are public. A target whose status changes shows up in the results
instead of silently vanishing.
"""

TARGETS = [
    # name               path                                                              kind      expected
    ("nginx_zarray",     "/collections/S2-L2A-C1/T33UWP/indices/time/.zarray",             "file",   200),
    ("nginx_png_16k",    "/collections/TOP4EO/BFW/forest_mask/THUMB/UTM32_10_10.png",      "file",   200),
    ("nginx_sig0_14M",   "/collections/SENTINEL1_SIG0_20M/V1M2R3/EQUI7_EU020M/E048N015T3/"
                         "SIG0_20260412T171426__VV_A015_E048N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif",
                                                                                           "file",   200),
    ("nginx_403_nodisk", "/collections/ARA/__regex_fail__",                                "status", 403),
    ("apisix_404",       "/collections/doesnotexist/x.tif",                                "status", 404),
    ("go_eopf_zarr",     "/collections/EOPF_ZARR/products/cpm_v300/S02MSIL2A/2026/09/06/"
                         "S2B_MSIL2A_20260906T100019_N0512_R122_T35WNU_20260906T134813.zarr/zarr.json",
                                                                                           "file",   200),
    ("ext_cloudferro",   "/collections/data/cop_marine/GLOBAL_ANALYSISFORECAST_BGC_001_028/"
                         "GLOBAL_ANALYSIS_FORECAST_BIO_001_028_mask/native",               "redirect", 302),
    ("fed_ceph_8M",      "/collections/data/nasa/OPERA_L2_RTC-S1_V1/"
                         "OPERA_L2_RTC-S1_T078-166569-IW1_20260705T013657Z_20260715T021510Z_S1C_30_v1.0/"
                         "OPERA_L2_RTC-S1_T078-166569-IW1_20260705T013657Z_20260715T021510Z_S1C_30_v1.0_BROWSE.png",
                                                                                           "redirect", 302),
]

# what each name means, for reports
DESCRIPTIONS = {
    "nginx_zarray": "APISIX → nginx-static, small file from EODC disk",
    "nginx_png_16k": "APISIX → nginx-static, 16 KB png from EODC disk",
    "nginx_sig0_14M": "APISIX → nginx-static, 14 MB GeoTIFF from EODC disk",
    "nginx_403_nodisk": "APISIX → nginx-static regex reject, no disk access",
    "apisix_404": "APISIX only (unknown collection)",
    "go_eopf_zarr": "APISIX → hda-go, EOPF Zarr zarr.json",
    "ext_cloudferro_302": "APISIX → HDA app, 302 to external provider",
    "ext_cloudferro_follow": "… followed to CloudFerro S3 (external, 2.3 MB)",
    "fed_ceph_8M_302": "APISIX → HDA app (federated), 302 presigned URL",
    "fed_ceph_8M_follow": "… followed to EODC Ceph S3 objects.eodc.eu (8 MB)",
}
