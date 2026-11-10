# one public test file per HDA delivery path
# kind: file = served directly, status = expected error status, redirect = 302 to storage

TARGETS = [
    ("nginx_zarray",     "/collections/S2-L2A-C1/T33UWP/indices/time/.zarray", "file", 200),
    ("nginx_png_16k",    "/collections/TOP4EO/BFW/forest_mask/THUMB/UTM32_10_10.png", "file", 200),
    ("nginx_sig0_14M",   "/collections/SENTINEL1_SIG0_20M/V1M2R3/EQUI7_EU020M/E048N015T3/"
                         "SIG0_20260412T171426__VV_A015_E048N015T3_EU020M_V1M2R3_S1CIWGRDH_TUWIEN.tif", "file", 200),
    ("nginx_403_nodisk", "/collections/ARA/__regex_fail__", "status", 403),
    ("apisix_404",       "/collections/doesnotexist/x.tif", "status", 404),
    ("go_eopf_zarr",     "/collections/EOPF_ZARR/products/cpm_v300/S02MSIL2A/2026/09/06/"
                         "S2B_MSIL2A_20260906T100019_N0512_R122_T35WNU_20260906T134813.zarr/zarr.json", "file", 200),
    ("ext_cloudferro",   "/collections/data/cop_marine/GLOBAL_ANALYSISFORECAST_BGC_001_028/"
                         "GLOBAL_ANALYSIS_FORECAST_BIO_001_028_mask/native", "redirect", 302),
    ("fed_ceph_8M",      "/collections/data/nasa/OPERA_L2_RTC-S1_V1/"
                         "OPERA_L2_RTC-S1_T078-166569-IW1_20260705T013657Z_20260715T021510Z_S1C_30_v1.0/"
                         "OPERA_L2_RTC-S1_T078-166569-IW1_20260705T013657Z_20260715T021510Z_S1C_30_v1.0_BROWSE.png",
                         "redirect", 302),
]

DESCRIPTIONS = {
    "nginx_zarray": "nginx-static, small file",
    "nginx_png_16k": "nginx-static, 16 KB png",
    "nginx_sig0_14M": "nginx-static, 14 MB GeoTIFF",
    "nginx_403_nodisk": "nginx-static regex reject, no disk access",
    "apisix_404": "APISIX only",
    "go_eopf_zarr": "hda-go, EOPF zarr.json",
    "ext_cloudferro_302": "HDA app, 302 to external provider",
    "ext_cloudferro_follow": "download from CloudFerro S3 (2.3 MB)",
    "fed_ceph_8M_302": "HDA app, 302 to EODC Ceph",
    "fed_ceph_8M_follow": "download from EODC Ceph S3 (8 MB)",
}
