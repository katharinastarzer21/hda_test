### net_diag `1_before` — https://data.eodc.eu (2026-11-10T07:51:04+00:00, 10× each, fresh connection per request)

| path | what | status | DNS | TCP | TLS | TTFB | total (median) | p90 | max | MB/s | TCP mss/pmtu/retrans |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| nginx_zarray | APISIX → nginx-static, small file from EODC disk | 200 | 70 ms | 131 ms | 133 ms | 503 ms | **504 ms** | 537 ms | 542 ms |  |  |
| nginx_png_16k | APISIX → nginx-static, 16 KB png from EODC disk | 200 | 66 ms | 131 ms | 132 ms | 596 ms | **596 ms** | 737 ms | 763 ms |  |  |
| nginx_sig0_14M | APISIX → nginx-static, 14 MB GeoTIFF from EODC disk | 200 | 6 ms | 129 ms | 132 ms | 423 ms | **1824 ms** | 1879 ms | 1949 ms | 7.8 | ['1424']/['1500']/0 |
| nginx_403_nodisk | APISIX → nginx-static regex reject, no disk access | 403 | 79 ms | 130 ms | 131 ms | 491 ms | **491 ms** | 588 ms | 2457 ms |  |  |
| apisix_404 | APISIX only (unknown collection) | 404 | 41 ms | 130 ms | 133 ms | 434 ms | **434 ms** | 552 ms | 1862 ms |  |  |
| go_eopf_zarr | APISIX → hda-go, EOPF Zarr zarr.json | 200 | 8 ms | 130 ms | 133 ms | 406 ms | **1048 ms** | 1080 ms | 1129 ms |  |  |
| ext_cloudferro_302 | APISIX → HDA app, 302 to external provider | 302 | 9 ms | 130 ms | 133 ms | 411 ms | **411 ms** | 456 ms | 481 ms |  |  |
| ext_cloudferro_follow | … followed to CloudFerro S3 (external, 2.3 MB) | 200 | 319 ms | 271 ms | 268 ms | 1137 ms | **2099 ms** | 2155 ms | 2223 ms | 1.1 |  |
| fed_ceph_8M_302 | APISIX → HDA app (federated), 302 presigned URL | 302 | 8 ms | 132 ms | 131 ms | 496 ms | **496 ms** | 669 ms | 833 ms |  |  |
| fed_ceph_8M_follow | … followed to EODC Ceph S3 objects.eodc.eu (8 MB) | 200 | 38 ms | 258 ms | 262 ms | 907 ms | **2120 ms** | 2186 ms | 2234 ms | 3.9 | ['1424']/['1500']/0 |

tracepath pmtu: ['1500', '1500', '1500'] · DF ping: 1500 B fail, 1428 B fail, 1228 B fail · resolved: ['193.170.203.244']

