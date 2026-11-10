### net_diag `1_before` — https://data.eodc.eu (2026-11-10T10:26:02+00:00, 10× each, fresh connection per request)

runner region: **centralus** (Standard_D4ads_v7) · RTT ≈ TCP connect of apisix_404: **130 ms**

| path | what | status | DNS | TCP | TLS | TTFB | server ≈ | total (median) | p90 | max | MB/s | TCP mss/pmtu/retrans |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| nginx_zarray | APISIX → nginx-static, small file from EODC disk | 200 | 70 ms | 129 ms | 131 ms | 464 ms | 7 ms | **464 ms** | 608 ms | 633 ms |  |  |
| nginx_png_16k | APISIX → nginx-static, 16 KB png from EODC disk | 200 | 47 ms | 132 ms | 133 ms | 571 ms | 123 ms | **573 ms** | 599 ms | 812 ms |  |  |
| nginx_sig0_14M | APISIX → nginx-static, 14 MB GeoTIFF from EODC disk | 200 | 18 ms | 131 ms | 130 ms | 450 ms | 20 ms | **1834 ms** | 1970 ms | 1987 ms | 7.8 | ['1424']/['1500']/0 |
| nginx_403_nodisk | APISIX → nginx-static regex reject, no disk access | 403 | 6 ms | 130 ms | 131 ms | 409 ms | 13 ms | **409 ms** | 517 ms | 532 ms |  |  |
| apisix_404 | APISIX only (unknown collection) | 404 | 6 ms | 130 ms | 129 ms | 398 ms | 0 ms | **398 ms** | 406 ms | 406 ms |  |  |
| go_eopf_zarr | APISIX → hda-go, EOPF Zarr zarr.json | 200 | 8 ms | 130 ms | 130 ms | 401 ms | 0 ms | **1029 ms** | 1075 ms | 1169 ms |  |  |
| ext_cloudferro_302 | APISIX → HDA app, 302 to external provider | 302 | 6 ms | 132 ms | 133 ms | 406 ms | 2 ms | **406 ms** | 429 ms | 433 ms |  |  |
| ext_cloudferro_follow | … followed to CloudFerro S3 (external, 2.3 MB) | 200 | 139 ms | 267 ms | 267 ms | 974 ms | 5 ms | **1913 ms** | 2040 ms | 2095 ms | 1.2 |  |
| fed_ceph_8M_302 | APISIX → HDA app (federated), 302 presigned URL | 302 | 7 ms | 131 ms | 131 ms | 482 ms | 83 ms | **482 ms** | 650 ms | 650 ms |  |  |
| fed_ceph_8M_follow | … followed to EODC Ceph S3 objects.eodc.eu (8 MB) | 200 | 48 ms | 261 ms | 258 ms | 934 ms | 101 ms | **2083 ms** | 2231 ms | 2422 ms | 4.0 | ['1424']/['1500']/0 |

tracepath pmtu: ['1500', '1500', '1500'] · DF ping: 1500 B fail, 1428 B fail, 1228 B fail · resolved: ['193.170.203.244']

