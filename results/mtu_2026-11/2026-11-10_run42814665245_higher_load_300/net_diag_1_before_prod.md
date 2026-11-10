### net_diag `1_before` — https://data.eodc.eu (2026-11-10T12:19:01+00:00, 10× each, fresh connection per request)

runner region: **westus2** (Standard_D4ads_v6) · RTT ≈ TCP connect of apisix_404: **156 ms**

| path | what | status | DNS | TCP | TLS | TTFB | server ≈ | total (median) | p90 | max | MB/s | TCP mss/pmtu/retrans |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| nginx_zarray | APISIX → nginx-static, small file from EODC disk | 200 | 83 ms | 159 ms | 167 ms | 1409 ms | 740 ms | **1409 ms** | 1910 ms | 2347 ms |  |  |
| nginx_png_16k | APISIX → nginx-static, 16 KB png from EODC disk | 200 | 37 ms | 162 ms | 176 ms | 1833 ms | 708 ms | **1833 ms** | 3089 ms | 3297 ms |  |  |
| nginx_sig0_14M | APISIX → nginx-static, 14 MB GeoTIFF from EODC disk | 200 | 4 ms | 162 ms | 171 ms | 640 ms | 113 ms | **2456 ms** | 2585 ms | 9781 ms | 5.8 | ['1424', '1428']/['1500']/0 |
| nginx_403_nodisk | APISIX → nginx-static regex reject, no disk access | 403 | 3 ms | 164 ms | 195 ms | 1441 ms | 856 ms | **1441 ms** | 2025 ms | 2289 ms |  |  |
| apisix_404 | APISIX only (unknown collection) | 404 | 3 ms | 156 ms | 168 ms | 504 ms | 16 ms | **504 ms** | 590 ms | 622 ms |  |  |
| go_eopf_zarr | APISIX → hda-go, EOPF Zarr zarr.json | 200 | 3 ms | 158 ms | 167 ms | 536 ms | 43 ms | **1318 ms** | 1371 ms | 1920 ms |  |  |
| ext_cloudferro_302 | APISIX → HDA app, 302 to external provider | 302 | 3 ms | 159 ms | 169 ms | 525 ms | 25 ms | **525 ms** | 585 ms | 600 ms |  |  |
| ext_cloudferro_follow | … followed to CloudFerro S3 (external, 2.3 MB) | 200 | 7 ms | 317 ms | 342 ms | 1053 ms | 45 ms | **2161 ms** | 2316 ms | 2522 ms | 1.1 |  |
| fed_ceph_8M_302 | APISIX → HDA app (federated), 302 presigned URL | 302 | 4 ms | 159 ms | 168 ms | 672 ms | 142 ms | **672 ms** | 705 ms | 791 ms |  |  |
| fed_ceph_8M_follow | … followed to EODC Ceph S3 objects.eodc.eu (8 MB) | 200 | 64 ms | 325 ms | 346 ms | 1240 ms | 209 ms | **2721 ms** | 2815 ms | 2830 ms | 3.0 | ['1424']/['1500']/0 |

tracepath pmtu: ['1500', '1500', '1500'] · DF ping: 1500 B fail, 1428 B fail, 1228 B fail · resolved: ['193.170.203.244']

