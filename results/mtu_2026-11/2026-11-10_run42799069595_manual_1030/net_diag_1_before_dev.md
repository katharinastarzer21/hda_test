### net_diag `1_before` — https://dev.hda.eodchosting.eu (2026-11-10T10:28:54+00:00, 10× each, fresh connection per request)

runner region: **centralus** (Standard_D4ads_v7) · RTT ≈ TCP connect of apisix_404: **128 ms**

| path | what | status | DNS | TCP | TLS | TTFB | server ≈ | total (median) | p90 | max | MB/s | TCP mss/pmtu/retrans |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| nginx_zarray | APISIX → nginx-static, small file from EODC disk | 200 | 54 ms | 129 ms | 129 ms | 457 ms | 15 ms | **457 ms** | 481 ms | 488 ms |  |  |
| nginx_png_16k | APISIX → nginx-static, 16 KB png from EODC disk | 200 | 29 ms | 128 ms | 128 ms | 613 ms | 125 ms | **613 ms** | 704 ms | 731 ms |  |  |
| nginx_sig0_14M | APISIX → nginx-static, 14 MB GeoTIFF from EODC disk | 200 | 12 ms | 126 ms | 128 ms | 428 ms | 24 ms | **1819 ms** | 1901 ms | 2014 ms | 7.9 | ['1424']/['1500']/0 |
| nginx_403_nodisk | APISIX → nginx-static regex reject, no disk access | 403 | 8 ms | 128 ms | 129 ms | 420 ms | 10 ms | **420 ms** | 542 ms | 571 ms |  |  |
| apisix_404 | APISIX only (unknown collection) | 404 | 7 ms | 128 ms | 129 ms | 395 ms | 3 ms | **396 ms** | 455 ms | 471 ms |  |  |
| go_eopf_zarr | APISIX → hda-go, EOPF Zarr zarr.json | 200 | 11 ms | 129 ms | 130 ms | 400 ms | 0 ms | **1029 ms** | 1084 ms | 1088 ms |  |  |
| ext_cloudferro_302 | APISIX → HDA app, 302 to external provider | 302 | 7 ms | 130 ms | 130 ms | 396 ms | 2 ms | **396 ms** | 411 ms | 418 ms |  |  |
| ext_cloudferro_follow | … followed to CloudFerro S3 (external, 2.3 MB) | 200 | 342 ms | 269 ms | 264 ms | 1153 ms | 3 ms | **2077 ms** | 2175 ms | 2225 ms | 1.1 |  |
| fed_ceph_8M_302 | APISIX → HDA app (federated), 302 presigned URL | 302 | 36 ms | 130 ms | 128 ms | 512 ms | 79 ms | **513 ms** | 552 ms | 598 ms |  |  |
| fed_ceph_8M_follow | … followed to EODC Ceph S3 objects.eodc.eu (8 MB) | 200 | 80 ms | 261 ms | 259 ms | 968 ms | 97 ms | **2139 ms** | 2270 ms | 2466 ms | 3.9 | ['1424']/['1500']/0 |

tracepath pmtu: ['1500', '1500', '1500'] · DF ping: 1500 B fail, 1428 B fail, 1228 B fail · resolved: ['193.170.203.244']

