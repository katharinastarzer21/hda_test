### net_diag `2_after` — https://dev.hda.eodchosting.eu (2026-11-10T08:11:05+00:00, 10× each, fresh connection per request)

| path | what | status | DNS | TCP | TLS | TTFB | total (median) | p90 | max | MB/s | TCP mss/pmtu/retrans |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| nginx_zarray | APISIX → nginx-static, small file from EODC disk | 200 | 36 ms | 131 ms | 133 ms | 448 ms | **449 ms** | 614 ms | 780 ms |  |  |
| nginx_png_16k | APISIX → nginx-static, 16 KB png from EODC disk | 200 | 9 ms | 132 ms | 132 ms | 540 ms | **540 ms** | 568 ms | 582 ms |  |  |
| nginx_sig0_14M | APISIX → nginx-static, 14 MB GeoTIFF from EODC disk | 200 | 9 ms | 130 ms | 131 ms | 416 ms | **1828 ms** | 1885 ms | 1945 ms | 7.8 | ['1424']/['1500']/0 |
| nginx_403_nodisk | APISIX → nginx-static regex reject, no disk access | 403 | 5 ms | 128 ms | 130 ms | 399 ms | **399 ms** | 424 ms | 448 ms |  |  |
| apisix_404 | APISIX only (unknown collection) | 404 | 7 ms | 129 ms | 132 ms | 407 ms | **407 ms** | 437 ms | 460 ms |  |  |
| go_eopf_zarr | APISIX → hda-go, EOPF Zarr zarr.json | 200 | 6 ms | 128 ms | 131 ms | 401 ms | **1032 ms** | 1059 ms | 1072 ms |  |  |
| ext_cloudferro_302 | APISIX → HDA app, 302 to external provider | 302 | 4 ms | 129 ms | 130 ms | 395 ms | **395 ms** | 403 ms | 417 ms |  |  |
| ext_cloudferro_follow | … followed to CloudFerro S3 (external, 2.3 MB) | 200 | 285 ms | 263 ms | 267 ms | 1094 ms | **2013 ms** | 2084 ms | 2112 ms | 1.2 |  |
| fed_ceph_8M_302 | APISIX → HDA app (federated), 302 presigned URL | 302 | 43 ms | 129 ms | 129 ms | 705 ms | **705 ms** | 761 ms | 1010 ms |  |  |
| fed_ceph_8M_follow | … followed to EODC Ceph S3 objects.eodc.eu (8 MB) | 200 | 121 ms | 254 ms | 257 ms | 1276 ms | **2408 ms** | 2487 ms | 2496 ms | 3.4 | ['1424']/['1500']/0 |

tracepath pmtu: ['1500', '1500', '1500'] · DF ping: 1500 B fail, 1428 B fail, 1228 B fail · resolved: ['193.170.203.244']

