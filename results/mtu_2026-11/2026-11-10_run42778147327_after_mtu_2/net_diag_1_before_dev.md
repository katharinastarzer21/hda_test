### net_diag `1_before` — https://dev.hda.eodchosting.eu (2026-11-10T07:54:04+00:00, 10× each, fresh connection per request)

| path | what | status | DNS | TCP | TLS | TTFB | total (median) | p90 | max | MB/s | TCP mss/pmtu/retrans |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| nginx_zarray | APISIX → nginx-static, small file from EODC disk | 200 | 56 ms | 128 ms | 131 ms | 454 ms | **454 ms** | 494 ms | 512 ms |  |  |
| nginx_png_16k | APISIX → nginx-static, 16 KB png from EODC disk | 200 | 30 ms | 130 ms | 132 ms | 548 ms | **548 ms** | 575 ms | 581 ms |  |  |
| nginx_sig0_14M | APISIX → nginx-static, 14 MB GeoTIFF from EODC disk | 200 | 7 ms | 127 ms | 131 ms | 417 ms | **1801 ms** | 1830 ms | 1849 ms | 7.9 | ['1424']/['1500']/0 |
| nginx_403_nodisk | APISIX → nginx-static regex reject, no disk access | 403 | 8 ms | 130 ms | 133 ms | 415 ms | **415 ms** | 531 ms | 600 ms |  |  |
| apisix_404 | APISIX only (unknown collection) | 404 | 5 ms | 130 ms | 132 ms | 402 ms | **402 ms** | 408 ms | 410 ms |  |  |
| go_eopf_zarr | APISIX → hda-go, EOPF Zarr zarr.json | 200 | 4 ms | 131 ms | 133 ms | 406 ms | **1043 ms** | 1089 ms | 1141 ms |  |  |
| ext_cloudferro_302 | APISIX → HDA app, 302 to external provider | 302 | 8 ms | 130 ms | 133 ms | 406 ms | **407 ms** | 419 ms | 419 ms |  |  |
| ext_cloudferro_follow | … followed to CloudFerro S3 (external, 2.3 MB) | 200 | 260 ms | 265 ms | 267 ms | 1070 ms | **1984 ms** | 2060 ms | 2105 ms | 1.2 |  |
| fed_ceph_8M_302 | APISIX → HDA app (federated), 302 presigned URL | 302 | 5 ms | 132 ms | 133 ms | 493 ms | **493 ms** | 539 ms | 692 ms |  |  |
| fed_ceph_8M_follow | … followed to EODC Ceph S3 objects.eodc.eu (8 MB) | 200 | 42 ms | 259 ms | 262 ms | 932 ms | **2100 ms** | 2212 ms | 2259 ms | 3.9 | ['1424']/['1500']/0 |

tracepath pmtu: ['1500', '1500', '1500'] · DF ping: 1500 B fail, 1428 B fail, 1228 B fail · resolved: ['193.170.203.244']

