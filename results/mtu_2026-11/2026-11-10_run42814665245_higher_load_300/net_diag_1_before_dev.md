### net_diag `1_before` — https://dev.hda.eodchosting.eu (2026-11-10T12:22:57+00:00, 10× each, fresh connection per request)

runner region: **westus2** (Standard_D4ads_v6) · RTT ≈ TCP connect of apisix_404: **159 ms**

| path | what | status | DNS | TCP | TLS | TTFB | server ≈ | total (median) | p90 | max | MB/s | TCP mss/pmtu/retrans |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| nginx_zarray | APISIX → nginx-static, small file from EODC disk | 200 | 93 ms | 161 ms | 164 ms | 653 ms | 114 ms | **653 ms** | 968 ms | 1077 ms |  |  |
| nginx_png_16k | APISIX → nginx-static, 16 KB png from EODC disk | 200 | 6 ms | 162 ms | 167 ms | 681 ms | 162 ms | **682 ms** | 866 ms | 1028 ms |  |  |
| nginx_sig0_14M | APISIX → nginx-static, 14 MB GeoTIFF from EODC disk | 200 | 42 ms | 157 ms | 161 ms | 560 ms | 24 ms | **2291 ms** | 2434 ms | 2503 ms | 6.2 | ['1424']/['1500']/0 |
| nginx_403_nodisk | APISIX → nginx-static regex reject, no disk access | 403 | 3 ms | 159 ms | 163 ms | 598 ms | 95 ms | **599 ms** | 729 ms | 857 ms |  |  |
| apisix_404 | APISIX only (unknown collection) | 404 | 4 ms | 159 ms | 163 ms | 493 ms | 5 ms | **493 ms** | 515 ms | 545 ms |  |  |
| go_eopf_zarr | APISIX → hda-go, EOPF Zarr zarr.json | 200 | 4 ms | 157 ms | 162 ms | 504 ms | 4 ms | **1297 ms** | 1340 ms | 1466 ms |  |  |
| ext_cloudferro_302 | APISIX → HDA app, 302 to external provider | 302 | 4 ms | 163 ms | 167 ms | 502 ms | 4 ms | **502 ms** | 511 ms | 512 ms |  |  |
| ext_cloudferro_follow | … followed to CloudFerro S3 (external, 2.3 MB) | 200 | 166 ms | 316 ms | 329 ms | 1191 ms | 43 ms | **2271 ms** | 2291 ms | 2298 ms | 1.0 |  |
| fed_ceph_8M_302 | APISIX → HDA app (federated), 302 presigned URL | 302 | 3 ms | 160 ms | 163 ms | 580 ms | 92 ms | **580 ms** | 640 ms | 804 ms |  |  |
| fed_ceph_8M_follow | … followed to EODC Ceph S3 objects.eodc.eu (8 MB) | 200 | 58 ms | 325 ms | 336 ms | 1163 ms | 106 ms | **2612 ms** | 2874 ms | 3031 ms | 3.2 | ['1424']/['1500']/0 |

tracepath pmtu: ['1500', '1500', '1500'] · DF ping: 1500 B fail, 1428 B fail, 1228 B fail · resolved: ['193.170.203.244']

