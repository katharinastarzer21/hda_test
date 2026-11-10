### net_diag `2_after` — https://dev.hda.eodchosting.eu (2026-11-10T12:54:21+00:00, 10× each, fresh connection per request)

runner region: **westus2** (Standard_D4ads_v6) · RTT ≈ TCP connect of apisix_404: **160 ms**

| path | what | status | DNS | TCP | TLS | TTFB | server ≈ | total (median) | p90 | max | MB/s | TCP mss/pmtu/retrans |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| nginx_zarray | APISIX → nginx-static, small file from EODC disk | 200 | 4 ms | 160 ms | 205 ms | 904 ms | 365 ms | **904 ms** | 1376 ms | 1383 ms |  |  |
| nginx_png_16k | APISIX → nginx-static, 16 KB png from EODC disk | 200 | 4 ms | 162 ms | 174 ms | 709 ms | 196 ms | **709 ms** | 934 ms | 1412 ms |  |  |
| nginx_sig0_14M | APISIX → nginx-static, 14 MB GeoTIFF from EODC disk | 200 | 3 ms | 164 ms | 181 ms | 743 ms | 222 ms | **2618 ms** | 2756 ms | 2787 ms | 5.5 | ['1424']/['1500']/0 |
| nginx_403_nodisk | APISIX → nginx-static regex reject, no disk access | 403 | 3 ms | 163 ms | 203 ms | 653 ms | 126 ms | **653 ms** | 890 ms | 996 ms |  |  |
| apisix_404 | APISIX only (unknown collection) | 404 | 4 ms | 160 ms | 187 ms | 581 ms | 43 ms | **581 ms** | 605 ms | 616 ms |  |  |
| go_eopf_zarr | APISIX → hda-go, EOPF Zarr zarr.json | 200 | 3 ms | 156 ms | 164 ms | 532 ms | 40 ms | **1286 ms** | 1341 ms | 1405 ms |  |  |
| ext_cloudferro_302 | APISIX → HDA app, 302 to external provider | 302 | 3 ms | 161 ms | 168 ms | 513 ms | 6 ms | **513 ms** | 553 ms | 573 ms |  |  |
| ext_cloudferro_follow | … followed to CloudFerro S3 (external, 2.3 MB) | 200 | 7 ms | 317 ms | 327 ms | 998 ms | 11 ms | **2105 ms** | 2171 ms | 2438 ms | 1.1 |  |
| fed_ceph_8M_302 | APISIX → HDA app (federated), 302 presigned URL | 302 | 79 ms | 162 ms | 164 ms | 659 ms | 89 ms | **659 ms** | 802 ms | 893 ms |  |  |
| fed_ceph_8M_follow | … followed to EODC Ceph S3 objects.eodc.eu (8 MB) | 200 | 143 ms | 322 ms | 327 ms | 1259 ms | 132 ms | **2730 ms** | 2849 ms | 3011 ms | 3.0 | ['1424']/['1500']/0 |

tracepath pmtu: ['1500', '1500', '1500'] · DF ping: 1500 B fail, 1428 B fail, 1228 B fail · resolved: ['193.170.203.244']

