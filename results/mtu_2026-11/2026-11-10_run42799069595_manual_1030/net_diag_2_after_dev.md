### net_diag `2_after` — https://dev.hda.eodchosting.eu (2026-11-10T10:45:56+00:00, 10× each, fresh connection per request)

runner region: **centralus** (Standard_D4ads_v7) · RTT ≈ TCP connect of apisix_404: **127 ms**

| path | what | status | DNS | TCP | TLS | TTFB | server ≈ | total (median) | p90 | max | MB/s | TCP mss/pmtu/retrans |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| nginx_zarray | APISIX → nginx-static, small file from EODC disk | 200 | 60 ms | 131 ms | 131 ms | 486 ms | 3 ms | **487 ms** | 607 ms | 653 ms |  |  |
| nginx_png_16k | APISIX → nginx-static, 16 KB png from EODC disk | 200 | 33 ms | 130 ms | 132 ms | 561 ms | 124 ms | **561 ms** | 582 ms | 701 ms |  |  |
| nginx_sig0_14M | APISIX → nginx-static, 14 MB GeoTIFF from EODC disk | 200 | 6 ms | 129 ms | 130 ms | 416 ms | 22 ms | **1799 ms** | 1874 ms | 1886 ms | 8.0 | ['1424']/['1500']/0 |
| nginx_403_nodisk | APISIX → nginx-static regex reject, no disk access | 403 | 9 ms | 129 ms | 131 ms | 422 ms | 21 ms | **422 ms** | 536 ms | 572 ms |  |  |
| apisix_404 | APISIX only (unknown collection) | 404 | 11 ms | 127 ms | 128 ms | 397 ms | 3 ms | **397 ms** | 405 ms | 460 ms |  |  |
| go_eopf_zarr | APISIX → hda-go, EOPF Zarr zarr.json | 200 | 7 ms | 130 ms | 132 ms | 402 ms | 0 ms | **1039 ms** | 1074 ms | 1080 ms |  |  |
| ext_cloudferro_302 | APISIX → HDA app, 302 to external provider | 302 | 5 ms | 130 ms | 131 ms | 399 ms | 2 ms | **399 ms** | 406 ms | 418 ms |  |  |
| ext_cloudferro_follow | … followed to CloudFerro S3 (external, 2.3 MB) | 200 | 23 ms | 263 ms | 266 ms | 846 ms | 22 ms | **1756 ms** | 1913 ms | 1923 ms | 1.3 |  |
| fed_ceph_8M_302 | APISIX → HDA app (federated), 302 presigned URL | 302 | 76 ms | 130 ms | 132 ms | 646 ms | 89 ms | **646 ms** | 697 ms | 864 ms |  |  |
| fed_ceph_8M_follow | … followed to EODC Ceph S3 objects.eodc.eu (8 MB) | 200 | 87 ms | 259 ms | 260 ms | 984 ms | 100 ms | **2151 ms** | 2199 ms | 2442 ms | 3.8 | ['1424']/['1500']/0 |

tracepath pmtu: ['1500', '1500', '1500'] · DF ping: 1500 B fail, 1428 B fail, 1228 B fail · resolved: ['193.170.203.244']

