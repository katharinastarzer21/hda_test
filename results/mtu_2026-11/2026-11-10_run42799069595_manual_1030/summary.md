## HDA performance overview

Delivery paths: APISIX → nginx-static (EODC disk) · APISIX → hda-go (EOPF Zarr) · APISIX → HDA app → 302 to external provider (CloudFerro S3) or to EODC Ceph S3 (federated). Same target list for every run (scripts/hda_targets.py), so runs can be compared over time.

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


### Load per delivery path (dev, TASK_MODE=paths, time to last byte)

breakpoint: none

| endpoint | 5 VUs p50 / p95 | 30 VUs p50 / p95 | 55 VUs p50 / p95 | errors (max stage) |
|---|---:|---:|---:|---:|
| P_apisix_404 | 0.13 / 0.14 s | 0.14 / 0.18 s | 0.16 / 0.21 s | 0.0% |
| P_ext_cloudferro_302 | 0.13 / 0.14 s | 0.13 / 0.18 s | 0.16 / 0.22 s | 0.0% |
| P_ext_cloudferro_follow | 0.37 / 1.50 s | 0.37 / 1.50 s | 0.41 / 1.60 s | 1.1% |
| P_fed_ceph_8M_302 | 0.21 / 0.24 s | 0.23 / 0.29 s | 0.27 / 0.37 s | 0.0% |
| P_fed_ceph_8M_follow | 1.50 / 1.50 s | 1.50 / 2.30 s | 1.50 / 1.90 s | 0.0% |
| P_go_eopf_zarr | 0.52 / 0.78 s | 0.51 / 0.79 s | 0.57 / 0.83 s | 0.0% |
| P_nginx_403_nodisk | 0.16 / 0.35 s | 0.18 / 0.41 s | 0.22 / 0.48 s | 0.0% |
| P_nginx_png_16k | 0.21 / 0.49 s | 0.26 / 0.42 s | 0.30 / 0.55 s | 0.0% |
| P_nginx_sig0_14M | 1.20 / 1.50 s | 1.40 / 3.50 s | 1.60 / 2.40 s | 0.0% |
| P_nginx_zarray | 0.18 / 0.35 s | 0.19 / 0.38 s | 0.22 / 0.45 s | 0.0% |

### Mixed load (dev) vs. July 2026 baseline

Only GET_zarray / HEAD_tif / GET_tif_range_tile latencies are comparable (same meaning as in July). Most files in this mix redirect to CloudFerro S3, so throughput is mainly external storage.

```
common stages (VUs): [5, 30, 55, 80, 105, 130]

GET_zarray  p50 / p95 [s]
  run                                           5           30           55           80          105          130
  2026-07-14_1158_vu800.json            0.11/0.11    0.18/0.53    0.45/0.72    0.58/0.92    0.81/1.00    0.91/1.10
  2026-07-15_0648_vu800.json            0.10/0.11    0.15/0.48    0.52/0.80    0.55/0.97    0.67/0.96    0.86/1.00
  perf_results_e2e.json                 0.15/0.25    0.25/0.62    0.48/0.90    0.83/1.20    1.00/1.50    1.20/1.60

HEAD_tif  p50 / p95 [s]
  run                                           5           30           55           80          105          130
  2026-07-14_1158_vu800.json            0.10/0.11    0.20/0.61    0.46/0.69    0.59/0.92    0.83/1.10    0.91/1.10
  2026-07-15_0648_vu800.json            0.10/0.13    0.14/0.45    0.50/0.80    0.57/0.95    0.69/0.97    0.85/1.00
  perf_results_e2e.json                 0.13/0.15    0.23/0.60    0.46/0.92    0.84/1.30    1.00/1.40    1.20/1.60

GET_tif_range_tile  p50 / p95 [s]
  run                                           5           30           55           80          105          130
  2026-07-14_1158_vu800.json            0.22/0.65    0.46/1.20    0.99/1.70    1.20/1.90    1.70/2.70    1.90/2.80
  2026-07-15_0648_vu800.json            0.21/0.65    0.33/1.00    1.00/1.80    1.20/2.30    1.40/2.20    1.70/2.80
  perf_results_e2e.json                 0.28/0.38    0.49/1.20    0.95/1.80    1.70/2.60    2.00/3.10    2.50/3.10

total throughput [MB/s] / total rps / error rate
  run (n files)                                     5               30               55               80              105              130
  2026-07-14_1158_vu800.json (?)           146/8/1.0%      722/30/0.7%      693/33/1.0%      710/33/1.0%      680/28/1.6%      591/31/1.3%
  2026-07-15_0648_vu800.json (?)           137/8/1.3%      657/33/1.0%      748/29/1.3%      704/34/0.9%      537/34/1.4%      573/34/1.8%
  perf_results_e2e.json (46)               203/7/0.0%      922/27/0.0%     1042/36/0.1%     1163/34/0.0%     1087/36/0.1%      978/36/0.3%

```

### TCP counters on the runner (during load ramps)

| counter | delta |
|---|---:|
| TcpExtTCPFastRetrans | 0 |
| TcpExtTCPLostRetransmit | 0 |
| TcpExtTCPSlowStartRetrans | 0 |
| TcpExtTCPTimeouts | 1 |
| TcpInErrs | 0 |
| TcpInSegs | 79047487 |
| TcpOutSegs | 9758270 |
| TcpRetransSegs | 10 |

retransmitted / sent segments: 10 / 9758270 = 0.000%

