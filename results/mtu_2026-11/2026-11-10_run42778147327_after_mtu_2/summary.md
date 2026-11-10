## HDA performance overview

Delivery paths: APISIX -> nginx-static (EODC disk) · APISIX -> hda-go (EOPF Zarr) · APISIX -> HDA app -> 302 to external provider (CloudFerro S3) or to EODC Ceph S3 (federated). Same target list for every run (scripts/hda_targets.py), so runs can be compared over time.

### net_diag `1_before` — https://dev.hda.eodchosting.eu (2026-11-10T07:54:04+00:00, 10× each, fresh connection per request)

| path | what | status | DNS | TCP | TLS | TTFB | total (median) | p90 | max | MB/s | TCP mss/pmtu/retrans |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| nginx_zarray | APISIX -> nginx-static, small file from EODC disk | 200 | 56 ms | 128 ms | 131 ms | 454 ms | 454 ms | 494 ms | 512 ms |  |  |
| nginx_png_16k | APISIX -> nginx-static, 16 KB png from EODC disk | 200 | 30 ms | 130 ms | 132 ms | 548 ms | 548 ms | 575 ms | 581 ms |  |  |
| nginx_sig0_14M | APISIX -> nginx-static, 14 MB GeoTIFF from EODC disk | 200 | 7 ms | 127 ms | 131 ms | 417 ms | 1801 ms | 1830 ms | 1849 ms | 7.9 | ['1424']/['1500']/0 |
| nginx_403_nodisk | APISIX -> nginx-static regex reject, no disk access | 403 | 8 ms | 130 ms | 133 ms | 415 ms | 415 ms | 531 ms | 600 ms |  |  |
| apisix_404 | APISIX only (unknown collection) | 404 | 5 ms | 130 ms | 132 ms | 402 ms | 402 ms | 408 ms | 410 ms |  |  |
| go_eopf_zarr | APISIX -> hda-go, EOPF Zarr zarr.json | 200 | 4 ms | 131 ms | 133 ms | 406 ms | 1043 ms | 1089 ms | 1141 ms |  |  |
| ext_cloudferro_302 | APISIX -> HDA app, 302 to external provider | 302 | 8 ms | 130 ms | 133 ms | 406 ms | 407 ms | 419 ms | 419 ms |  |  |
| ext_cloudferro_follow | ... followed to CloudFerro S3 (external, 2.3 MB) | 200 | 260 ms | 265 ms | 267 ms | 1070 ms | 1984 ms | 2060 ms | 2105 ms | 1.2 |  |
| fed_ceph_8M_302 | APISIX -> HDA app (federated), 302 presigned URL | 302 | 5 ms | 132 ms | 133 ms | 493 ms | 493 ms | 539 ms | 692 ms |  |  |
| fed_ceph_8M_follow | ... followed to EODC Ceph S3 objects.eodc.eu (8 MB) | 200 | 42 ms | 259 ms | 262 ms | 932 ms | 2100 ms | 2212 ms | 2259 ms | 3.9 | ['1424']/['1500']/0 |

tracepath pmtu: ['1500', '1500', '1500'] · DF ping: 1500 B fail, 1428 B fail, 1228 B fail · resolved: ['193.170.203.244']


### net_diag `1_before` — https://data.eodc.eu (2026-11-10T07:51:04+00:00, 10× each, fresh connection per request)

| path | what | status | DNS | TCP | TLS | TTFB | total (median) | p90 | max | MB/s | TCP mss/pmtu/retrans |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| nginx_zarray | APISIX -> nginx-static, small file from EODC disk | 200 | 70 ms | 131 ms | 133 ms | 503 ms | 504 ms | 537 ms | 542 ms |  |  |
| nginx_png_16k | APISIX -> nginx-static, 16 KB png from EODC disk | 200 | 66 ms | 131 ms | 132 ms | 596 ms | 596 ms | 737 ms | 763 ms |  |  |
| nginx_sig0_14M | APISIX -> nginx-static, 14 MB GeoTIFF from EODC disk | 200 | 6 ms | 129 ms | 132 ms | 423 ms | 1824 ms | 1879 ms | 1949 ms | 7.8 | ['1424']/['1500']/0 |
| nginx_403_nodisk | APISIX -> nginx-static regex reject, no disk access | 403 | 79 ms | 130 ms | 131 ms | 491 ms | 491 ms | 588 ms | 2457 ms |  |  |
| apisix_404 | APISIX only (unknown collection) | 404 | 41 ms | 130 ms | 133 ms | 434 ms | 434 ms | 552 ms | 1862 ms |  |  |
| go_eopf_zarr | APISIX -> hda-go, EOPF Zarr zarr.json | 200 | 8 ms | 130 ms | 133 ms | 406 ms | 1048 ms | 1080 ms | 1129 ms |  |  |
| ext_cloudferro_302 | APISIX -> HDA app, 302 to external provider | 302 | 9 ms | 130 ms | 133 ms | 411 ms | 411 ms | 456 ms | 481 ms |  |  |
| ext_cloudferro_follow | ... followed to CloudFerro S3 (external, 2.3 MB) | 200 | 319 ms | 271 ms | 268 ms | 1137 ms | 2099 ms | 2155 ms | 2223 ms | 1.1 |  |
| fed_ceph_8M_302 | APISIX -> HDA app (federated), 302 presigned URL | 302 | 8 ms | 132 ms | 131 ms | 496 ms | 496 ms | 669 ms | 833 ms |  |  |
| fed_ceph_8M_follow | ... followed to EODC Ceph S3 objects.eodc.eu (8 MB) | 200 | 38 ms | 258 ms | 262 ms | 907 ms | 2120 ms | 2186 ms | 2234 ms | 3.9 | ['1424']/['1500']/0 |

tracepath pmtu: ['1500', '1500', '1500'] · DF ping: 1500 B fail, 1428 B fail, 1228 B fail · resolved: ['193.170.203.244']


### net_diag `2_after` — https://dev.hda.eodchosting.eu (2026-11-10T08:11:05+00:00, 10× each, fresh connection per request)

| path | what | status | DNS | TCP | TLS | TTFB | total (median) | p90 | max | MB/s | TCP mss/pmtu/retrans |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| nginx_zarray | APISIX -> nginx-static, small file from EODC disk | 200 | 36 ms | 131 ms | 133 ms | 448 ms | 449 ms | 614 ms | 780 ms |  |  |
| nginx_png_16k | APISIX -> nginx-static, 16 KB png from EODC disk | 200 | 9 ms | 132 ms | 132 ms | 540 ms | 540 ms | 568 ms | 582 ms |  |  |
| nginx_sig0_14M | APISIX -> nginx-static, 14 MB GeoTIFF from EODC disk | 200 | 9 ms | 130 ms | 131 ms | 416 ms | 1828 ms | 1885 ms | 1945 ms | 7.8 | ['1424']/['1500']/0 |
| nginx_403_nodisk | APISIX -> nginx-static regex reject, no disk access | 403 | 5 ms | 128 ms | 130 ms | 399 ms | 399 ms | 424 ms | 448 ms |  |  |
| apisix_404 | APISIX only (unknown collection) | 404 | 7 ms | 129 ms | 132 ms | 407 ms | 407 ms | 437 ms | 460 ms |  |  |
| go_eopf_zarr | APISIX -> hda-go, EOPF Zarr zarr.json | 200 | 6 ms | 128 ms | 131 ms | 401 ms | 1032 ms | 1059 ms | 1072 ms |  |  |
| ext_cloudferro_302 | APISIX -> HDA app, 302 to external provider | 302 | 4 ms | 129 ms | 130 ms | 395 ms | 395 ms | 403 ms | 417 ms |  |  |
| ext_cloudferro_follow | ... followed to CloudFerro S3 (external, 2.3 MB) | 200 | 285 ms | 263 ms | 267 ms | 1094 ms | 2013 ms | 2084 ms | 2112 ms | 1.2 |  |
| fed_ceph_8M_302 | APISIX -> HDA app (federated), 302 presigned URL | 302 | 43 ms | 129 ms | 129 ms | 705 ms | 705 ms | 761 ms | 1010 ms |  |  |
| fed_ceph_8M_follow | ... followed to EODC Ceph S3 objects.eodc.eu (8 MB) | 200 | 121 ms | 254 ms | 257 ms | 1276 ms | 2408 ms | 2487 ms | 2496 ms | 3.4 | ['1424']/['1500']/0 |

tracepath pmtu: ['1500', '1500', '1500'] · DF ping: 1500 B fail, 1428 B fail, 1228 B fail · resolved: ['193.170.203.244']


### Load per delivery path (dev, TASK_MODE=paths, time to last byte)

breakpoint: none

| endpoint | 5 VUs p50 / p95 | 30 VUs p50 / p95 | 55 VUs p50 / p95 | errors (max stage) |
|---|---:|---:|---:|---:|
| P_apisix_404 | 0.13 / 0.13 s | 0.13 / 0.14 s | 0.14 / 0.15 s | 0.0% |
| P_ext_cloudferro_302 | 0.13 / 0.14 s | 0.13 / 0.14 s | 0.13 / 0.15 s | 0.0% |
| P_ext_cloudferro_follow | 0.36 / 1.50 s | 0.37 / 1.50 s | 0.37 / 1.50 s | 1.5% |
| P_fed_ceph_8M_302 | 0.40 / 0.44 s | 0.39 / 0.43 s | 0.41 / 0.49 s | 0.0% |
| P_fed_ceph_8M_follow | 1.80 / 1.90 s | 1.70 / 2.00 s | 1.80 / 1.90 s | 0.0% |
| P_go_eopf_zarr | 0.50 / 0.78 s | 0.40 / 0.79 s | 0.50 / 0.79 s | 0.0% |
| P_nginx_403_nodisk | 0.13 / 0.25 s | 0.14 / 0.23 s | 0.15 / 0.24 s | 0.0% |
| P_nginx_png_16k | 0.14 / 0.34 s | 0.21 / 0.32 s | 0.19 / 0.35 s | 0.0% |
| P_nginx_sig0_14M | 1.00 / 1.50 s | 1.20 / 1.60 s | 1.20 / 1.60 s | 0.0% |
| P_nginx_zarray | 0.13 / 0.26 s | 0.15 / 0.23 s | 0.15 / 0.25 s | 0.0% |

### Mixed load (dev) vs. July 2026 baseline

Only GET_zarray / HEAD_tif / GET_tif_range_tile latencies are comparable (same meaning as in July). Most files in this mix redirect to CloudFerro S3, so throughput is mainly external storage.

```
common stages (VUs): [5, 30, 55, 80, 105, 130]

GET_zarray  p50 / p95 [s]
  run                                           5           30           55           80          105          130
  2026-07-14_1158_vu800.json            0.11/0.11    0.18/0.53    0.45/0.72    0.58/0.92    0.81/1.00    0.91/1.10
  2026-07-15_0648_vu800.json            0.10/0.11    0.15/0.48    0.52/0.80    0.55/0.97    0.67/0.96    0.86/1.00
  perf_results_e2e.json                 0.15/0.27    0.35/0.77    0.71/1.10    1.10/1.50    1.30/1.70    1.40/1.70

HEAD_tif  p50 / p95 [s]
  run                                           5           30           55           80          105          130
  2026-07-14_1158_vu800.json            0.10/0.11    0.20/0.61    0.46/0.69    0.59/0.92    0.83/1.10    0.91/1.10
  2026-07-15_0648_vu800.json            0.10/0.13    0.14/0.45    0.50/0.80    0.57/0.95    0.69/0.97    0.85/1.00
  perf_results_e2e.json                 0.13/0.13    0.30/0.72    0.74/1.10    1.20/1.60    1.30/1.80    1.30/1.70

GET_tif_range_tile  p50 / p95 [s]
  run                                           5           30           55           80          105          130
  2026-07-14_1158_vu800.json            0.22/0.65    0.46/1.20    0.99/1.70    1.20/1.90    1.70/2.70    1.90/2.80
  2026-07-15_0648_vu800.json            0.21/0.65    0.33/1.00    1.00/1.80    1.20/2.30    1.40/2.20    1.70/2.80
  perf_results_e2e.json                 0.27/0.33    0.73/1.40    1.30/2.20    2.20/3.10    2.70/3.60    2.70/4.30

total throughput [MB/s] / total rps / error rate
  run (n files)                                     5               30               55               80              105              130
  2026-07-14_1158_vu800.json (?)           146/8/1.0%      722/30/0.7%      693/33/1.0%      710/33/1.0%      680/28/1.6%      591/31/1.3%
  2026-07-15_0648_vu800.json (?)           137/8/1.3%      657/33/1.0%      748/29/1.3%      704/34/0.9%      537/34/1.4%      573/34/1.8%
  perf_results_e2e.json (46)               216/7/0.0%      847/22/0.1%      913/29/0.0%      813/26/0.3%      824/25/0.2%      895/30/0.3%

```

### TCP counters on the runner (during load ramps)

| counter | delta |
|---|---:|
| TcpExtTCPFastRetrans | 0 |
| TcpExtTCPLostRetransmit | 0 |
| TcpExtTCPSlowStartRetrans | 0 |
| TcpExtTCPTimeouts | 0 |
| TcpInErrs | 0 |
| TcpInSegs | 79370416 |
| TcpOutSegs | 8903542 |
| TcpRetransSegs | 1 |

retransmitted / sent segments: 1 / 8903542 = 0.000%

