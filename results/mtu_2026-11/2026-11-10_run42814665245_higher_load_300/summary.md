## HDA performance overview

Delivery paths: APISIX → nginx-static (EODC disk) · APISIX → hda-go (EOPF Zarr) · APISIX → HDA app → 302 to external provider (CloudFerro S3) or to EODC Ceph S3 (federated). Same target list for every run (scripts/hda_targets.py), so runs can be compared over time.

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


### Load per delivery path (dev, TASK_MODE=paths, time to last byte)

breakpoint: none

| endpoint | 5 VUs p50 / p95 | 30 VUs p50 / p95 | 55 VUs p50 / p95 | 80 VUs p50 / p95 | 105 VUs p50 / p95 | 130 VUs p50 / p95 | errors (max stage) |
|---|---:|---:|---:|---:|---:|---:|---:|
| P_apisix_404 | 0.16 / 0.17 s | 0.19 / 0.27 s | 0.18 / 0.25 s | 0.19 / 0.24 s | 0.19 / 0.24 s | 0.34 / 0.79 s | 0.0% |
| P_ext_cloudferro_302 | 0.16 / 0.17 s | 0.19 / 0.27 s | 0.18 / 0.24 s | 0.18 / 0.24 s | 0.19 / 0.25 s | 0.35 / 0.96 s | 0.0% |
| P_ext_cloudferro_follow | 0.43 / 1.80 s | 0.49 / 1.90 s | 0.48 / 1.90 s | 0.51 / 1.90 s | 0.53 / 2.00 s | 1.80 / 2.40 s | 0.8% |
| P_fed_ceph_8M_302 | 0.25 / 0.29 s | 0.31 / 0.42 s | 0.28 / 0.41 s | 0.29 / 0.38 s | 0.30 / 0.42 s | 0.52 / 1.10 s | 0.0% |
| P_fed_ceph_8M_follow | 1.90 / 2.20 s | 1.90 / 2.10 s | 1.90 / 2.30 s | 1.90 / 2.40 s | 1.90 / 2.40 s | 2.20 / 2.90 s | 0.0% |
| P_go_eopf_zarr | 0.50 / 1.00 s | 0.68 / 1.00 s | 0.69 / 1.00 s | 0.94 / 1.00 s | 0.93 / 1.10 s | 1.00 / 1.60 s | 0.0% |
| P_nginx_403_nodisk | 0.18 / 0.25 s | 0.28 / 0.53 s | 0.41 / 1.20 s | 0.81 / 2.10 s | 0.88 / 2.40 s | 0.65 / 1.40 s | 0.0% |
| P_nginx_png_16k | 0.20 / 0.40 s | 0.34 / 0.59 s | 0.50 / 1.30 s | 0.87 / 2.00 s | 1.00 / 2.80 s | 0.74 / 1.50 s | 0.0% |
| P_nginx_sig0_14M | 1.50 / 2.30 s | 1.90 / 3.00 s | 1.90 / 3.50 s | 1.90 / 2.30 s | 1.90 / 3.40 s | 4.40 / 8.00 s | 0.2% |
| P_nginx_zarray | 0.17 / 0.25 s | 0.28 / 0.53 s | 0.43 / 1.20 s | 0.83 / 1.90 s | 0.97 / 2.50 s | 0.65 / 1.40 s | 0.0% |

### Mixed load (dev) vs. July 2026 baseline

Only GET_zarray / HEAD_tif / GET_tif_range_tile latencies are comparable (same meaning as in July). Most files in this mix redirect to CloudFerro S3, so throughput is mainly external storage.

```
common stages (VUs): [5, 30, 55, 80, 105, 130, 155, 180, 205, 230, 255, 280]

GET_zarray  p50 / p95 [s]
  run                                           5           30           55           80          105          130          155          180          205          230          255          280
  2026-07-14_1158_vu800.json            0.11/0.11    0.18/0.53    0.45/0.72    0.58/0.92    0.81/1.00    0.91/1.10    0.89/1.20    1.00/1.30    0.85/1.10    1.00/1.20    0.94/1.20    1.00/1.40
  2026-07-15_0648_vu800.json            0.10/0.11    0.15/0.48    0.52/0.80    0.55/0.97    0.67/0.96    0.86/1.00    0.76/1.20    0.95/1.20    0.88/1.10    0.97/1.20    0.97/1.20    0.98/1.30
  perf_results_e2e.json                 0.58/1.70    0.49/0.96    0.65/2.20    1.00/1.40    1.10/1.50    1.50/2.40    1.50/1.90    1.50/1.90    1.40/2.20    1.70/2.50    1.80/2.60    1.80/2.20

HEAD_tif  p50 / p95 [s]
  run                                           5           30           55           80          105          130          155          180          205          230          255          280
  2026-07-14_1158_vu800.json            0.10/0.11    0.20/0.61    0.46/0.69    0.59/0.92    0.83/1.10    0.91/1.10    0.89/1.20    1.00/1.20    0.87/1.10    1.00/1.20    0.93/1.20    1.00/1.30
  2026-07-15_0648_vu800.json            0.10/0.13    0.14/0.45    0.50/0.80    0.57/0.95    0.69/0.97    0.85/1.00    0.76/1.20    0.96/1.30    0.85/1.20    0.99/1.20    0.97/1.20    0.98/1.30
  perf_results_e2e.json                 0.18/0.30    0.28/0.50    0.37/0.83    1.00/1.30    1.10/1.50    1.40/2.00    1.50/1.90    1.50/1.90    1.30/1.70    1.60/1.90    1.80/2.10    1.80/2.10

GET_tif_range_tile  p50 / p95 [s]
  run                                           5           30           55           80          105          130          155          180          205          230          255          280
  2026-07-14_1158_vu800.json            0.22/0.65    0.46/1.20    0.99/1.70    1.20/1.90    1.70/2.70    1.90/2.80    1.70/3.40    2.00/3.50    1.70/3.10    2.00/3.40    1.90/3.40    2.10/4.40
  2026-07-15_0648_vu800.json            0.21/0.65    0.33/1.00    1.00/1.80    1.20/2.30    1.40/2.20    1.70/2.80    1.50/2.80    1.90/3.50    1.80/3.00    2.00/3.60    2.00/3.50    2.00/3.20
  perf_results_e2e.json                 0.38/0.54    0.48/0.96    0.71/1.60    2.00/2.70    2.10/3.10    2.70/4.20    3.00/6.10    3.10/5.20    2.80/5.20    3.20/6.20    3.60/7.30    3.70/7.60

total throughput [MB/s] / total rps / error rate
  run (n files)                                     5               30               55               80              105              130              155              180              205              230              255              280
  2026-07-14_1158_vu800.json (?)           146/8/1.0%      722/30/0.7%      693/33/1.0%      710/33/1.0%      680/28/1.6%      591/31/1.3%      470/38/1.0%      499/38/1.1%      470/44/1.1%      385/43/1.2%      458/47/1.3%      329/44/1.4%
  2026-07-15_0648_vu800.json (?)           137/8/1.3%      657/33/1.0%      748/29/1.3%      704/34/0.9%      537/34/1.4%      573/34/1.8%      454/36/1.1%      464/35/1.2%      431/48/0.8%      431/41/1.4%      439/46/1.2%      356/55/1.1%
  perf_results_e2e.json (46)               178/5/0.0%      808/27/0.0%     1023/37/0.0%      927/29/0.0%      988/34/0.2%      951/31/0.3%      890/33/0.8%      918/34/0.6%      920/45/0.4%      864/39/0.8%      776/38/0.7%      817/41/0.8%

```

### TCP counters on the runner (during load ramps)

| counter | delta |
|---|---:|
| TcpExtTCPFastRetrans | 0 |
| TcpExtTCPLostRetransmit | 14 |
| TcpExtTCPSlowStartRetrans | 0 |
| TcpExtTCPTimeouts | 24 |
| TcpInErrs | 0 |
| TcpInSegs | 68224281 |
| TcpOutSegs | 13168727 |
| TcpRetransSegs | 38 |

retransmitted / sent segments: 38 / 13168727 = 0.000%

