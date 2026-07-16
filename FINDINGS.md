# HDA ingestion ramp — results

Target: `dev.hda.eodchosting.eu` · Collection: `OPERA_L2_RTC-S1_V1` · 2026-07-16

## Ramp 1: 1 → 5 → 10 → 15 → 20 VUs

All genuinely cold-start, zero failures:

| VUs | Succeeded | Avg ingest wait | Max ingest wait | Avg download |
|----:|----------:|-----------------:|-----------------:|--------------:|
|   1 |       1/1 |             30.0s |             30.0s |         0.43s |
|   5 |       5/5 |             33.0s |             45.0s |         0.34s |
|  10 |     10/10 |             42.0s |             45.0s |         0.30s |
|  15 |     15/15 |             45.0s |             45.0s |         0.45s |
|  20 |     20/20 |             51.0s |             75.0s |         0.78s |

## Ramp 2: 10 → 100 VUs (bigger steps)

500 total attempts, 500/500 succeeded, 0 timeouts, every attempt genuinely
cold-start:

| VUs | Succeeded | Within acceptable wait (≤180s) | Avg ingest wait | Max ingest wait | Avg download | Max download |
|----:|----------:|--------------------------------:|-----------------:|-----------------:|--------------:|--------------:|
|  10 |     10/10 |                            10/10 |             39.0s |             45.0s |         0.35s |         0.74s |
|  20 |     20/20 |                            20/20 |             49.5s |             75.0s |         0.50s |         1.13s |
|  30 |     30/30 |                            30/30 |             60.0s |             90.0s |         0.67s |         1.64s |
|  40 |     40/40 |                            40/40 |             68.3s |            105.0s |         0.44s |         1.10s |
|  50 |     50/50 |                            50/50 |             80.7s |            135.0s |         0.43s |         1.60s |
|  60 |     60/60 |                            60/60 |             92.5s |            150.0s |         0.46s |         1.73s |
|  70 |     70/70 |                            70/70 |            104.1s |            180.0s |         0.63s |         2.01s |
|  80 |     80/80 |                            78/80 |            112.1s |            195.0s |         0.64s |         1.58s |
|  90 |     90/90 |                            67/90 |            137.5s |            225.0s |         0.51s |         1.52s |
| 100 |   100/100 |                          79/100 |            133.1s |            240.0s |         0.42s |         1.56s |
