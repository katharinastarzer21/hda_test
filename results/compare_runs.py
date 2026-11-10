# compare perf_test.py runs stage by stage
# usage: python results/compare_runs.py results/baseline_2026-07/*.json <run.json> ...
import json, os, sys

ENDPOINTS = ["GET_zarray", "HEAD_tif", "GET_tif_range_tile"]


def load(path):
    d = json.load(open(path))
    return os.path.basename(path), d["stages"], d["stage_meta"], len(d.get("data_file_paths") or []) or "?"


runs = [load(p) for p in sys.argv[1:]]
common = sorted(set.intersection(*(set(map(int, r[1])) for r in runs)))
print(f"common stages (VUs): {common}\n")
names = sorted({ep for r in runs for st in r[1].values() for ep in st})
if any(n.startswith("P_") for n in names):
    ENDPOINTS = [n for n in names if n.startswith("P_")]

for ep in ENDPOINTS:
    print(f"{ep}  p50 / p95 [s]")
    print(f"  {'run':34}" + "".join(f"{v:>13}" for v in common))
    for name, stages, _, _ in runs:
        cells = []
        for v in common:
            s = stages[str(v)].get(ep)
            cells.append(f"{s['p50']:.2f}/{s['p95']:.2f}" if s else "-")
        print(f"  {name:34}" + "".join(f"{c:>13}" for c in cells))
    print()

print("total throughput [MB/s] / total rps / error rate")
print(f"  {'run (n files)':34}" + "".join(f"{v:>17}" for v in common))
for name, stages, meta, nfiles in runs:
    cells = []
    for v in common:
        st, m = stages[str(v)], meta[str(v)]
        mbps = sum(s["throughput_mbps"] for s in st.values())
        err = m["total_failures"] / m["total_requests"] if m["total_requests"] else 0
        cells.append(f"{mbps:.0f}/{m['total_rps']:.0f}/{err:.1%}")
    print(f"  {name + f' ({nfiles})':34}" + "".join(f"{c:>17}" for c in cells))
