"""Builds the GitHub step summary (markdown) for a perf_test_mtu.yml run.

Usage: python scripts/make_summary.py <out_dir>
Reads from <out_dir>: net_diag_*.md, perf_results_e2e.json, perf_results_paths.json,
nstat_before.txt / nstat_after.txt. Prints markdown to stdout.
"""
import glob, json, os, re, subprocess, sys

out = sys.argv[1] if len(sys.argv) > 1 else "out"
p = lambda *a: print(*a)

p("## HDA performance overview\n")
p("Delivery paths: APISIX → nginx-static (EODC disk) · APISIX → hda-go (EOPF Zarr) · "
  "APISIX → HDA app → 302 to external provider (CloudFerro S3) or to EODC Ceph S3 (federated). "
  "Same target list for every run (scripts/hda_targets.py), so runs can be compared over time.\n")

# 1) per-path diagnostics, idle (before) and after the load
for f in sorted(glob.glob(os.path.join(out, "net_diag_*.md"))):
    p(open(f).read())

# 2) load ramp per delivery path (dev), latency to the last byte
def stage_table(path, title):
    if not os.path.exists(path):
        p(f"_{title}: no results_\n")
        return
    d = json.load(open(path))
    stages = d["stages"]
    vus = sorted(stages, key=int)
    eps = sorted({e for s in stages.values() for e in s})
    p(f"### {title}\n")
    p(f"breakpoint: {d.get('breakpoint') or 'none'}\n")
    p("| endpoint | " + " | ".join(f"{v} VUs p50 / p95" for v in vus) + " | errors (max stage) |")
    p("|---|" + "---:|" * len(vus) + "---:|")
    for e in eps:
        cells = []
        for v in vus:
            s = stages[v].get(e)
            cells.append(f"{s['p50']:.2f} / {s['p95']:.2f} s" if s else "–")
        err = max((stages[v][e]["err"] for v in vus if e in stages[v]), default=0)
        p(f"| {e} | " + " | ".join(cells) + f" | {err:.1%} |")
    p("")

stage_table(os.path.join(out, "perf_results_paths.json"),
            "Load per delivery path (dev, TASK_MODE=paths, time to last byte)")

# 3) mixed ramp vs. July 2026 baseline
e2e = os.path.join(out, "perf_results_e2e.json")
if os.path.exists(e2e):
    base = sorted(glob.glob("results/baseline_2026-07/*_vu800.json"))
    r = subprocess.run([sys.executable, "results/compare_runs.py", *base, e2e], capture_output=True, text=True)
    p("### Mixed load (dev) vs. July 2026 baseline\n")
    p("Only GET_zarray / HEAD_tif / GET_tif_range_tile latencies are comparable (same meaning as in July). "
      "Most files in this mix redirect to CloudFerro S3, so throughput is mainly external storage.\n")
    p("```\n" + (r.stdout or r.stderr) + "\n```\n")

# 4) TCP counters on the runner over the whole run
def nstat(fn):
    vals = {}
    if os.path.exists(fn):
        for line in open(fn):
            m = re.match(r"(\S+)\s+(\d+)", line)
            if m:
                vals[m.group(1)] = int(m.group(2))
    return vals

b, a = nstat(os.path.join(out, "nstat_before.txt")), nstat(os.path.join(out, "nstat_after.txt"))
if a:
    p("### TCP counters on the runner (during load ramps)\n")
    p("| counter | delta |\n|---|---:|")
    for k in sorted(a):
        p(f"| {k} | {a[k] - b.get(k, 0)} |")
    if a.get("TcpOutSegs"):
        out_segs = a["TcpOutSegs"] - b.get("TcpOutSegs", 0)
        retr = a.get("TcpRetransSegs", 0) - b.get("TcpRetransSegs", 0)
        p(f"\nretransmitted / sent segments: {retr} / {out_segs}"
          + (f" = {retr / out_segs:.3%}" if out_segs else ""))
    p("")
