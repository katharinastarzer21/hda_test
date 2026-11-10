# github step summary for perf_test_mtu.yml
# usage: python scripts/make_summary.py <out_dir>
import glob, json, os, re, subprocess, sys

out = sys.argv[1] if len(sys.argv) > 1 else "out"

print("## HDA performance\n")

for f in sorted(glob.glob(os.path.join(out, "net_diag_*.md"))):
    print(open(f).read())


def stage_table(path, title):
    if not os.path.exists(path):
        print(f"{title}: no results\n")
        return
    d = json.load(open(path))
    stages = d["stages"]
    vus = sorted(stages, key=int)
    eps = sorted({e for s in stages.values() for e in s})
    print(f"### {title}\n")
    print(f"breakpoint: {d.get('breakpoint') or 'none'}\n")
    print("| endpoint | " + " | ".join(f"{v} VUs p50 / p95" for v in vus) + " | max error rate |")
    print("|---|" + "---:|" * len(vus) + "---:|")
    for e in eps:
        cells = [f"{stages[v][e]['p50']:.2f} / {stages[v][e]['p95']:.2f} s" if e in stages[v] else "-" for v in vus]
        err = max((stages[v][e]["err"] for v in vus if e in stages[v]), default=0)
        print(f"| {e} | " + " | ".join(cells) + f" | {err:.1%} |")
    print()


stage_table(os.path.join(out, "perf_results_paths.json"), "Load per delivery path (dev)")

e2e = os.path.join(out, "perf_results_e2e.json")
if os.path.exists(e2e):
    base = sorted(glob.glob("results/baseline_2026-07/*_vu800.json"))
    r = subprocess.run([sys.executable, "results/compare_runs.py", *base, e2e], capture_output=True, text=True)
    print("### Mixed load (dev) vs. July 2026\n")
    print("```\n" + (r.stdout or r.stderr) + "\n```\n")


def nstat(fn):
    if not os.path.exists(fn):
        return {}
    return {m.group(1): int(m.group(2)) for m in (re.match(r"(\S+)\s+(\d+)", l) for l in open(fn)) if m}


b, a = nstat(os.path.join(out, "nstat_before.txt")), nstat(os.path.join(out, "nstat_after.txt"))
if a:
    print("### TCP counters during load\n")
    print("| counter | delta |\n|---|---:|")
    for k in sorted(a):
        print(f"| {k} | {a[k] - b.get(k, 0)} |")
    sent = a.get("TcpOutSegs", 0) - b.get("TcpOutSegs", 0)
    retr = a.get("TcpRetransSegs", 0) - b.get("TcpRetransSegs", 0)
    if sent:
        print(f"\nretransmitted: {retr} / {sent} = {retr / sent:.3%}\n")
