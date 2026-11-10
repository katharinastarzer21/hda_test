# curl timings per delivery path + TCP info (mss, pmtu, retransmits)
# usage: python scripts/net_diag.py <label> <base_url> [out.json]
import json, os, re, socket, statistics, subprocess, sys, time
from datetime import datetime, timezone
from urllib.parse import urlparse
from hda_targets import TARGETS, DESCRIPTIONS

REPEATS = int(os.environ.get("NET_DIAG_REPEATS", 10))
BIG = {"nginx_sig0_14M", "fed_ceph_8M_follow"}

CURL_FIELDS = ["http_code", "http_version", "remote_ip", "num_connects", "num_redirects",
               "size_download", "speed_download", "time_namelookup", "time_connect",
               "time_appconnect", "time_starttransfer", "time_total"]
WRITE_OUT = "\\n".join(f"{f}=%{{{f}}}" for f in CURL_FIELDS)


def run(cmd, timeout=60):
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, (p.stdout + p.stderr).strip()
    except Exception as e:
        return None, str(e)


def parse_curl(out):
    d = {}
    for line in (out or "").splitlines():
        k, _, v = line.partition("=")
        if k in CURL_FIELDS:
            try:
                d[k] = float(v) if k.startswith(("time_", "speed_", "size_")) else v
            except ValueError:
                d[k] = v
    return d


def parse_ss(out):
    best = None
    for block in re.split(r"\n(?=\S)", out or ""):
        m = dict(re.findall(r"\b(mss|pmtu|bytes_received|rtt|retrans):([\d./]+)", block))
        if not m:
            continue
        info = {"mss": m.get("mss"), "pmtu": m.get("pmtu"), "rtt": m.get("rtt"),
                "bytes_received": int(m.get("bytes_received", 0)),
                "retrans_total": int(m["retrans"].split("/")[-1]) if "retrans" in m else 0}
        if best is None or info["bytes_received"] > best["bytes_received"]:
            best = info
    return best


def curl(url, follow=False, sample_ss=False):
    cmd = ["curl", "-s", "-o", "/dev/null", "--max-time", "120", "-w", WRITE_OUT] + (["-L"] if follow else []) + [url]
    if not sample_ss:
        rc, out = run(cmd, timeout=130)
        return {**parse_curl(out), "rc": rc}
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    last = None
    while p.poll() is None:
        time.sleep(0.1)
        last = parse_ss(run(["ss", "-tinH", "state", "established", "dport", "=", ":443"], timeout=5)[1]) or last
    out, _ = p.communicate(timeout=130)
    return {**parse_curl(out), "rc": p.returncode, "ss": last}


def med(v):
    return statistics.median(v) if v else None


def summarize(samples, expected):
    ok = [x for x in samples if x.get("rc") == 0 and isinstance(x.get("time_total"), float)]
    s = {"n": len(samples), "curl_errors": len(samples) - len(ok),
         "http_codes": sorted({str(x.get("http_code", "?")) for x in samples}),
         "unexpected_status": sum(1 for x in ok if x.get("http_code") != str(expected))}
    if not ok:
        return s
    s["median_s"] = {
        "dns": med([x["time_namelookup"] for x in ok]),
        "tcp": med([x["time_connect"] - x["time_namelookup"] for x in ok]),
        "tls": med([x["time_appconnect"] - x["time_connect"] for x in ok]),
        "ttfb": med([x["time_starttransfer"] for x in ok]),
        "total": med([x["time_total"] for x in ok]),
        # ttfb minus connection setup minus one RTT
        "server": med([max(0.0, x["time_starttransfer"] - x["time_appconnect"]
                           - (x["time_connect"] - x["time_namelookup"])) for x in ok]),
    }
    s["p90_total_s"] = sorted(x["time_total"] for x in ok)[max(0, int(len(ok) * 0.9) - 1)]
    s["max_total_s"] = max(x["time_total"] for x in ok)
    s["median_MBps"] = med([x["speed_download"] / 1e6 for x in ok])
    s["size_bytes"] = med([x["size_download"] for x in ok])
    ss = [x["ss"] for x in ok if x.get("ss")]
    if ss:
        s["tcp"] = {"mss": sorted({x["mss"] for x in ss}), "pmtu": sorted({x["pmtu"] for x in ss}),
                    "retrans_total": sum(x["retrans_total"] for x in ss)}
    return s


def runner_location():
    # github runners are azure vms
    _, meta = run(["curl", "-s", "-m", "3", "-H", "Metadata:true",
                   "http://169.254.169.254/metadata/instance/compute?api-version=2021-02-01"], timeout=10)
    try:
        m = json.loads(meta)
        return {"region": m.get("location"), "vm_size": m.get("vmSize")}
    except Exception:
        return {"region": "unknown"}


def main():
    label = sys.argv[1] if len(sys.argv) > 1 else "diag"
    base = (sys.argv[2] if len(sys.argv) > 2 else os.environ.get("HDA_URL", "https://dev.hda.eodchosting.eu")).rstrip("/")
    host = urlparse(base).hostname
    out_path = sys.argv[3] if len(sys.argv) > 3 else f"net_diag_{label}_{host}.json"

    res = {"label": label, "utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "target": base, "repeats": REPEATS, "runner_location": runner_location()}
    try:
        res["resolved_ips"] = sorted({a[4][0] for a in socket.getaddrinfo(host, 443)})
    except Exception as e:
        res["resolved_ips"] = str(e)
    res["runner_ip_link"] = run(["ip", "-o", "link"])[1]

    plan = []
    for name, path, kind, expected in TARGETS:
        if kind == "redirect":
            plan += [(f"{name}_302", path, False, 302), (f"{name}_follow", path, True, 200)]
        else:
            plan.append((name, path, False, expected))

    res["paths"] = {}
    for name, path, follow, expected in plan:
        samples = [curl(base + path, follow=follow, sample_ss=name in BIG) for _ in range(REPEATS)]
        res["paths"][name] = {"url": base + path, "expected": expected,
                              "summary": summarize(samples, expected), "samples": samples}

    res["tracepath"] = run(["tracepath", "-n", "-m", "20", host], timeout=90)[1]
    res["tracepath_pmtu"] = re.findall(r"pmtu (\d+)", res["tracepath"])
    res["ping_df"] = {}
    for size in (1472, 1400, 1200):
        rc, out = run(["ping", "-M", "do", "-c", "3", "-W", "2", "-s", str(size), host], timeout=20)
        res["ping_df"][size] = {"ok": rc == 0, "out": out[-300:]}

    with open(out_path, "w") as f:
        json.dump(res, f, indent=2)

    ms = lambda v: f"{v * 1000:.0f} ms" if isinstance(v, float) else "-"
    rtt = res["paths"].get("apisix_404", {}).get("summary", {}).get("median_s", {}).get("tcp")
    md = [f"### net_diag {label}: {base} ({res['utc']}, {REPEATS} requests per path)", "",
          f"runner region: {res['runner_location'].get('region')}, RTT approx. {ms(rtt)}", "",
          "| path | what | status | DNS | TCP | TLS | TTFB | server | total | p90 | max | MB/s | mss/pmtu/retrans |",
          "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    for name, r in res["paths"].items():
        s = r["summary"]
        m = s.get("median_s", {})
        codes = ",".join(s["http_codes"]) + (" (!)" if s["unexpected_status"] or s["curl_errors"] else "")
        tcp = s.get("tcp")
        tcp_s = f"{tcp['mss']}/{tcp['pmtu']}/{tcp['retrans_total']}" if tcp else ""
        mbs = f"{s['median_MBps']:.1f}" if s.get("size_bytes", 0) > 1_000_000 else ""
        md.append(f"| {name} | {DESCRIPTIONS.get(name, '')} | {codes} | {ms(m.get('dns'))} | {ms(m.get('tcp'))} | "
                  f"{ms(m.get('tls'))} | {ms(m.get('ttfb'))} | {ms(m.get('server'))} | {ms(m.get('total'))} | "
                  f"{ms(s.get('p90_total_s'))} | {ms(s.get('max_total_s'))} | {mbs} | {tcp_s} |")
    md += ["", f"tracepath pmtu: {res['tracepath_pmtu'] or 'n/a'}, DF ping: "
           + ", ".join(f"{k + 28} B {'ok' if v['ok'] else 'fail'}" for k, v in res["ping_df"].items())
           + f", resolved: {res['resolved_ips']}", ""]
    with open(out_path.rsplit(".", 1)[0] + ".md", "w") as f:
        f.write("\n".join(md) + "\n")
    print("\n".join(md))


if __name__ == "__main__":
    main()
