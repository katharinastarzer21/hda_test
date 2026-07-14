"""
Builds a PDF report from perf_results.json (output of perf_test.py).
Usage: python report_generator.py perf_results.json report.pdf
"""
import sys, os, json
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak,
)

CHART_DIR = "_charts"


def load_results(path):
    with open(path) as f:
        return json.load(f)


def endpoints_from(stages):
    names = set()
    for vu_stats in stages.values():
        names.update(vu_stats.keys())
    return sorted(names)


def make_chart(stages, endpoints, metric_key, ylabel, title, filename, as_percent=False):
    vus = sorted(int(v) for v in stages.keys())
    plt.figure(figsize=(6.5, 3.8))
    for ep in endpoints:
        ys = []
        for vu in vus:
            s = stages.get(str(vu), {}).get(ep)
            ys.append((s[metric_key] * 100 if as_percent else s[metric_key]) if s else None)
        plt.plot(vus, ys, marker="o", label=ep)
    plt.xlabel("Virtual Users (VUs)")
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend(fontsize=8)
    plt.tight_layout()
    plt.savefig(filename, dpi=150)
    plt.close()


def build_pdf(results, out_path):
    os.makedirs(CHART_DIR, exist_ok=True)

    stages = results["stages"]
    stage_meta = results.get("stage_meta", {})
    endpoints = endpoints_from(stages)
    breakpoint_info = results.get("breakpoint")
    cfg = results.get("config", {})

    charts = [
        ("p95", "seconds", "p95 latency vs. VUs", f"{CHART_DIR}/latency_p95.png", False),
        ("rps", "requests/s", "Throughput (RPS) vs. VUs", f"{CHART_DIR}/rps.png", False),
        ("err", "error rate (%)", "Error rate vs. VUs", f"{CHART_DIR}/error_rate.png", True),
        ("throughput_mbps", "MB/s", "Data throughput vs. VUs", f"{CHART_DIR}/throughput.png", False),
    ]
    for metric_key, ylabel, title, filename, as_pct in charts:
        make_chart(stages, endpoints, metric_key, ylabel, title, filename, as_pct)

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="SmallGrey", fontSize=8, textColor=colors.grey))

    doc = SimpleDocTemplate(out_path, pagesize=A4,
                             topMargin=2 * cm, bottomMargin=2 * cm,
                             leftMargin=2 * cm, rightMargin=2 * cm)
    story = []

    # cover / summary
    story.append(Paragraph("HDA Performance / Stress Test Report", styles["Title"]))
    story.append(Spacer(1, 4))
    story.append(Paragraph(f"Target: {results.get('target', '?')}", styles["Normal"]))
    story.append(Paragraph(
        f"Config: start {cfg.get('vu_start')} VUs, step +{cfg.get('vu_step')}, "
        f"max {cfg.get('vu_max')} VUs, {cfg.get('stage_secs')}s/stage "
        f"({cfg.get('warmup_secs')}s warmup excluded)",
        styles["Normal"]))
    p95_line = f"Breakpoint criteria: error rate &gt; {cfg.get('error_rate_threshold', 0):.0%} or p95 &gt; {cfg.get('p95_threshold_secs')}s"
    if cfg.get("full_download_p95_threshold_secs"):
        p95_line += f" ({cfg['full_download_p95_threshold_secs']}s for GET_tif_full, which moves far more data per request)"
    story.append(Paragraph(p95_line, styles["Normal"]))
    story.append(Spacer(1, 12))

    if breakpoint_info:
        story.append(Paragraph("Result: breakpoint reached", styles["Heading2"]))
        story.append(Paragraph(
            f"At <b>{breakpoint_info['vus']} VUs</b> the thresholds were breached for the first time "
            f"(confirmed at {breakpoint_info['confirmed_at_vus']} VUs, 2 stages in a row). "
            f"Reason: {'; '.join(breakpoint_info['reasons'])}",
            styles["Normal"]))
    else:
        max_vu = max(int(v) for v in stages.keys())
        story.append(Paragraph("Result: no breakpoint reached", styles["Heading2"]))
        story.append(Paragraph(
            f"No thresholds were breached up to the highest tested stage ({max_vu} VUs). "
            f"The service was stable in the tested range — increase VU_MAX for a harder limit.",
            styles["Normal"]))
    story.append(Spacer(1, 16))

    for _, _, title, filename, _ in charts:
        story.append(Paragraph(title, styles["Heading2"]))
        story.append(Image(filename, width=16 * cm, height=16 * cm * 3.8 / 6.5))
        story.append(Spacer(1, 10))
    story.append(PageBreak())

    if stage_meta:
        story.append(Paragraph("Stage timing & volume", styles["Heading2"]))
        story.append(Spacer(1, 8))

        timing_header = ["VUs", "Measured (s)", "Total (incl. warmup/spawn) (s)", "Requests", "Failures", "Total RPS"]
        timing_rows = [timing_header]
        for vu in sorted(int(v) for v in stages.keys()):
            m = stage_meta.get(str(vu))
            if not m:
                continue
            # older result files predate total_rps; derive it so the report still renders
            total_rps = m["total_rps"] if "total_rps" in m else m["total_requests"] / m["measured_secs"]
            timing_rows.append([
                str(vu),
                f"{m['measured_secs']:.1f}",
                f"{m['wall_secs']:.1f}",
                str(m["total_requests"]),
                str(m["total_failures"]),
                f"{total_rps:.1f}",
            ])

        timing_table = Table(timing_rows, repeatRows=1, colWidths=[1.7 * cm, 3 * cm, 4.3 * cm, 2.5 * cm, 2.5 * cm, 2.5 * cm])
        timing_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2b2d42")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f2f2f2")]),
            ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        story.append(timing_table)
        story.append(Spacer(1, 16))

    story.append(Paragraph("Stage details", styles["Heading2"]))
    story.append(Spacer(1, 8))

    header = ["VUs", "Endpoint", "p50 (s)", "p95 (s)", "p99 (s)", "RPS", "MB/s", "Err %"]
    rows = [header]
    for vu in sorted(int(v) for v in stages.keys()):
        for ep in endpoints:
            s = stages.get(str(vu), {}).get(ep)
            if not s:
                continue
            rows.append([
                str(vu), ep,
                f"{s['p50']:.3f}", f"{s['p95']:.3f}", f"{s['p99']:.3f}",
                f"{s['rps']:.1f}", f"{s['throughput_mbps']:.2f}", f"{s['err'] * 100:.1f}",
            ])

    table = Table(rows, repeatRows=1, colWidths=[1.5 * cm, 4.5 * cm] + [1.9 * cm] * 6)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2b2d42")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f2f2f2")]),
        ("ALIGN", (2, 0), (-1, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(table)

    story.append(Spacer(1, 20))
    story.append(Paragraph(
        "Note: client-side metrics only (Locust). For full diagnosis, cross-check against "
        "server-side VM metrics (CPU/RAM/I-O) for the same time window, e.g. via VictoriaMetrics/Grafana.",
        styles["SmallGrey"]))

    doc.build(story)


if __name__ == "__main__":
    in_path = sys.argv[1] if len(sys.argv) > 1 else "perf_results.json"
    out_path = sys.argv[2] if len(sys.argv) > 2 else "report.pdf"
    results = load_results(in_path)
    build_pdf(results, out_path)
    print(f"Report written: {out_path}")
