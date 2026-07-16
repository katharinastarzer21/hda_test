"""
Builds a PDF report from an ingestion_ramp_smoke_test.py results JSON
(e.g. opera_ramp_results_3.json). Mirrors ingestion_report_generator.py's
structure, adapted for this script's cold/warm-tagged schema (see
ingestion_ramp_smoke_test.py's module docstring for why that distinction
exists and why it's load-bearing for reading these numbers correctly).

Usage: python ingestion_ramp_report_generator.py opera_ramp_results_3.json result.pdf
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

CHART_DIR = "_ingestion_ramp_charts"


def load_results(path):
    with open(path) as f:
        return json.load(f)


def make_outcome_chart(levels, filename):
    ns = sorted(int(n) for n in levels.keys())
    cold_ok = [levels[str(n)]["cold_succeeded"] for n in ns]
    warm_ok = [levels[str(n)]["warm_count"] for n in ns]
    timed_out = [levels[str(n)]["timed_out_waiting_for_ingestion"] for n in ns]
    other = [levels[str(n)]["other_failures"] for n in ns]

    plt.figure(figsize=(6.5, 3.8))
    x = range(len(ns))
    bottom = [0] * len(ns)
    for series, label, color in [
        (cold_ok, "Succeeded (genuine cold-start)", "#2e7d32"),
        (warm_ok, "Succeeded (already warm)", "#9e9e9e"),
        (timed_out, "Timed out waiting for ingestion", "#c62828"),
        (other, "Other failures", "#616161"),
    ]:
        plt.bar(x, series, bottom=bottom, label=label, color=color)
        bottom = [b + s for b, s in zip(bottom, series)]
    plt.xticks(list(x), [str(n) for n in ns])
    plt.xlabel("Concurrent VUs")
    plt.ylabel("Count")
    plt.title("Outcome breakdown vs. concurrency")
    plt.legend(fontsize=7)
    plt.grid(True, linestyle="--", alpha=0.4, axis="y")
    plt.tight_layout()
    plt.savefig(filename, dpi=150)
    plt.close()


def make_timing_chart(levels, key_avg, key_max, ylabel, title, filename):
    ns = sorted(int(n) for n in levels.keys())
    avgs = [levels[str(n)].get(key_avg) for n in ns]
    maxes = [levels[str(n)].get(key_max) for n in ns]

    plt.figure(figsize=(6.5, 3.8))
    plt.plot(ns, avgs, marker="o", label="average", color="#2a78d6")
    plt.plot(ns, maxes, marker="s", linestyle="--", label="max", color="#1baf7a")
    plt.xlabel("Concurrent VUs")
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend(fontsize=8)
    plt.tight_layout()
    plt.savefig(filename, dpi=150)
    plt.close()


def build_pdf(results, out_path):
    os.makedirs(CHART_DIR, exist_ok=True)

    levels = results["levels"]
    cfg = results.get("config", {})

    outcome_chart = f"{CHART_DIR}/outcomes.png"
    ingest_chart = f"{CHART_DIR}/ingest_wait.png"
    download_chart = f"{CHART_DIR}/download_time.png"
    make_outcome_chart(levels, outcome_chart)
    make_timing_chart(levels, "avg_cold_ingest_wait_secs", "max_cold_ingest_wait_secs",
                       "seconds", "Cold-start ingestion wait vs. concurrency (cold attempts only)",
                       ingest_chart)
    make_timing_chart(levels, "avg_download_secs", "max_download_secs",
                       "seconds", "Download time vs. concurrency", download_chart)

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="SmallGrey", fontSize=8, textColor=colors.grey))

    doc = SimpleDocTemplate(out_path, pagesize=A4,
                             topMargin=2 * cm, bottomMargin=2 * cm,
                             leftMargin=2 * cm, rightMargin=2 * cm)
    story = []

    story.append(Paragraph("HDA Ingestion Ramp — Cold-Start Concurrency Report", styles["Title"]))
    story.append(Spacer(1, 4))
    story.append(Paragraph(f"Target: {results.get('target', '?')}", styles["Normal"]))
    story.append(Paragraph(f"Collections: {', '.join(cfg.get('target_collections', []))}", styles["Normal"]))
    story.append(Paragraph(
        f"Ramp levels tested: {cfg.get('ramp_levels')}. "
        f"Acceptable ingestion wait: {cfg.get('acceptable_ingest_wait_secs')}s, "
        f"hard give-up after: {cfg.get('max_ingest_wait_secs')}s "
        f"(poll every {cfg.get('poll_interval_secs')}s). "
        f"Download timeout once ready: {cfg.get('download_timeout_secs')}s.",
        styles["Normal"]))
    story.append(Paragraph(
        "Each stage draws that many genuinely fresh, distinct assets (never reused across "
        "stages or previous runs — see the seen-asset cache) and attempts them concurrently. "
        "Every result is tagged <b>cold</b> (a real 202 was observed before the asset became "
        "ready — Airflow ingestion genuinely ran) or <b>warm</b> (ready on the very first poll, "
        "e.g. already ingested by earlier testing). Only cold attempts feed the ingestion-wait "
        "statistics below, so a warm hit can never be mistaken for fast ingestion.",
        styles["SmallGrey"]))
    story.append(Spacer(1, 16))

    story.append(Paragraph("Outcome breakdown vs. concurrency", styles["Heading2"]))
    story.append(Image(outcome_chart, width=16 * cm, height=16 * cm * 3.8 / 6.5))
    story.append(Spacer(1, 10))
    story.append(Paragraph("Cold-start ingestion wait vs. concurrency", styles["Heading2"]))
    story.append(Image(ingest_chart, width=16 * cm, height=16 * cm * 3.8 / 6.5))
    story.append(Spacer(1, 10))
    story.append(PageBreak())
    story.append(Paragraph("Download time vs. concurrency", styles["Heading2"]))
    story.append(Image(download_chart, width=16 * cm, height=16 * cm * 3.8 / 6.5))
    story.append(Spacer(1, 16))

    story.append(Paragraph("Level details", styles["Heading2"]))
    story.append(Paragraph(
        "Cold ontime = cold-start success within the acceptable wait. Cold = all cold-start "
        "successes (ontime or slower). Warm = already available on first poll.",
        styles["SmallGrey"]))
    story.append(Spacer(1, 8))
    header = ["VUs", "Attempted", "Cold ok", "Cold ontime", "Warm", "Timeout", "OtherFail",
              "Avg wait (s)", "Max wait (s)", "Avg dl (s)", "Max dl (s)"]
    rows = [header]
    for n in sorted(int(k) for k in levels.keys()):
        lvl = levels[str(n)]

        def fmt(v):
            return f"{v:.1f}" if isinstance(v, (int, float)) and v is not None else "n/a"

        rows.append([
            str(n),
            str(lvl["attempted"]),
            str(lvl["cold_succeeded"]),
            str(lvl["cold_succeeded_within_acceptable_wait"]),
            str(lvl["warm_count"]),
            str(lvl["timed_out_waiting_for_ingestion"]),
            str(lvl["other_failures"]),
            fmt(lvl["avg_cold_ingest_wait_secs"]),
            fmt(lvl["max_cold_ingest_wait_secs"]),
            fmt(lvl["avg_download_secs"]),
            fmt(lvl["max_download_secs"]),
        ])

    table = Table(rows, repeatRows=1, colWidths=[1.3 * cm] + [1.65 * cm] * 10)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2b2d42")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTSIZE", (0, 0), (-1, -1), 7),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f2f2f2")]),
        ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(table)

    story.append(Spacer(1, 16))
    story.append(Paragraph(
        "Note: every downloaded file is deleted immediately after its size is verified, except "
        "up to KEEP_DOWNLOADS_SAMPLE kept on disk for manual spot-checking (see KEEP_DOWNLOADS_DIR).",
        styles["SmallGrey"]))

    doc.build(story)


if __name__ == "__main__":
    in_path = sys.argv[1] if len(sys.argv) > 1 else "ingestion_ramp_results.json"
    out_path = sys.argv[2] if len(sys.argv) > 2 else "result.pdf"
    results = load_results(in_path)
    build_pdf(results, out_path)
    print(f"Report written: {out_path}")
