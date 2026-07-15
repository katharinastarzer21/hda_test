"""
Builds a PDF report from ingestion_results.json (output of
ingestion_download_test.py).
Usage: python ingestion_report_generator.py ingestion_results.json ingestion_report.pdf
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

CHART_DIR = "_ingestion_charts"


def load_results(path):
    with open(path) as f:
        return json.load(f)


def make_outcome_chart(levels, filename):
    ns = sorted(int(n) for n in levels.keys())
    within = [levels[str(n)]["succeeded_within_acceptable_wait"] for n in ns]
    slower = [levels[str(n)]["succeeded_but_slower_than_acceptable"] for n in ns]
    timed_out = [levels[str(n)]["timed_out_waiting_for_ingestion"] for n in ns]
    other = [levels[str(n)]["other_failures"] for n in ns]

    plt.figure(figsize=(6.5, 3.8))
    x = range(len(ns))
    bottom = [0] * len(ns)
    for series, label, color in [
        (within, "Succeeded (within acceptable wait)", "#2e7d32"),
        (slower, "Succeeded (slower than acceptable)", "#f9a825"),
        (timed_out, "Timed out waiting for ingestion", "#c62828"),
        (other, "Other failures", "#616161"),
    ]:
        plt.bar(x, series, bottom=bottom, label=label, color=color)
        bottom = [b + s for b, s in zip(bottom, series)]
    plt.xticks(list(x), [str(n) for n in ns])
    plt.xlabel("Concurrent attempts")
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
    plt.plot(ns, avgs, marker="o", label="average")
    plt.plot(ns, maxes, marker="s", linestyle="--", label="max")
    plt.xlabel("Concurrent attempts")
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
    make_timing_chart(levels, "avg_ingest_wait_secs", "max_ingest_wait_secs",
                       "seconds", "Ingestion wait time vs. concurrency", ingest_chart)
    make_timing_chart(levels, "avg_download_secs", "max_download_secs",
                       "seconds", "Download time vs. concurrency", download_chart)

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="SmallGrey", fontSize=8, textColor=colors.grey))

    doc = SimpleDocTemplate(out_path, pagesize=A4,
                             topMargin=2 * cm, bottomMargin=2 * cm,
                             leftMargin=2 * cm, rightMargin=2 * cm)
    story = []

    story.append(Paragraph("HDA Ingestion + Download Concurrency Report", styles["Title"]))
    story.append(Spacer(1, 4))
    story.append(Paragraph(f"Target: {results.get('target', '?')}", styles["Normal"]))
    story.append(Paragraph(
        f"Concurrency levels tested: {cfg.get('concurrency_levels')}. "
        f"Acceptable ingestion wait: {cfg.get('acceptable_ingest_wait_secs')}s, "
        f"hard give-up after: {cfg.get('max_ingest_wait_secs')}s "
        f"(poll every {cfg.get('poll_interval_secs')}s). "
        f"Download timeout once ready: {cfg.get('download_timeout_secs')}s.",
        styles["Normal"]))
    story.append(Paragraph(
        "Each attempt: request a cold asset -&gt; if 202 \"ingestion requested\", poll until "
        "ready or give up -&gt; download the whole file for real -&gt; verify byte count against "
        "Content-Length -&gt; delete immediately. This measures how many such flows can run "
        "at once, not requests/second.",
        styles["SmallGrey"]))
    story.append(Spacer(1, 16))

    story.append(Paragraph("Outcome breakdown vs. concurrency", styles["Heading2"]))
    story.append(Image(outcome_chart, width=16 * cm, height=16 * cm * 3.8 / 6.5))
    story.append(Spacer(1, 10))
    story.append(Paragraph("Ingestion wait time vs. concurrency", styles["Heading2"]))
    story.append(Image(ingest_chart, width=16 * cm, height=16 * cm * 3.8 / 6.5))
    story.append(Spacer(1, 10))
    story.append(PageBreak())
    story.append(Paragraph("Download time vs. concurrency", styles["Heading2"]))
    story.append(Image(download_chart, width=16 * cm, height=16 * cm * 3.8 / 6.5))
    story.append(Spacer(1, 16))

    story.append(Paragraph("Level details", styles["Heading2"]))
    story.append(Paragraph(
        f"OK ontime = succeeded within the {cfg.get('acceptable_ingest_wait_secs')}s acceptable wait. "
        f"OK slow = succeeded, but took longer than that (up to the {cfg.get('max_ingest_wait_secs')}s "
        f"hard give-up).",
        styles["SmallGrey"]))
    story.append(Spacer(1, 8))
    header = ["N", "OK total", "OK ontime", "OK slow", "Timeout", "OtherFail",
              "Avg wait (s)", "Max wait (s)", "Avg dl (s)", "Max dl (s)"]
    rows = [header]
    for n in sorted(int(k) for k in levels.keys()):
        lvl = levels[str(n)]

        def fmt(v):
            return f"{v:.1f}" if isinstance(v, (int, float)) and v is not None else "n/a"

        rows.append([
            str(n),
            str(lvl["succeeded"]),
            str(lvl["succeeded_within_acceptable_wait"]),
            str(lvl["succeeded_but_slower_than_acceptable"]),
            str(lvl["timed_out_waiting_for_ingestion"]),
            str(lvl["other_failures"]),
            fmt(lvl["avg_ingest_wait_secs"]),
            fmt(lvl["max_ingest_wait_secs"]),
            fmt(lvl["avg_download_secs"]),
            fmt(lvl["max_download_secs"]),
        ])

    table = Table(rows, repeatRows=1, colWidths=[1.3 * cm] + [1.9 * cm] * 9)
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
        "Note: every downloaded file is deleted immediately after its size is verified — this "
        "report reflects timing/outcomes only, no data is retained.",
        styles["SmallGrey"]))

    doc.build(story)


if __name__ == "__main__":
    in_path = sys.argv[1] if len(sys.argv) > 1 else "ingestion_results.json"
    out_path = sys.argv[2] if len(sys.argv) > 2 else "ingestion_report.pdf"
    results = load_results(in_path)
    build_pdf(results, out_path)
    print(f"Report written: {out_path}")
