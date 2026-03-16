"""
reporter.py — Generate the HTML summary report and CSV export.
"""

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from jinja2 import Template

import config

logger = logging.getLogger(__name__)

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Customer Segmentation Report — {{ run_date }}</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 1100px; margin: 40px auto; color: #1a1a1a; }
  h1   { font-size: 22px; font-weight: 500; margin-bottom: 4px; }
  .meta { font-size: 13px; color: #666; margin-bottom: 32px; }
  .kpi-row { display: grid; grid-template-columns: repeat(4,1fr); gap: 12px; margin-bottom: 32px; }
  .kpi { background: #f5f5f3; border-radius: 8px; padding: 16px; }
  .kpi .label { font-size: 12px; color: #666; }
  .kpi .value { font-size: 24px; font-weight: 500; margin-top: 4px; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; margin-bottom: 32px; }
  th    { text-align: left; padding: 8px 12px; border-bottom: 1px solid #e0e0e0; font-weight: 500; }
  td    { padding: 8px 12px; border-bottom: 0.5px solid #eee; }
  .seg-pill { display: inline-block; padding: 2px 10px; border-radius: 20px; font-size: 11px; font-weight: 500; }
  .tier-low      { color: #085041; background: #E1F5EE; }
  .tier-amber    { color: #633806; background: #FAEEDA; }
  .tier-high     { color: #A32D2D; background: #FCEBEB; }
  .tier-critical { color: #501313; background: #FCEBEB; font-weight:600; }
  h2 { font-size:16px; font-weight:500; margin: 32px 0 12px; }
</style>
</head>
<body>
<h1>Customer Segmentation Report</h1>
<div class="meta">Generated {{ run_date }} &nbsp;·&nbsp; {{ total_customers }} customers analysed</div>

<div class="kpi-row">
  <div class="kpi"><div class="label">Total customers</div><div class="value">{{ total_customers }}</div></div>
  <div class="kpi"><div class="label">Avg RFM score</div><div class="value">{{ avg_rfm }}</div></div>
  <div class="kpi"><div class="label">High / critical churn risk</div><div class="value">{{ at_risk_count }}</div></div>
  <div class="kpi"><div class="label">Champions</div><div class="value">{{ champion_count }}</div></div>
</div>

<h2>Segment summary</h2>
<table>
  <thead><tr>
    <th>Segment</th><th>Customers</th><th>%</th>
    <th>Avg RFM</th><th>Avg spend</th><th>Avg churn risk</th><th>Recommended action</th>
  </tr></thead>
  <tbody>
  {% for row in segment_summary %}
  <tr>
    <td><span class="seg-pill" style="background:{{ row.color }}22;color:{{ row.color }}">{{ row.segment }}</span></td>
    <td>{{ row.count }}</td>
    <td>{{ row.pct }}%</td>
    <td>{{ row.avg_rfm }}</td>
    <td>{{ row.avg_monetary | int }}</td>
    <td>{{ row.avg_churn }}</td>
    <td style="color:#555;font-size:12px">{{ row.action }}</td>
  </tr>
  {% endfor %}
  </tbody>
</table>

<h2>High-risk customers (churn tier: high or critical)</h2>
<table>
  <thead><tr>
    <th>Customer ID</th><th>Segment</th><th>RFM score</th>
    <th>Recency (days)</th><th>Frequency</th><th>Monetary</th><th>Churn score</th><th>Tier</th>
  </tr></thead>
  <tbody>
  {% for row in high_risk %}
  <tr>
    <td>{{ row.customer_id }}</td>
    <td>{{ row.segment }}</td>
    <td>{{ row.rfm_score }}</td>
    <td>{{ row.recency_days }}</td>
    <td>{{ row.frequency }}</td>
    <td>{{ row.monetary | int }}</td>
    <td>{{ row.churn_score }}</td>
    <td><span class="seg-pill tier-{{ row.churn_tier }}">{{ row.churn_tier }}</span></td>
  </tr>
  {% endfor %}
  </tbody>
</table>
</body>
</html>
"""


def generate_html_report(
    customers: pd.DataFrame,
    orders: pd.DataFrame | None = None,
    output_path: Path | None = None,
) -> Path:
    """Render an HTML report and save it."""
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = output_path or config.REPORT_HTML

    total      = len(customers)
    at_risk    = customers["churn_tier"].isin(["high", "critical"]).sum()
    champions  = (customers["segment"] == "Champions").sum()
    avg_rfm    = round(customers["rfm_score"].mean(), 2)

    # Segment summary
    seg_rows = []
    seg_meta = {s["name"]: s for s in __import__("segmenter").SEGMENT_RULES}
    for seg, grp in customers.groupby("segment"):
        meta = seg_meta.get(seg, {})
        seg_rows.append({
            "segment":     seg,
            "count":       len(grp),
            "pct":         round(len(grp) / total * 100, 1),
            "avg_rfm":     round(grp["rfm_score"].mean(), 2),
            "avg_monetary":round(grp["monetary"].mean(), 2),
            "avg_churn":   round(grp["churn_score"].mean(), 1),
            "action":      meta.get("action", ""),
            "color":       meta.get("color", "#888"),
        })
    seg_rows.sort(key=lambda x: x["count"], reverse=True)

    # High risk table (top 50)
    hr = (
        customers[customers["churn_tier"].isin(["high", "critical"])]
        .sort_values("churn_score", ascending=False)
        .head(50)
    )

    html = Template(HTML_TEMPLATE).render(
        run_date         = datetime.now().strftime("%d %b %Y, %H:%M"),
        total_customers  = total,
        avg_rfm          = avg_rfm,
        at_risk_count    = int(at_risk),
        champion_count   = int(champions),
        segment_summary  = seg_rows,
        high_risk        = hr.to_dict(orient="records"),
    )

    out.write_text(html, encoding="utf-8")
    logger.info(f"HTML report saved to {out}")
    return out


def export_csv(customers: pd.DataFrame, output_path: Path | None = None) -> Path:
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = output_path or config.EXPORT_CSV
    customers.to_csv(out, index=False)
    logger.info(f"CSV export saved to {out}")
    return out
