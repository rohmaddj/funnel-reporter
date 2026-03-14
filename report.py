"""
Report Builder
Reads report_data.json + analysis.json and produces three outputs:
  1. HTML email (sent via Gmail API or saved as output/email.html for SMTP)
  2. Google Sheets row appended to trend tracker
  3. Google Doc created for the week (annotatable, shareable)

Usage:
    python report.py                  # full run, live Google APIs
    python report.py --mock           # skip all API calls, preview email.html only
    python report.py --no-email       # sheets + docs only, skip sending email
    python report.py --no-sheets      # skip sheets append
    python report.py --no-docs        # skip doc creation
"""

import argparse
import base64
import json
import os
import sys
import requests
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

SHEET_ID        = "116dp7y01lDO6P-eR8zOpiRTxQW-W1fLF3sIDXv8xOdo"
SHEET_TAB       = "Weekly Data"          # tab name — will be created if missing
CREDENTIALS_FILE = os.environ.get("GOOGLE_CREDENTIALS_FILE", "google_credentials.json")
EMAIL_TO        = os.environ.get("REPORT_EMAIL_TO", "")   # who gets the report
EMAIL_FROM      = os.environ.get("REPORT_EMAIL_FROM", "") # your Gmail address

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/gmail.send",
]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--data",       default="output/report_data.json")
    p.add_argument("--analysis",   default="output/analysis.json")
    p.add_argument("--mock",       action="store_true")
    p.add_argument("--no-email",   action="store_true")
    p.add_argument("--no-sheets",  action="store_true")
    p.add_argument("--no-docs",    action="store_true")
    return p.parse_args()


# ── Google auth ───────────────────────────────────────────────────────────────

def get_google_services():
    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE, scopes=SCOPES
    )
    return {
        "sheets": build("sheets", "v4",  credentials=creds, cache_discovery=False),
        "docs":   build("docs",   "v1",  credentials=creds, cache_discovery=False),
        "drive":  build("drive",  "v3",  credentials=creds, cache_discovery=False),
        "gmail":  build("gmail",  "v1",  credentials=creds, cache_discovery=False),
    }


# ── 1. HTML EMAIL ─────────────────────────────────────────────────────────────

def build_email_html(data: dict, analysis: dict, doc_url: str = None) -> str:
    snap     = data["funnel_snapshot"]
    tracking = data["traffic_sources"]["tracking"]
    backend  = data["funnel_backend"]
    a        = analysis["analysis"]
    period   = f"{data['meta']['period_start']} → {data['meta']['period_end']}"
    ns       = a["northstar"]

    # ── helpers ──
    def status_dot(s):
        return {"green": "🟢", "yellow": "🟡", "red": "🔴"}.get(s, "⚪")

    def priority_badge(p):
        colors = {"critical": "#dc2626", "high": "#d97706", "medium": "#6b7280"}
        labels = {"critical": "CRITICAL", "high": "HIGH", "medium": "MEDIUM"}
        c = colors.get(p, "#6b7280")
        return f'<span style="background:{c};color:#fff;font-size:10px;font-weight:700;padding:2px 7px;border-radius:3px;letter-spacing:.05em">{labels.get(p,"")}</span>'

    def action_badge(action):
        colors = {
            "scale":   "#16a34a", "protect": "#2563eb", "fix":    "#dc2626",
            "pause":   "#9333ea", "audit":   "#d97706",
        }
        c = colors.get(action, "#6b7280")
        return f'<span style="background:{c}20;color:{c};font-size:10px;font-weight:700;padding:2px 8px;border-radius:3px;border:1px solid {c}40;letter-spacing:.05em">{action.upper()}</span>'

    # ── email campaigns table ──
    email_camps = {k: v for k, v in tracking["campaigns"].items() if v["source"] == "email"}
    paid_camps  = {k: v for k, v in tracking["campaigns"].items() if v["source"] == "facebook"}

    def camp_rows(camps):
        rows = ""
        for cid, c in sorted(camps.items(), key=lambda x: -x[1]["revenue"]):
            dot = {"strong": "🟢", "active": "🟢", "low_volume": "🟡", "weak": "🟡", "dead": "🔴"}.get(c["status"], "⚪")
            rows += f"""
            <tr style="border-bottom:1px solid #f0f0f0">
              <td style="padding:8px 10px;font-size:13px">{dot} #{cid} — {c['label']}</td>
              <td style="padding:8px 10px;font-size:13px;text-align:right">{c['views']:,}</td>
              <td style="padding:8px 10px;font-size:13px;text-align:right">{c['conversions']}</td>
              <td style="padding:8px 10px;font-size:13px;text-align:right">${c['revenue']:,.2f}</td>
              <td style="padding:8px 10px;font-size:13px;text-align:right">{c['cr_pct']}%</td>
            </tr>"""
        return rows

    def short_name(name, max_len=35):
      return name if len(name) <= max_len else name[:max_len] + "…"
    
    def paid_rows():
        rows = ""
        paid_data = data["traffic_sources"]["paid"]["campaigns"]
        for key, camp in paid_data.items():
            for platform, pd in camp["platforms"].items():
                roi_color = "#16a34a" if (pd.get("purchases", 0) > 0 and pd.get("cpa_sgd") and pd["cpa_sgd"] < 110) else "#dc2626"
                rows += f"""
                <tr style="border-bottom:1px solid #f0f0f0">
                  <td style="padding:8px 10px;font-size:13px">{camp['campaign_name']} — <span style="color:blue">{platform.title()}</span></td>
                  <td style="padding:8px 10px;font-size:13px;text-align:right">{pd['impressions']:,}</td>
                  <td style="padding:8px 10px;font-size:13px;text-align:right">S${pd['spend_sgd']:.2f}</td>
                  <td style="padding:8px 10px;font-size:13px;text-align:right">{pd['lp_ctr_pct']}%</td>
                  <td style="padding:8px 10px;font-size:13px;text-align:right">{pd['purchases']}</td>
                  <td style="padding:8px 10px;font-size:13px;text-align:right;color:{roi_color};font-weight:600">{pd['cr_pct']}%</td>
                </tr>"""
        return rows

    # ── funnel backend rows ──
    def backend_rows():
        rows = ""
        stage_order = ["frontend", "order_bump", "oto1", "oto1_downsell", "oto2", "oto2_downsell"]
        skus = backend["sku_breakdown"]
        sorted_skus = sorted(skus.items(), key=lambda x: stage_order.index(x[1]["stage"]) if x[1]["stage"] in stage_order else 99)
        for sku, s in sorted_skus:
            take = f"{s.get('take_rate_pct', '—')}%" if "take_rate_pct" in s else "—"
            rev_color = "#dc2626" if s["revenue"] == 0 and s["new_sales"] == 0 and s["stage"] not in ("frontend",) else "#111"
            rows += f"""
            <tr style="border-bottom:1px solid #f0f0f0">
              <td style="padding:8px 10px;font-size:13px">{s['label']}</td>
              <td style="padding:8px 10px;font-size:13px;text-align:right">{s['new_sales']}</td>
              <td style="padding:8px 10px;font-size:13px;text-align:right">{s.get('rebills', 0)}</td>
              <td style="padding:8px 10px;font-size:13px;text-align:right">{take}</td>
              <td style="padding:8px 10px;font-size:13px;text-align:right;color:{rev_color};font-weight:{'600' if s['revenue'] > 0 else '400'}">${s['revenue']:,.2f}</td>
            </tr>"""
        return rows

    # ── needle movers ──
    def needle_mover_blocks():
        blocks = ""
        for nm in a["needle_movers"]:
            steps = "".join(f'<li style="margin:4px 0;font-size:13px;color:#374151">{s}</li>' for s in nm["what"])
            blocks += f"""
            <div style="border:1px solid #e5e7eb;border-radius:8px;padding:16px 20px;margin-bottom:12px;border-left:4px solid {'#dc2626' if nm['priority']=='critical' else '#d97706' if nm['priority']=='high' else '#6b7280'}">
              <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;">
                {priority_badge(nm['priority'])}
                <span style="font-weight:700;font-size:14px;color:#111">#{nm['rank']} — {nm['area']}</span>
              </div>
              <p style="margin:0 0 10px;font-size:13px;color:#374151;line-height:1.5">{nm['why']}</p>
              <ul style="margin:0 0 10px;padding-left:18px">{steps}</ul>
              <div style="background:#f9fafb;border-radius:4px;padding:8px 12px;font-size:12px;color:#6b7280">
                <strong style="color:#374151">Revenue impact:</strong> {nm['revenue_impact']}
              </div>
            </div>"""
        return blocks

    # ── scorecard ──
    def scorecard_rows():
        rows = ""
        for row in a["scorecard"]:
            rows += f"""
            <tr style="border-bottom:1px solid #f0f0f0">
              <td style="padding:8px 10px;font-size:13px">{status_dot(row['status'])} {row['area']}</td>
              <td style="padding:8px 10px;font-size:13px;color:#6b7280">{row['cpv_id_or_sku']}</td>
              <td style="padding:8px 10px;font-size:13px;color:#6b7280">{row['status_reason']}</td>
              <td style="padding:8px 10px;font-size:13px">{action_badge(row['action'])}</td>
            </tr>"""
        return rows

    doc_link = f'<p style="text-align:center;margin:0 0 24px"><a href="{doc_url}" style="background:#2563eb;color:#fff;padding:10px 24px;border-radius:6px;text-decoration:none;font-size:13px;font-weight:600">Open in Google Docs (annotate / comment)</a></p>' if doc_url else ""

    cross = data.get("cross_check", {})
    cross_color = {"ok": "#16a34a", "warn": "#d97706", "alert": "#dc2626"}.get(cross.get("status", "ok"), "#6b7280")
    cross_block = f'<p style="font-size:12px;color:{cross_color};margin:8px 0 0">⚠ Data check: {cross.get("note", "")}</p>' if cross.get("status") != "ok" else ""

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif">
<div style="max-width:680px;margin:0 auto;padding:24px 16px">

  <!-- Header -->
  <div style="background:#111;border-radius:10px 10px 0 0;padding:24px 28px">
    <p style="margin:0 0 4px;font-size:11px;color:#9ca3af;letter-spacing:.1em;text-transform:uppercase">Ask Sabrina · Funnel Performance</p>
    <h1 style="margin:0 0 4px;font-size:22px;color:#fff;font-weight:700">Weekly Report</h1>
    <p style="margin:0;font-size:13px;color:#6b7280">{period}</p>
  </div>

  <!-- Snapshot -->
  <div style="background:#fff;padding:24px 28px;border-bottom:1px solid #f0f0f0">
    <h2 style="margin:0 0 16px;font-size:13px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.08em">Funnel Snapshot</h2>
    <div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:16px">
      <div><p style="margin:0 0 4px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em">Revenue</p><p style="margin:0;font-size:24px;font-weight:700;color:#111">${snap['total_revenue_usd']:,.2f}</p></div>
      <div><p style="margin:0 0 4px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em">FE Sales</p><p style="margin:0;font-size:24px;font-weight:700;color:#111">{snap['frontend_sales']}</p></div>
      <div><p style="margin:0 0 4px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em">Avg/Buyer</p><p style="margin:0;font-size:24px;font-weight:700;color:#111">${snap['avg_revenue_per_buyer_usd']:,.2f}</p></div>
      <div><p style="margin:0 0 4px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em">Ad Spend</p><p style="margin:0;font-size:24px;font-weight:700;color:#111">S${snap['paid_traffic_spend_sgd']:,.2f}</p><p style="margin:2px 0 0;font-size:11px;color:#9ca3af">~${snap['paid_traffic_spend_usd']:,.2f} USD</p></div>
    </div>
    {cross_block}
  </div>

  <!-- RPV Northstar -->
  <div style="background:#eff6ff;padding:16px 28px;border-bottom:1px solid #dbeafe">
    <p style="margin:0 0 6px;font-size:11px;font-weight:700;color:#1d4ed8;text-transform:uppercase;letter-spacing:.08em">Northstar — Revenue Per Visitor</p>
    <div style="display:flex;gap:32px">
      <div><span style="font-size:20px;font-weight:700;color:#1e3a8a">${ns['email_rpv']}</span><span style="font-size:12px;color:#3b82f6;margin-left:6px;margin-right:6px">Email RPV</span></div>
      <div><span style="font-size:20px;font-weight:700;color:#1e3a8a">${ns['paid_rpv_usd']}</span><span style="font-size:12px;color:#3b82f6;margin-left:6px;margin-right:6px">Paid RPV</span></div>
      <div><span style="font-size:20px;font-weight:700;color:#1e3a8a">${ns['avg_funnel_value']}</span><span style="font-size:12px;color:#3b82f6;margin-left:6px;margin-right:6px">Avg Funnel Value</span></div>
    </div>
    <p style="margin:8px 0 0;font-size:12px;color:#1d4ed8">{ns['commentary']}</p>
  </div>

  <!-- Summary -->
  <div style="background:#fff;padding:20px 28px;border-bottom:1px solid #f0f0f0">
    <p style="margin:0;font-size:14px;color:#374151;line-height:1.6">{a['period_summary']}</p>
  </div>

  <!-- Doc link -->
  <div style="background:#fff;padding:16px 28px;border-bottom:1px solid #f0f0f0">
    {doc_link}
  </div>

  <!-- Needle Movers -->
  <div style="background:#fff;padding:24px 28px;border-bottom:1px solid #f0f0f0">
    <h2 style="margin:0 0 16px;font-size:13px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.08em">Needle Movers — Priority Order</h2>
    {needle_mover_blocks()}
  </div>

  <!-- Email campaigns -->
  <div style="background:#fff;padding:24px 28px;border-bottom:1px solid #f0f0f0">
    <h2 style="margin:0 0 12px;font-size:13px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.08em">Email Traffic (Maropost via CPV Labs)</h2>
    <table style="width:100%;border-collapse:collapse">
      <thead><tr style="background:#f9fafb">
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:left;font-weight:600;text-transform:uppercase;letter-spacing:.06em">Campaign</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em">Views</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em">Conv</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em">Revenue</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em">CR%</th>
      </tr></thead>
      <tbody>{camp_rows(email_camps)}</tbody>
      <tfoot><tr style="background:#f9fafb;font-weight:700">
        <td style="padding:8px 10px;font-size:13px">Total</td>
        <td style="padding:8px 10px;font-size:13px;text-align:right">{tracking['totals_by_source']['email']['views']:,}</td>
        <td style="padding:8px 10px;font-size:13px;text-align:right">{tracking['totals_by_source']['email']['conversions']}</td>
        <td style="padding:8px 10px;font-size:13px;text-align:right">${tracking['totals_by_source']['email']['revenue']:,.2f}</td>
        <td style="padding:8px 10px;font-size:13px;text-align:right"></td>
      </tfoot>
    </table>
  </div>

  <!-- Paid campaigns -->
  <div style="background:#fff;padding:24px 28px;border-bottom:1px solid #f0f0f0">
    <h2 style="margin:0 0 12px;font-size:13px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.08em">Paid Traffic (Facebook / Instagram)</h2>
    <table style="width:100%;border-collapse:collapse">
      <thead><tr style="background:#f9fafb">
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:left;font-weight:600;text-transform:uppercase;letter-spacing:.06em">Campaign</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em">Impressions</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em">Spend</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em">LP CTR</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em">Sales</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em">CR%</th>
      </tr></thead>
      <tbody>{paid_rows()}</tbody>
    </table>
  </div>

  <!-- Funnel backend -->
  <div style="background:#fff;padding:24px 28px;border-bottom:1px solid #f0f0f0">
    <h2 style="margin:0 0 12px;font-size:13px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.08em">Funnel Backend (ClickBank)</h2>
    <table style="width:100%;border-collapse:collapse">
      <thead><tr style="background:#f9fafb">
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:left;font-weight:600;text-transform:uppercase;letter-spacing:.06em">Stage</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em">New Sales</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em">Rebills</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em">Take Rate</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em">Revenue</th>
      </tr></thead>
      <tbody>{backend_rows()}</tbody>
      <tfoot><tr style="background:#f9fafb;font-weight:700">
        <td style="padding:8px 10px;font-size:13px">Total</td>
        <td style="padding:8px 10px;font-size:13px;text-align:right">{sum(s.get('new_sales',0) for s in backend['sku_breakdown'].values())}</td>
        <td style="padding:8px 10px;font-size:13px;text-align:right">{sum(s.get('rebills',0) for s in backend['sku_breakdown'].values())}</td>
        <td></td>
        <td style="padding:8px 10px;font-size:13px;text-align:right">${backend['total_revenue']:,.2f}</td>
      </tfoot>
    </table>
    <p style="margin:10px 0 0;font-size:12px;color:#6b7280">Advanced vs Basic: {backend['frontend_mix']['advanced_pct']}% choosing ${backend['sku_breakdown']['abdt-advanced']['price']} Advanced ({backend['frontend_mix']['advanced_count']} of {backend['frontend_sales_count']} buyers)</p>
  </div>

  <!-- Scorecard -->
  <div style="background:#fff;padding:24px 28px;border-radius:0 0 10px 10px">
    <h2 style="margin:0 0 12px;font-size:13px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.08em">Summary Scorecard</h2>
    <table style="width:100%;border-collapse:collapse">
      <thead><tr style="background:#f9fafb">
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:left;font-weight:600;text-transform:uppercase;letter-spacing:.06em">Area</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:left;font-weight:600;text-transform:uppercase;letter-spacing:.06em">ID / SKU</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:left;font-weight:600;text-transform:uppercase;letter-spacing:.06em">Why</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:left;font-weight:600;text-transform:uppercase;letter-spacing:.06em">Action</th>
      </tr></thead>
      <tbody>{scorecard_rows()}</tbody>
    </table>
  </div>

  <!-- Footer -->
  <div style="padding:20px 0;text-align:center">
    <p style="margin:0;font-size:11px;color:#9ca3af">Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC · Ask Sabrina Funnel Reporter</p>
    {'<p style="margin:4px 0 0;font-size:11px;color:#9ca3af">Data notes: ' + a.get('data_notes','') + '</p>' if a.get('data_notes') else ''}
  </div>

</div>
</body>
</html>"""

    return html


def send_email(services, html: str, subject: str):
    sendgrid_key = os.environ.get("SENDGRID_API_KEY", "")

    if sendgrid_key:
        # SendGrid — works with personal Gmail, no domain-wide delegation needed
        resp = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={
                "Authorization": f"Bearer {sendgrid_key}",
                "Content-Type":  "application/json",
            },
            json={
                "personalizations": [{"to": [{"email": EMAIL_TO}]}],
                "from":    {"email": EMAIL_FROM},
                "subject": subject,
                "content": [{"type": "text/html", "value": html}],
            },
            timeout=30,
        )
        if resp.status_code == 202:
            print(f"[report] ✓ Email sent to {EMAIL_TO} via SendGrid")
        else:
            raise Exception(f"SendGrid {resp.status_code}: {resp.text[:200]}")


# ── 2. GOOGLE SHEETS ──────────────────────────────────────────────────────────

SHEET_HEADERS = [
    "Week Start", "Week End", "Revenue USD", "FE Sales", "Avg/Buyer USD",
    "Ad Spend SGD", "Ad Spend USD",
    "Email Views", "Email Convs", "Email Revenue", "Email RPV",
    "Paid Views", "Paid Convs", "Paid Spend SGD", "Paid RPV USD",
    "OTO1 Take %", "OTO2 Take %", "OB Take %", "Advanced %",
    "Top Needle Mover", "Doc URL",
]

def ensure_sheet_headers(services, spreadsheet_id: str):
    """Create tab and add headers if this is the first run."""
    sheet = services["sheets"].spreadsheets()

    # Check existing sheets
    meta = sheet.get(spreadsheetId=spreadsheet_id).execute()
    existing = [s["properties"]["title"] for s in meta["sheets"]]

    if SHEET_TAB not in existing:
        sheet.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": SHEET_TAB}}}]}
        ).execute()
        print(f"[report] Created sheet tab '{SHEET_TAB}'")

    # Check if headers row exists
    result = sheet.values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{SHEET_TAB}!A1:A1"
    ).execute()

    if not result.get("values"):
        sheet.values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{SHEET_TAB}!A1",
            valueInputOption="RAW",
            body={"values": [SHEET_HEADERS]}
        ).execute()
        print(f"[report] Added headers to '{SHEET_TAB}'")


def append_sheets_row(services, data: dict, analysis: dict, doc_url: str = None):
    snap     = data["funnel_snapshot"]
    tracking = data["traffic_sources"]["tracking"]
    backend  = data["funnel_backend"]
    a        = analysis["analysis"]
    ns       = a["northstar"]

    email_totals = tracking["totals_by_source"].get("email", {})
    paid_totals  = tracking["totals_by_source"].get("facebook", {})
    paid_data    = data["traffic_sources"]["paid"]

    oto1_take = backend["sku_breakdown"].get("SSR", {}).get("take_rate_pct", 0)
    oto2_take = backend["sku_breakdown"].get("dhr", {}).get("take_rate_pct", 0)
    ob_take   = backend["sku_breakdown"].get("SPR-OB2", {}).get("new_sales", 0)
    ob_take_pct = round(ob_take / backend["frontend_sales_count"] * 100, 1) if backend["frontend_sales_count"] else 0

    top_nm = a["needle_movers"][0]["area"] if a["needle_movers"] else ""

    email_views = email_totals.get("views", 0) or 0
    email_rev   = email_totals.get("revenue", 0) or 0
    email_rpv   = round(email_rev / email_views, 3) if email_views else 0

    paid_views  = paid_totals.get("views", 0) or 0
    paid_rev    = paid_totals.get("revenue", 0) or 0

    row = [
        data["meta"]["period_start"],
        data["meta"]["period_end"],
        snap["total_revenue_usd"],
        snap["frontend_sales"],
        snap["avg_revenue_per_buyer_usd"],
        snap["paid_traffic_spend_sgd"],
        snap["paid_traffic_spend_usd"],
        email_views,
        email_totals.get("conversions", 0),
        email_rev,
        email_rpv,
        paid_views,
        paid_totals.get("conversions", 0),
        paid_data.get("total_spend_sgd", 0),
        ns["paid_rpv_usd"],
        oto1_take,
        oto2_take,
        ob_take_pct,
        backend["frontend_mix"]["advanced_pct"],
        top_nm,
        doc_url or "",
    ]

    ensure_sheet_headers(services, SHEET_ID)
    services["sheets"].spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_TAB}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]}
    ).execute()
    print(f"[report] ✓ Row appended to Google Sheets")


# ── 3. GOOGLE DOC ─────────────────────────────────────────────────────────────

def create_google_doc(services, data: dict, analysis: dict) -> str:
    """Creates a new Google Doc with the full report. Returns the doc URL."""
    period     = f"{data['meta']['period_start']} → {data['meta']['period_end']}"
    snap       = data["funnel_snapshot"]
    backend    = data["funnel_backend"]
    tracking   = data["traffic_sources"]["tracking"]
    a          = analysis["analysis"]
    ns         = a["northstar"]

    title = f"Ask Sabrina — Weekly Report {data['meta']['period_start']}"

    # Create blank doc
    folder_id = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")

    # Create via Drive API instead of Docs API — works with service accounts
    file_metadata = {
        "name":     title,
        "mimeType": "application/vnd.google-apps.document",
    }
    if folder_id:
        file_metadata["parents"] = [folder_id]

    drive_file = services["drive"].files().create(
        body=file_metadata,
        fields="id",
        supportsAllDrives=True,
    ).execute()
    doc_id = drive_file["id"]

    # Build content as a series of insertText + formatting requests
    # We build the text first, track character positions, then apply styles
    requests = []
    cursor   = 1  # Google Docs index starts at 1

    def insert(text, style=None):
        nonlocal cursor
        requests.append({"insertText": {"location": {"index": cursor}, "text": text}})
        start = cursor
        cursor += len(text)
        if style:
            requests.append({"updateParagraphStyle": {
                "range": {"startIndex": start, "endIndex": cursor},
                "paragraphStyle": {"namedStyleType": style},
                "fields": "namedStyleType"
            }})
        return start, cursor

    def insert_text_style(text, bold=False, color=None):
        nonlocal cursor
        requests.append({"insertText": {"location": {"index": cursor}, "text": text}})
        start = cursor
        cursor += len(text)
        ts = {}
        if bold:
            ts["bold"] = True
        if color:
            ts["foregroundColor"] = {"color": {"rgbColor": color}}
        if ts:
            requests.append({"updateTextStyle": {
                "range": {"startIndex": start, "endIndex": cursor},
                "textStyle": ts,
                "fields": ",".join(ts.keys())
            }})

    # Title
    insert(f"Ask Sabrina — Weekly Funnel Report\n", "HEADING_1")
    insert(f"Period: {period}\n", "NORMAL_TEXT")
    insert(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC\n\n", "NORMAL_TEXT")

    # Snapshot
    insert("Funnel Snapshot\n", "HEADING_2")
    insert(f"Total Revenue: ${snap['total_revenue_usd']:,.2f}\n")
    insert(f"Front-End Sales: {snap['frontend_sales']}\n")
    insert(f"Avg Revenue/Buyer: ${snap['avg_revenue_per_buyer_usd']:,.2f}\n")
    insert(f"Paid Traffic Spend: S${snap['paid_traffic_spend_sgd']:,.2f} (~${snap['paid_traffic_spend_usd']:,.2f} USD)\n\n")

    # RPV
    insert("Northstar — Revenue Per Visitor\n", "HEADING_2")
    insert(f"Email RPV: ${ns['email_rpv']}  |  Paid RPV: ${ns['paid_rpv_usd']}  |  Avg Funnel Value: ${ns['avg_funnel_value']}\n")
    insert(f"{ns['commentary']}\n\n")

    # Summary
    insert("Week Summary\n", "HEADING_2")
    insert(f"{a['period_summary']}\n\n")

    # Needle movers
    insert("Needle Movers\n", "HEADING_2")
    for nm in a["needle_movers"]:
        priority_label = {"critical": "🔴", "high": "🟡", "medium": "⚪"}.get(nm["priority"], "")
        insert(f"{priority_label} #{nm['rank']} — {nm['area']}\n", "HEADING_3")
        insert(f"Why: {nm['why']}\n")
        for step in nm["what"]:
            insert(f"  • {step}\n")
        insert(f"Revenue impact: {nm['revenue_impact']}\n\n")

    # Funnel backend
    insert("Funnel Backend (ClickBank)\n", "HEADING_2")
    for sku, s in backend["sku_breakdown"].items():
        take = f"  |  Take: {s.get('take_rate_pct', '—')}%" if "take_rate_pct" in s else ""
        insert(f"{s['label']}: {s['new_sales']} sales  |  ${s['revenue']:,.2f}{take}\n")
    insert(f"\nAdvanced vs Basic: {backend['frontend_mix']['advanced_pct']}% choosing Advanced\n\n")

    # Scorecard
    insert("Summary Scorecard\n", "HEADING_2")
    for row in a["scorecard"]:
        dot = {"green": "🟢", "yellow": "🟡", "red": "🔴"}.get(row["status"], "⚪")
        insert(f"{dot} {row['area']} ({row['cpv_id_or_sku']}) — {row['status_reason']} → {row['action'].upper()}\n")

    # Notes
    if a.get("data_notes"):
        insert("\nData Notes\n", "HEADING_2")
        insert(f"{a['data_notes']}\n")

    # Action notes section (blank, for annotation)
    insert("\nAction Notes (add your comments here)\n", "HEADING_2")
    insert("_\n")

    # Execute all requests in one batch
    services["docs"].documents().batchUpdate(
        documentId=doc_id,
        body={"requests": requests}
    ).execute()

    doc_url = f"https://docs.google.com/document/d/{doc_id}/edit"
    print(f"[report] ✓ Google Doc created: {doc_url}")
    return doc_url


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    # Load data
    with open(args.data) as f:
        data = json.load(f)
    with open(args.analysis) as f:
        analysis = json.load(f)

    period  = f"{data['meta']['period_start']} → {data['meta']['period_end']}"
    subject = f"Ask Sabrina — Weekly Report {data['meta']['period_start']}"

    doc_url = None

    if args.mock:
        print("[report] Mock mode — skipping all Google API calls")
        html = build_email_html(data, analysis, doc_url="https://docs.google.com/document/d/MOCK")
        Path("output").mkdir(exist_ok=True)
        Path("output/email.html").write_text(html)
        print("[report] ✓ Email preview saved to output/email.html")
        print("[report]   Open it in your browser to check the layout")
        return

    # Live mode
    print("[report] Authenticating with Google...")
    services = get_google_services()

    if not args.no_docs:
        print("[report] Skipping Google Doc (not supported with personal Gmail service accounts)")
        print("[report]   Run with --no-docs to suppress this message")
        doc_url = None

    if not args.no_sheets:
        print("[report] Appending to Google Sheets...")
        append_sheets_row(services, data, analysis, doc_url)

    if not args.no_email:
        if not EMAIL_TO or not EMAIL_FROM:
            print("[report] ⚠ REPORT_EMAIL_TO or REPORT_EMAIL_FROM not set — skipping email send")
            print("[report]   Add them to .env to enable email delivery")
        else:
            html = build_email_html(data, analysis, doc_url)
            Path("output/email.html").write_text(html)
            print("[report] Sending email...")
            send_email(services, html, subject)

    print(f"\n[report] Done ✓")
    if doc_url:
        print(f"  Doc: {doc_url}")
    print(f"  Sheet: https://docs.google.com/spreadsheets/d/{SHEET_ID}")


if __name__ == "__main__":
    main()