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
    p.add_argument("--data",       default="")
    p.add_argument("--analysis",   default="")
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

def build_variant_section(data: dict, analysis: dict) -> str:
    """Builds the /destiny vs /destiny/v2 comparison section for the email."""
    variants = data.get("funnel_variants") or {}
    cb       = data.get("funnel_backend", {})
    variant_sales = cb.get("variant_sales", {})

    if not variants:
        return ""

    def pct_color(val, good=10, warn=5):
        if val >= good:   return "#16a34a"
        if val >= warn:   return "#d97706"
        return "#dc2626"

    def drop_bar(pct):
        """Visual bar showing retention at each step."""
        width = max(int(pct), 2)
        color = "#16a34a" if pct >= 70 else "#d97706" if pct >= 40 else "#dc2626"
        return f'<div style="background:{color};height:6px;width:{width}%;border-radius:3px;display:inline-block"></div>'

    def fmt_sec(sec):
        m, s = divmod(int(sec), 60)
        return f"{m}m {s}s"

    # Build per-variant column
    def variant_col(vkey, vdata):
        if "error" in vdata:
            return f'<td style="padding:8px 10px;font-size:13px;color:#dc2626">Error: {vdata["error"][:60]}</td>'

        cb_sales   = variant_sales.get(vkey, {}).get("sales", 0)
        cb_rev     = variant_sales.get(vkey, {}).get("revenue", 0)
        sessions   = vdata.get("sessions", 0)
        checkout   = vdata.get("checkout_total", 0)
        optin_done = vdata.get("optin_completed", 0)
        co_to_sale = round(cb_sales / checkout * 100, 1) if checkout else 0
        v_to_sale  = round(cb_sales / sessions * 100, 2) if sessions else 0

        dropoff    = vdata.get("dropoff", {})

        return f"""
        <td style="padding:8px 12px;font-size:13px;vertical-align:top;border-left:1px solid #f0f0f0">
          <div style="font-weight:700;font-size:14px;margin-bottom:10px">{vdata['label']}</div>

          <div style="margin-bottom:14px">
            <div style="font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px">Traffic</div>
            <div>{sessions:,} sessions · {vdata.get('total_users',0):,} users</div>
            <div style="font-size:12px;color:#6b7280">{fmt_sec(vdata.get('avg_session_sec',0))} avg · {vdata.get('bounce_rate_pct',0)}% bounce</div>
          </div>

          <div style="margin-bottom:14px">
            <div style="font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px">Funnel Drop-off</div>
            <table style="width:100%;border-collapse:collapse;font-size:12px">
              <tr><td style="padding:2px 0;color:#374151">Session → Choice</td><td style="text-align:right;color:{pct_color(dropoff.get('session_to_choice',0),20,10)};font-weight:600">{dropoff.get('session_to_choice',0)}%</td></tr>
              <tr><td style="padding:2px 0;color:#374151">Choice → Opt-in S1</td><td style="text-align:right;color:{pct_color(dropoff.get('choice_to_optin1',0),60,40)};font-weight:600">{dropoff.get('choice_to_optin1',0)}%</td></tr>
              <tr><td style="padding:2px 0;color:#374151">S1 → S2</td><td style="text-align:right;color:{pct_color(dropoff.get('optin1_to_optin2',0),75,55)};font-weight:600">{dropoff.get('optin1_to_optin2',0)}%</td></tr>
              <tr><td style="padding:2px 0;color:#374151">S2 → S3</td><td style="text-align:right;color:{pct_color(dropoff.get('optin2_to_optin3',0),75,55)};font-weight:600">{dropoff.get('optin2_to_optin3',0)}%</td></tr>
              <tr><td style="padding:2px 0;color:#374151">S3 → Opt-in Done</td><td style="text-align:right;color:{pct_color(dropoff.get('optin3_to_complete',0),75,55)};font-weight:600">{dropoff.get('optin3_to_complete',0)}%</td></tr>
              <tr><td style="padding:2px 0;color:#374151">Done → Checkout</td><td style="text-align:right;color:{pct_color(dropoff.get('complete_to_checkout',0),50,30)};font-weight:600">{dropoff.get('complete_to_checkout',0)}%</td></tr>
            </table>
          </div>

          <div style="margin-bottom:14px">
            <div style="font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px">Checkout</div>
            <div>{checkout:,} total clicks ({vdata.get('checkout_rate_pct',0)}% of sessions)</div>
            <div style="font-size:12px;color:#6b7280">Basic: {vdata.get('checkout_basic',0):,} · Advanced: {vdata.get('checkout_advanced',0):,}</div>
          </div>

          <div style="margin-bottom:6px">
            <div style="font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px">ClickBank Sales</div>
            <div style="font-weight:700;font-size:16px">{cb_sales} sales · ${cb_rev:,.2f}</div>
            <div style="font-size:12px;color:#6b7280">Checkout→Sale: <span style="color:{pct_color(co_to_sale,30,15)};font-weight:600">{co_to_sale}%</span> · Visitor→Sale: <span style="font-weight:600">{v_to_sale}%</span></div>
          </div>
        </td>"""

    v1_data = variants.get("destiny_v1", {})
    v2_data = variants.get("destiny_v2", {})

    # Biggest drop-off finder
    def biggest_dropoff(vdata):
        if "error" in vdata:
            return "N/A", 0
        d = vdata.get("dropoff", {})
        stages = {
            "Session → Choice":    d.get("session_to_choice", 100),
            "Choice → Opt-in S1":  d.get("choice_to_optin1", 100),
            "S1 → S2":             d.get("optin1_to_optin2", 100),
            "S2 → S3":             d.get("optin2_to_optin3", 100),
            "S3 → Complete":       d.get("optin3_to_complete", 100),
            "Complete → Checkout": d.get("complete_to_checkout", 100),
        }
        worst = min(stages, key=stages.get)
        return worst, stages[worst]

    v1_worst, v1_worst_pct = biggest_dropoff(v1_data)
    v2_worst, v2_worst_pct = biggest_dropoff(v2_data)

    return f"""
  <!-- Funnel Variant Performance -->
  <div style="background:#fff;padding:24px 28px;border-top:3px solid #7c3aed;margin-top:2px">
    <h2 style="margin:0 0 6px;font-size:13px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.08em">Front-end Funnel Performance</h2>
    <p style="margin:0 0 16px;font-size:12px;color:#9ca3af">⚠ Variants are running separate traffic — not split tested. Compare directionally, not definitively.</p>

    <table style="width:100%;border-collapse:collapse">
      <thead><tr style="background:#f9fafb">
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:left;font-weight:600;text-transform:uppercase;letter-spacing:.06em;width:30%">Metric</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:left;font-weight:600;text-transform:uppercase;letter-spacing:.06em">/destiny (Full Screen)</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:left;font-weight:600;text-transform:uppercase;letter-spacing:.06em">/destiny/v2 (Headline)</th>
      </tr></thead>
      <tbody>
        <tr style="border-bottom:1px solid #f0f0f0"><td style="padding:7px 10px;font-size:12px;color:#6b7280">Sessions</td><td style="padding:7px 10px;font-size:13px">{v1_data.get('sessions',0):,}</td><td style="padding:7px 10px;font-size:13px">{v2_data.get('sessions',0):,}</td></tr>
        <tr style="border-bottom:1px solid #f0f0f0"><td style="padding:7px 10px;font-size:12px;color:#6b7280">Avg session</td><td style="padding:7px 10px;font-size:13px">{fmt_sec(v1_data.get('avg_session_sec',0))}</td><td style="padding:7px 10px;font-size:13px">{fmt_sec(v2_data.get('avg_session_sec',0))}</td></tr>
        <tr style="border-bottom:1px solid #f0f0f0"><td style="padding:7px 10px;font-size:12px;color:#6b7280">Choice selected</td><td style="padding:7px 10px;font-size:13px">{v1_data.get('choice_selected',0):,} <span style="color:#6b7280;font-size:12px">({v1_data.get('choice_rate_pct',0)}%)</span></td><td style="padding:7px 10px;font-size:13px">{v2_data.get('choice_selected',0):,} <span style="color:#6b7280;font-size:12px">({v2_data.get('choice_rate_pct',0)}%)</span></td></tr>
        <tr style="border-bottom:1px solid #f0f0f0"><td style="padding:7px 10px;font-size:12px;color:#6b7280">Opt-in completed</td><td style="padding:7px 10px;font-size:13px">{v1_data.get('optin_completed',0):,} <span style="color:#6b7280;font-size:12px">({v1_data.get('optin_rate_pct',0)}%)</span></td><td style="padding:7px 10px;font-size:13px">{v2_data.get('optin_completed',0):,} <span style="color:#6b7280;font-size:12px">({v2_data.get('optin_rate_pct',0)}%)</span></td></tr>
        <tr style="border-bottom:1px solid #f0f0f0"><td style="padding:7px 10px;font-size:12px;color:#6b7280">Checkout clicks</td><td style="padding:7px 10px;font-size:13px">{v1_data.get('checkout_total',0):,} <span style="color:#6b7280;font-size:12px">({v1_data.get('checkout_rate_pct',0)}%)</span></td><td style="padding:7px 10px;font-size:13px">{v2_data.get('checkout_total',0):,} <span style="color:#6b7280;font-size:12px">({v2_data.get('checkout_rate_pct',0)}%)</span></td></tr>
        <tr style="border-bottom:1px solid #f0f0f0"><td style="padding:7px 10px;font-size:12px;color:#6b7280">ClickBank sales</td><td style="padding:7px 10px;font-size:13px;font-weight:700">{variant_sales.get('destiny_v1',{}).get('sales',0)}</td><td style="padding:7px 10px;font-size:13px;font-weight:700">{variant_sales.get('destiny_v2',{}).get('sales',0)}</td></tr>
        <tr style="border-bottom:1px solid #f0f0f0"><td style="padding:7px 10px;font-size:12px;color:#6b7280">Checkout→Sale</td>
          <td style="padding:7px 10px;font-size:13px;font-weight:600;color:{pct_color(round(variant_sales.get('destiny_v1',{}).get('sales',0)/max(v1_data.get('checkout_total',1),1)*100,1),30,15)}">{round(variant_sales.get('destiny_v1',{}).get('sales',0)/max(v1_data.get('checkout_total',1),1)*100,1)}%</td>
          <td style="padding:7px 10px;font-size:13px;font-weight:600;color:{pct_color(round(variant_sales.get('destiny_v2',{}).get('sales',0)/max(v2_data.get('checkout_total',1),1)*100,1),30,15)}">{round(variant_sales.get('destiny_v2',{}).get('sales',0)/max(v2_data.get('checkout_total',1),1)*100,1)}%</td>
        </tr>
        <tr style="background:#fffbeb"><td style="padding:7px 10px;font-size:12px;color:#92400e;font-weight:600">Biggest drop-off</td>
          <td style="padding:7px 10px;font-size:12px;color:#dc2626;font-weight:600">{v1_worst} ({v1_worst_pct}%)</td>
          <td style="padding:7px 10px;font-size:12px;color:#dc2626;font-weight:600">{v2_worst} ({v2_worst_pct}%)</td>
        </tr>
      </tbody>
    </table>
  </div>"""



def _build_variant_section_safe(data, analysis):
    """Deferred call to build_variant_section — avoids forward reference issue."""
    try:
        return build_variant_section(data, analysis)
    except Exception as e:
        return f'<div style="padding:20px;color:#dc2626">Variant section error: {e}</div>'


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

    def paid_rows():
        rows = ""
        paid_data = data["traffic_sources"]["paid"]["campaigns"]
        for key, camp in paid_data.items():
            for platform, pd in camp["platforms"].items():
                roi_color = "#16a34a" if (pd.get("purchases", 0) > 0 and pd.get("cpa_sgd") and pd["cpa_sgd"] < 110) else "#dc2626"
                rows += f"""
                <tr style="border-bottom:1px solid #f0f0f0">
                  <td style="padding:8px 10px;font-size:13px">#{camp['cpv_campaign_id']} — {platform.title()}</td>
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
              <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
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
      <head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{period} — Ask Sabrina Funnel Report</title></head>
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
            <div><span style="font-size:20px;font-weight:700;color:#1e3a8a">${ns['email_rpv']}</span><span style="font-size:12px;color:#3b82f6;margin-left:6px">Email RPV</span></div>
            <div><span style="font-size:20px;font-weight:700;color:#1e3a8a">${ns['paid_rpv_usd']}</span><span style="font-size:12px;color:#3b82f6;margin-left:6px">Paid RPV</span></div>
            <div><span style="font-size:20px;font-weight:700;color:#1e3a8a">${ns['avg_funnel_value']}</span><span style="font-size:12px;color:#3b82f6;margin-left:6px">Avg Funnel Value</span></div>
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

        VARIANT_SECTION_PLACEHOLDER

        <!-- Footer -->
        <div style="padding:20px 0;text-align:center">
          <p style="margin:0;font-size:11px;color:#9ca3af">Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC · Ask Sabrina Funnel Reporter</p>
          {'<p style="margin:4px 0 0;font-size:11px;color:#9ca3af">Data notes: ' + a.get('data_notes','') + '</p>' if a.get('data_notes') else ''}
        </div>

      </div>
      </body>
      </html>"""

    # Now inject variant section (function defined later in module)
    html = html.replace("VARIANT_SECTION_PLACEHOLDER", _build_variant_section_safe(data, analysis))
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
    doc = services["docs"].documents().create(body={"title": title}).execute()
    doc_id = doc["documentId"]

    # Move to Drive root (optional: move to a folder by ID)
    services["drive"].files().update(
        fileId=doc_id,
        addParents="root",
    ).execute()

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

def build_index_html(output_dir: str = "output") -> None:
    """Scans output/ for email_*.html files and generates index.html."""
    import glob
    from pathlib import Path
    from datetime import datetime

    files = sorted(glob.glob(f"{output_dir}/email_*.html"), reverse=True)
    if not files:
        return

    rows = ""
    for i, filepath in enumerate(files):
        filename = Path(filepath).name
        date_part = filename.replace("email_", "").replace(".html", "")
        try:
            dt = datetime.strptime(date_part, "%Y_%m_%d")
            label = dt.strftime("%B %d, %Y")
        except Exception:
            label = date_part.replace("_", "-")

        # Try to pull revenue from matching report_data file for subtitle
        subtitle = ""
        data_file = Path(output_dir) / f"report_data_{date_part}.json"
        if data_file.exists():
            try:
                import json as _json
                d = _json.loads(data_file.read_text())
                rev  = d.get("funnel_snapshot", {}).get("total_revenue_usd", 0)
                sales = d.get("funnel_snapshot", {}).get("frontend_sales", 0)
                subtitle = f'<span style="font-size:12px;color:#6b7280;margin-left:8px">${rev:,.2f} · {sales} sales</span>'
            except Exception:
                pass

        is_latest = i == 0
        badge = '<span style="background:#16a34a;color:#fff;font-size:10px;font-weight:700;padding:2px 8px;border-radius:3px;margin-left:8px;letter-spacing:.05em">LATEST</span>' if is_latest else ""
        rows += f"""
        <tr style="border-bottom:1px solid #f0f0f0">
          <td style="padding:12px 16px;background:{'#f0fdf4' if is_latest else '#fff'}">
            <a href="{filename}" style="font-size:15px;font-weight:{'700' if is_latest else '400'};color:#111;text-decoration:none">{label}</a>
            {badge}
            {subtitle}
          </td>
          <td style="padding:12px 16px;text-align:right;background:{'#f0fdf4' if is_latest else '#fff'}">
            <a href="{filename}" style="font-size:13px;color:#2563eb;text-decoration:none">View report →</a>
          </td>
        </tr>"""

    html = f"""<!DOCTYPE html>
      <html>
      <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width,initial-scale=1">
        <title>Ask Sabrina — Weekly Reports</title>
        <style>
          * {{ box-sizing: border-box; margin: 0; padding: 0; }}
          body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f3f4f6;}}
          .container {{ max-width: 600px; margin: 60px auto; padding: 0 16px; }}
          .header {{ background: #111; border-radius: 10px 10px 0 0; padding: 24px 28px; }}
          .header p {{ color: #9ca3af; font-size: 11px; letter-spacing: .1em; text-transform: uppercase; margin-bottom: 4px; }}
          .header h1 {{ color: #fff; font-size: 22px; font-weight: 700; }}
          .card {{ background: #fff; border-radius: 0 0 10px 10px; overflow: hidden; }}
          table {{ width: 100%; border-collapse: collapse; }}
          tr:hover td {{ background: #f9fafb !important; }}
          .footer {{ text-align: center; padding: 20px 0; font-size: 11px; color: #9ca3af; }}
        </style>
      </head>
      <body>
        <div class="container">
          <div class="header">
            <p>Ask Sabrina · Funnel Performance</p>
            <h1>Weekly Reports</h1>
            <p style="color:#6b7280;font-size:13px;margin-top:6px">Funnel · Campaign · Revenue · Drop-off Analysis</p>
          </div>
          <div class="card">
            <table>{rows}
            </table>
          </div>
          <div class="footer">
            {len(files)} report{'s' if len(files) != 1 else ''} · Auto-generated each Monday
          </div>
        </div>
      </body>
      </html>"""

    index_path = Path(output_dir) / "index.html"
    index_path.write_text(html)
    print(f"[report] ✓ Index updated → {index_path} ({len(files)} reports listed)")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    # Auto-detect latest files if not specified
    if not args.data:
        import glob
        files = sorted(glob.glob("output/report_data_*.json"))
        args.data = files[-1] if files else "output/report_data.json"
        print(f"[report] Auto-detected data: {args.data}")

    if not args.analysis:
        import glob
        files = sorted(glob.glob("output/analysis_*.json"))
        args.analysis = files[-1] if files else "output/analysis.json"
        print(f"[report] Auto-detected analysis: {args.analysis}")

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
        Path("docs").mkdir(exist_ok=True)
        Path("docs/email.html").write_text(html)
        print("[report] ✓ Email preview saved to docs/email.html")
        print("[report]   Open it in your browser to check the layout")
        return

    # Live mode
    print("[report] Authenticating with Google...")
    services = get_google_services()

    # if not args.no_docs:
    #     print("[report] Creating Google Doc...")
    #     doc_url = create_google_doc(services, data, analysis)
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
            date_part = Path(args.data).stem.replace("report_data_", "")
            email_path = f"docs/email_{date_part}.html"
            Path("docs").mkdir(exist_ok=True)
            Path(email_path).write_text(html)
            print(f"[report] Email saved to {email_path}")
            print("[report] Sending email...")
            send_email(services, html, subject)

            build_index_html("docs")

    print(f"\n[report] Done ✓")
    if doc_url:
        print(f"  Doc: {doc_url}")
    print(f"  Sheet: https://docs.google.com/spreadsheets/d/{SHEET_ID}")


if __name__ == "__main__":
    main()


# ── FUNNEL VARIANT SECTION (injected into email HTML) ─────────────────────────