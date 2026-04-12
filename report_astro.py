"""
AstroLover Sketch — Report Builder
Reads report_data + analysis JSON and produces:
  1. HTML email saved to docs/astroloversketch/email_YYYY_MM_DD.html
  2. Google Sheets row appended to trend tracker
  3. Email sent via SendGrid

Usage:
    python report_astro.py                 # full run
    python report_astro.py --mock          # preview email.html only, no API calls
    python report_astro.py --no-email      # sheets only
    python report_astro.py --no-sheets     # email only
"""

import argparse
import glob
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

load_dotenv()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

CREDENTIALS_FILE = os.environ.get("GOOGLE_CREDENTIALS_FILE", "google_credentials.json")
SHEET_ID         = os.environ.get("ASTRO_SHEET_ID", "")
SHEET_TAB        = os.environ.get("ASTRO_SHEET_TAB", "Weekly Data")
EMAIL_TO         = os.environ.get("ASTRO_REPORT_EMAIL_TO", "")
EMAIL_FROM       = os.environ.get("ASTRO_REPORT_EMAIL_FROM", "")
EMAIL_NAME       = os.environ.get("ASTRO_REPORT_EMAIL_NAME", "Funnel Reports")


def parse_args():
    p = argparse.ArgumentParser(description="Build AstroLover Sketch funnel report")
    p.add_argument("--data",      default="", help="Path to report_data JSON")
    p.add_argument("--analysis",  default="", help="Path to analysis JSON")
    p.add_argument("--mock",      action="store_true")
    p.add_argument("--no-email",  action="store_true")
    p.add_argument("--no-sheets", action="store_true")
    return p.parse_args()


# ── Google auth ───────────────────────────────────────────────────────────────

def get_google_services():
    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE, scopes=SCOPES
    )
    return {
        "sheets": build("sheets", "v4", credentials=creds, cache_discovery=False),
    }


# ── HTML helpers ──────────────────────────────────────────────────────────────

def _pct_color(val, good, warn):
    if val >= good:  return "#16a34a"
    if val >= warn:  return "#d97706"
    return "#dc2626"

def _status_dot(s):
    return {"green": "🟢", "yellow": "🟡", "red": "🔴"}.get(s, "⚪")

def _priority_badge(p):
    colors = {"critical": "#dc2626", "high": "#d97706", "medium": "#6b7280"}
    c = colors.get(p, "#6b7280")
    label = p.upper()
    return (f'<span style="background:{c};color:#fff;font-size:10px;font-weight:700;'
            f'padding:2px 7px;border-radius:3px;letter-spacing:.05em">{label}</span>')

def _severity_dot(s):
    return {"critical": "🔴", "high": "🟡", "medium": "⚪"}.get(s, "⚪")

def _action_badge(action):
    colors = {
        "protect": "#2563eb", "fix":   "#dc2626", "test":  "#d97706",
        "monitor": "#6b7280", "scale": "#16a34a",
    }
    c = colors.get(action, "#6b7280")
    return (f'<span style="display:inline-block;background:{c}20;color:{c};font-size:10px;'
            f'font-weight:700;padding:2px 6px;border-radius:3px;border:1px solid {c}40;'
            f'letter-spacing:.05em">{action.upper()}</span>')

def _rate_cell(val, good, warn, suffix="%"):
    color = _pct_color(val, good, warn)
    return (f'<td style="padding:7px 10px;font-size:13px;text-align:right;'
            f'font-weight:600;color:{color}">{val}{suffix}</td>')


# ── Section builders ──────────────────────────────────────────────────────────

def _funnel_scorecard_section(snap: dict, scorecard: dict, variants: list) -> str:
    """
    Scorecard: top 5 flows by landing_page_views + combined total column.
    Horizontally scrollable when content overflows.
    """
    # Top 5 by landing views
    top = sorted(variants, key=lambda v: v.get("landing_page_views", 0), reverse=True)[:5]

    def cell(val, good, warn):
        if val is None:
            return '<td style="padding:8px 10px;font-size:13px;text-align:right;color:#9ca3af;white-space:nowrap">—</td>'
        color = _pct_color(val, good, warn)
        return f'<td style="padding:8px 10px;font-size:13px;text-align:right;font-weight:700;color:{color};white-space:nowrap">{val}%</td>'

    flow_headers = ""
    for v in top:
        lbl = v.get("label", v.get("variant", ""))
        flow_headers += (
            f'<th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;'
            f'font-weight:600;text-transform:uppercase;letter-spacing:.06em;white-space:nowrap">{lbl}</th>'
        )
    flow_headers += (
        '<th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;'
        'font-weight:600;text-transform:uppercase;letter-spacing:.06em;white-space:nowrap">Combined</th>'
    )

    steps = [
        ("Landing → Optin",    "landing_to_optin_rate",    "landing_to_optin_pct",   15, 10),
        ("Offer → Checkout",   "offer_to_checkout_rate",   "offer_to_checkout_pct",  80, 60),
        ("Checkout → Purchase","checkout_to_purchase_rate","checkout_to_sale_pct",   20, 15),
    ]

    rows = ""
    for label, vkey, snap_key, good, warn in steps:
        comb_val   = snap.get(snap_key, 0)
        flow_cells = "".join(cell(v.get(vkey), good, warn) for v in top)
        rows += f"""
        <tr style="border-bottom:1px solid #f0f0f0">
          <td style="padding:8px 10px;font-size:13px;color:#374151;white-space:nowrap">{label}</td>
          {flow_cells}
          {cell(comb_val, good, warn)}
        </tr>"""

    optin_pv = snap.get("optin_page_views", 0)
    landing  = snap.get("landing_page_views", 0)
    if optin_pv > 0 and landing > 0:
        optin_pv_rate = round(optin_pv / landing * 100, 2)
        color   = _pct_color(optin_pv_rate, 40, 25)
        colspan = len(top) + 1
        rows += f"""
        <tr style="border-bottom:1px solid #f0f0f0;background:#f9fafb">
          <td style="padding:8px 10px;font-size:13px;color:#374151;white-space:nowrap">Landing → Optin page view <span style="font-size:10px;color:#9ca3af">(GA4)</span></td>
          <td colspan="{colspan - 1}"></td>
          <td style="padding:8px 10px;font-size:13px;text-align:right;font-weight:700;color:{color};white-space:nowrap">{optin_pv_rate}%</td>
        </tr>"""

    weakest      = scorecard.get("weakest_step", "—").replace("_", " → ")
    weakest_pct  = scorecard.get("weakest_step_rate_pct") or scorecard.get("weakest_step_pct", 0)
    weakest_flow = scorecard.get("weakest_flow", "")
    commentary   = scorecard.get("commentary", "")
    weakest_suffix = f" in {weakest_flow}" if weakest_flow and weakest_flow != "all" else ""

    n_total = len(variants)
    subtitle = f"Top 5 of {n_total} flow{'s' if n_total != 1 else ''} by traffic · combined total included"

    return f"""
  <div style="background:#fff;padding:24px 28px;border-top:3px solid #7c3aed;margin-top:2px">
    <h2 style="margin:0 0 6px;font-size:13px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.08em">Funnel Scorecard</h2>
    <p style="margin:0 0 14px;font-size:12px;color:#9ca3af">{subtitle}</p>
    <div style="overflow-x:auto">
      <table style="min-width:100%;border-collapse:collapse">
        <thead><tr style="background:#f9fafb">
          <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:left;font-weight:600;text-transform:uppercase;letter-spacing:.06em;white-space:nowrap">Step</th>
          {flow_headers}
        </tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    <div style="margin-top:12px;background:#fef2f2;border-radius:6px;padding:10px 14px">
      <p style="margin:0;font-size:12px;color:#dc2626;font-weight:600">
        ⚠ Weakest step: {weakest} ({weakest_pct}%){weakest_suffix}
      </p>
      <p style="margin:6px 0 0;font-size:12px;color:#374151">{commentary}</p>
    </div>
  </div>"""


def _variant_section(variants: list) -> str:
    if not variants:
        return ""

    # Top 5 by landing views
    top = sorted(variants, key=lambda v: v.get("landing_page_views", 0), reverse=True)[:5]
    n_total = len(variants)

    def col(v: dict) -> str:
        l_to_o  = v.get("landing_to_optin_rate", 0) or 0
        o_to_ch = v.get("offer_to_checkout_rate", 0) or 0
        ch_to_p = v.get("checkout_to_purchase_rate", 0) or 0
        purchases  = v.get("purchases", 0) or 0
        checkout   = v.get("checkout_clicks", 0) or 0
        landing    = v.get("landing_page_views", 0) or 0
        optin_comp = v.get("optin_completions", 0) or 0
        offer_pv   = v.get("offer_page_views", 0) or 0
        revenue    = v.get("revenue") or 0

        return f"""
        <td style="padding:16px 18px;font-size:13px;vertical-align:top;border-left:1px solid #f0f0f0;min-width:200px">
          <div style="font-weight:700;font-size:13px;margin-bottom:14px;line-height:1.4;color:#111">{v.get('label','')}</div>
          <table style="width:100%;border-collapse:collapse;font-size:12px">
            <tr><td style="padding:3px 0;color:#6b7280">Landing views</td>
                <td style="text-align:right;font-weight:600">{landing:,}</td></tr>
            <tr><td style="padding:3px 0;color:#6b7280">Optin submits</td>
                <td style="text-align:right;font-weight:600">{optin_comp:,}</td></tr>
            <tr><td style="padding:3px 0;color:#6b7280">Offer page views</td>
                <td style="text-align:right;font-weight:600">{offer_pv:,}</td></tr>
            <tr><td style="padding:3px 0;color:#6b7280">Checkout clicks</td>
                <td style="text-align:right;font-weight:600">{checkout:,}</td></tr>
            <tr><td style="padding:3px 0;color:#6b7280">Purchases</td>
                <td style="text-align:right;font-weight:600">{purchases}</td></tr>
            <tr style="border-top:1px solid #f0f0f0">
                <td style="padding:5px 0 3px;color:#6b7280">Land → Optin</td>
                <td style="text-align:right;font-weight:600;color:{_pct_color(l_to_o,15,10)}">{l_to_o}%</td></tr>
            <tr><td style="padding:3px 0;color:#6b7280">Offer → Checkout</td>
                <td style="text-align:right;font-weight:600;color:{_pct_color(o_to_ch,80,60)}">{o_to_ch}%</td></tr>
            <tr><td style="padding:3px 0;color:#6b7280">Checkout → Purchase</td>
                <td style="text-align:right;font-weight:600;color:{_pct_color(ch_to_p,20,15)}">{ch_to_p}%</td></tr>
            {f'<tr><td style="padding:3px 0;color:#6b7280">Revenue</td><td style="text-align:right;font-weight:600">${revenue:,.2f}</td></tr>' if revenue else ""}
          </table>
          <div style="margin-top:10px">{_action_badge(v.get('action','monitor'))}</div>
        </td>"""

    cols = "".join(col(v) for v in top)
    subtitle = f"Top 5 of {n_total} flow{'s' if n_total != 1 else ''} by traffic · CPV Labs landing-stats"

    return f"""
  <div style="background:#fff;padding:24px 28px;border-bottom:1px solid #f0f0f0">
    <h2 style="margin:0 0 6px;font-size:13px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.08em">Funnel Flows</h2>
    <p style="margin:0 0 14px;font-size:12px;color:#9ca3af">{subtitle}</p>
    <div style="overflow-x:auto">
      <table style="border-collapse:collapse;table-layout:fixed">
        <tbody><tr>{cols}</tr></tbody>
      </table>
    </div>
  </div>"""


def _funnel_leaks_section(leaks: list) -> str:
    if not leaks:
        return ""
    cards = ""
    for leak in leaks:
        dot   = _severity_dot(leak.get("severity", ""))
        color = {"critical": "#dc2626", "high": "#d97706", "medium": "#6b7280"}.get(
            leak.get("severity", ""), "#6b7280")
        cards += f"""
        <div style="border-left:4px solid {color};background:#fff;border-radius:0 6px 6px 0;
                    padding:12px 16px;margin-bottom:10px;border:1px solid #e5e7eb;border-left:4px solid {color}">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">
            <span style="font-size:13px">{dot}</span>
            <span style="font-weight:700;font-size:13px;color:#111">{leak.get('step','')}</span>
            <span style="font-size:13px;font-weight:700;color:{color};margin-left:auto">{leak.get('rate_pct',0)}%</span>
          </div>
          <p style="margin:0;font-size:12px;color:#6b7280">{leak.get('why_it_matters','')}</p>
          <p style="margin:4px 0 0;font-size:11px;color:#9ca3af">{leak.get('evidence_type','').capitalize()}</p>
        </div>"""

    return f"""
  <div style="background:#fff;padding:24px 28px;border-bottom:1px solid #f0f0f0">
    <h2 style="margin:0 0 14px;font-size:13px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.08em">Funnel Leaks</h2>
    {cards}
  </div>"""


def _upsell_section(upsell: dict) -> str:
    if not upsell:
        return ""

    def oto_row(label, sales, take_pct, good=40, warn=25):
        if sales is None:
            return ""
        color = _pct_color(take_pct or 0, good, warn)
        return f"""
        <tr style="border-bottom:1px solid #f0f0f0">
          <td style="padding:8px 10px;font-size:13px">{label}</td>
          <td style="padding:8px 10px;font-size:13px;text-align:right">{sales}</td>
          <td style="padding:8px 10px;font-size:13px;text-align:right;font-weight:600;color:{color}">
            {f"{take_pct}%" if take_pct is not None else "—"}
          </td>
        </tr>"""

    fe    = upsell.get("frontend_sales", 0)
    rows  = oto_row("OTO1 Blueprint",           upsell.get("oto1_sales"),         upsell.get("oto1_take_rate_pct"))
    rows += oto_row("OTO1 Downsell",            upsell.get("oto1_downsell_sales"), None)
    rows += oto_row("OTO2 Timeline",            upsell.get("oto2_sales"),         upsell.get("oto2_take_rate_pct"), good=25, warn=15)
    rows += oto_row("OTO2 Downsell",            upsell.get("oto2_downsell_sales"), None)

    return f"""
  <div style="background:#fff;padding:24px 28px;border-bottom:1px solid #f0f0f0">
    <h2 style="margin:0 0 12px;font-size:13px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.08em">Backend Monetisation (ClickBank)</h2>
    <table style="width:100%;border-collapse:collapse">
      <thead><tr style="background:#f9fafb">
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:left;font-weight:600;text-transform:uppercase;letter-spacing:.06em">Stage</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em">Sales</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em">Take Rate</th>
      </tr></thead>
      <tbody>
        <tr style="border-bottom:1px solid #f0f0f0">
          <td style="padding:8px 10px;font-size:13px;font-weight:600">Front-end</td>
          <td style="padding:8px 10px;font-size:13px;text-align:right;font-weight:700">{fe}</td>
          <td style="padding:8px 10px;font-size:13px;text-align:right;color:#6b7280">—</td>
        </tr>
        {rows}
        <tr style="background:#f9fafb;font-weight:700">
          <td style="padding:8px 10px;font-size:13px">Total Revenue</td>
          <td style="padding:8px 10px;font-size:13px;text-align:right" colspan="2">${upsell.get('total_backend_revenue',0):,.2f}</td>
        </tr>
      </tbody>
    </table>
    <p style="margin:10px 0 0;font-size:12px;color:#6b7280">Avg revenue per buyer: <strong>${upsell.get('avg_revenue_per_buyer',0):,.2f}</strong></p>
    <p style="margin:6px 0 0;font-size:12px;color:#374151">{upsell.get('commentary','')}</p>
  </div>"""


def _needle_movers_section(needle_movers: list) -> str:
    if not needle_movers:
        return ""
    blocks = ""
    for nm in needle_movers:
        steps = "".join(
            f'<li style="margin:4px 0;font-size:13px;color:#374151">{s}</li>'
            for s in nm.get("what", [])
        )
        border_color = {"critical": "#dc2626", "high": "#d97706", "medium": "#6b7280"}.get(
            nm.get("priority", ""), "#6b7280")
        blocks += f"""
        <div style="border:1px solid #e5e7eb;border-radius:8px;padding:16px 20px;
                    margin-bottom:12px;border-left:4px solid {border_color}">
          <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
            {_priority_badge(nm.get('priority','medium'))}
            <span style="font-weight:700;font-size:14px;color:#111">#{nm['rank']} — {nm['area']}</span>
            <span style="margin-left:auto;font-size:11px;color:#9ca3af">{nm.get('evidence_type','')}</span>
          </div>
          <p style="margin:0 0 10px;font-size:13px;color:#374151;line-height:1.5">{nm.get('why','')}</p>
          <ul style="margin:0 0 10px;padding-left:18px">{steps}</ul>
          <div style="background:#f9fafb;border-radius:4px;padding:8px 12px;font-size:12px;color:#6b7280">
            <strong style="color:#374151">Revenue impact:</strong> {nm.get('revenue_impact','')}
          </div>
        </div>"""

    return f"""
  <div style="background:#fff;padding:24px 28px;border-bottom:1px solid #f0f0f0">
    <h2 style="margin:0 0 16px;font-size:13px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.08em">Needle Movers — Priority Order</h2>
    {blocks}
  </div>"""


def _cpv_backend_section(cpv: dict) -> str:
    camps = cpv.get("campaigns", {})
    if not camps:
        return ""

    sorted_camps = sorted(camps.items(), key=lambda x: -x[1].get("views", 0))
    top5         = sorted_camps[:5]
    n_total      = len(sorted_camps)

    rows = ""
    for cid, c in top5:
        checkout = c.get("checkout_clicks", 0)
        views    = c.get("views", 0)
        co_rate  = round(checkout / views * 100, 2) if views else 0
        dot      = {"strong": "🟢", "active": "🟢", "weak": "🟡", "dead": "🔴"}.get(
                       c.get("status", ""), "⚪")
        rows += f"""
        <tr style="border-bottom:1px solid #f0f0f0">
          <td style="padding:8px 10px;font-size:13px;white-space:nowrap">{dot} #{cid} — {c.get('label','')}</td>
          <td style="padding:8px 10px;font-size:13px;text-align:right;white-space:nowrap">{views:,}</td>
          <td style="padding:8px 10px;font-size:13px;text-align:right;white-space:nowrap">{c.get('conversions',0)}</td>
          <td style="padding:8px 10px;font-size:13px;text-align:right;white-space:nowrap">{checkout}</td>
          <td style="padding:8px 10px;font-size:13px;text-align:right;color:{_pct_color(co_rate,10,5)};font-weight:600;white-space:nowrap">{co_rate}%</td>
          <td style="padding:8px 10px;font-size:13px;text-align:right;white-space:nowrap">S${c.get('revenue',0):,.2f}</td>
        </tr>"""

    total_checkout = cpv.get("checkout_clicks_total", 0)
    totals         = cpv.get("totals_by_source", {})
    total_views    = sum(t.get("views", 0) for t in totals.values())
    total_convs    = sum(t.get("conversions", 0) for t in totals.values())
    total_rev      = sum(t.get("revenue", 0.0) for t in totals.values())
    subtitle       = f"Top 5 of {n_total} campaign{'s' if n_total != 1 else ''} by views" if n_total > 5 else f"{n_total} campaign{'s' if n_total != 1 else ''}"

    return f"""
  <div style="background:#fff;padding:24px 28px;border-bottom:1px solid #f0f0f0">
    <h2 style="margin:0 0 4px;font-size:13px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.08em">CPV Labs — Campaign Tracking</h2>
    <p style="margin:0 0 12px;font-size:12px;color:#9ca3af">{subtitle}</p>
    <div style="overflow-x:auto">
      <table style="min-width:100%;border-collapse:collapse">
        <thead><tr style="background:#f9fafb">
          <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:left;font-weight:600;text-transform:uppercase;letter-spacing:.06em;white-space:nowrap">Campaign</th>
          <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em;white-space:nowrap">Views</th>
          <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em;white-space:nowrap">Conv</th>
          <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em;white-space:nowrap">Checkout Clicks</th>
          <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em;white-space:nowrap">View→Checkout%</th>
          <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em;white-space:nowrap">Revenue (SGD)</th>
        </tr></thead>
        <tbody>{rows}</tbody>
        <tfoot><tr style="background:#f9fafb;font-weight:700">
          <td style="padding:8px 10px;font-size:13px;white-space:nowrap">All campaigns total</td>
          <td style="padding:8px 10px;font-size:13px;text-align:right;white-space:nowrap">{total_views:,}</td>
          <td style="padding:8px 10px;font-size:13px;text-align:right;white-space:nowrap">{total_convs}</td>
          <td style="padding:8px 10px;font-size:13px;text-align:right;white-space:nowrap">{total_checkout}</td>
          <td></td>
          <td style="padding:8px 10px;font-size:13px;text-align:right;white-space:nowrap">S${total_rev:,.2f}</td>
        </tfoot>
      </table>
    </div>
  </div>"""


# ── Week-over-week comparison ─────────────────────────────────────────────────

def _load_prev_data(current_start: str) -> dict | None:
    """
    Find the most recent report_data_*.json whose period_end < current_start.
    When multiple files share the same period_end, prefer the most recently
    written one (mtime) — avoids picking stale/legacy files with old key formats.
    """
    import glob as _glob, os
    files = sorted(
        _glob.glob("output/astroloversketch/report_data_*.json"),
        key=lambda p: os.path.getmtime(p),
    )
    for path in reversed(files):
        try:
            with open(path) as f:
                d = json.load(f)
            if d.get("meta", {}).get("period_end", "") < current_start:
                return d
        except Exception:
            continue
    return None


def _delta_badge(current, previous, higher_is_good=True, is_pct=False):
    """Return a coloured delta badge string like ▲ +3.5pp or ▼ -12%."""
    if previous is None or previous == 0:
        return '<span style="color:#9ca3af;font-size:11px">—</span>'
    if is_pct:
        diff = round(current - previous, 2)
        label = f"{'+' if diff >= 0 else ''}{diff}pp"
    else:
        diff_pct = round((current - previous) / abs(previous) * 100, 1)
        label = f"{'+' if diff_pct >= 0 else ''}{diff_pct}%"
        diff = diff_pct

    good = (diff >= 0) == higher_is_good
    color = "#16a34a" if good else "#dc2626"
    arrow = "▲" if diff >= 0 else "▼"
    return f'<span style="color:{color};font-size:11px;font-weight:700">{arrow} {label}</span>'


def _wow_section(snap: dict, cpv_totals: dict, prev_data: dict, period: str) -> str:
    if not prev_data:
        return ""

    prev_snap  = prev_data.get("funnel_snapshot", {})
    prev_ft    = prev_data.get("cpv_tracking", {}).get("funnel_totals", {})
    prev_meta  = prev_data.get("meta", {})
    prev_label = f"{prev_meta.get('period_start','')} → {prev_meta.get('period_end','')}"

    # Current period days vs previous period days (for partial-week note)
    try:
        from datetime import date
        cur_start, cur_end = period.split(" → ")
        cur_days  = (date.fromisoformat(cur_end) - date.fromisoformat(cur_start)).days + 1
        prev_days = (date.fromisoformat(prev_meta.get("period_end","")) -
                     date.fromisoformat(prev_meta.get("period_start",""))).days + 1
    except Exception:
        cur_days = prev_days = 7

    partial_note = (
        f'<p style="margin:0 0 14px;font-size:12px;color:#d97706">⚠ Current period is {cur_days} days vs {prev_days} days — volume metrics not directly comparable.</p>'
        if cur_days < prev_days else
        '<p style="margin:0 0 14px;font-size:12px;color:#9ca3af">Same-length periods — direct comparison valid.</p>'
    )

    def row(label, curr, prev, higher_is_good=True, is_pct=False, fmt_curr=None, fmt_prev=None):
        fc = fmt_curr or (f"{curr}%" if is_pct else f"{curr:,}" if isinstance(curr, int) else f"{curr}")
        fp = fmt_prev or (f"{prev}%" if is_pct else f"{prev:,}" if isinstance(prev, int) else f"{prev}")
        badge = _delta_badge(curr, prev, higher_is_good, is_pct)
        return f"""
        <tr style="border-bottom:1px solid #f0f0f0">
          <td style="padding:7px 10px;font-size:13px;color:#374151">{label}</td>
          <td style="padding:7px 10px;font-size:13px;text-align:right;color:#6b7280">{fp}</td>
          <td style="padding:7px 10px;font-size:13px;text-align:right;font-weight:600">{fc}</td>
          <td style="padding:7px 10px;text-align:right">{badge}</td>
        </tr>"""

    # Per-flow rows: top 5 by current-period landing views, then any prev-only flows
    sorted_curr = sorted(cpv_totals.keys(), key=lambda k: -cpv_totals[k].get("landing_page_views", 0))
    # Only include prev-only flows that use the new URL-path format (contain →)
    prev_only   = [k for k in prev_ft if k not in cpv_totals and "→" in k]
    all_flow_keys = sorted_curr[:5] + prev_only
    variant_rows = ""
    for fkey in all_flow_keys:
        curr_v = cpv_totals.get(fkey, {})
        prev_v = prev_ft.get(fkey, {})
        if not curr_v and not prev_v:
            continue
        # Use flow_label from current period, fall back to key name
        flow_label = curr_v.get("flow_label") or prev_v.get("flow_label") or fkey
        variant_rows += f"""
        <tr style="background:#f9fafb">
          <td colspan="4" style="padding:6px 10px;font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.06em">{flow_label}</td>
        </tr>"""
        variant_rows += row("  Landing views",      curr_v.get("landing_page_views",0),  prev_v.get("landing_page_views",0))
        variant_rows += row("  Optin submits",      curr_v.get("optin_completions",0),    prev_v.get("optin_completions",0))
        variant_rows += row("  Checkout clicks",    curr_v.get("checkout_clicks",0),      prev_v.get("checkout_clicks",0))
        variant_rows += row("  Purchases",          curr_v.get("purchases",0),            prev_v.get("purchases",0))
        variant_rows += row("  Land→Optin%",        curr_v.get("landing_to_optin_rate",0), prev_v.get("landing_to_optin_rate",0), is_pct=True)
        variant_rows += row("  Offer→Checkout%",    curr_v.get("offer_to_checkout_rate",0), prev_v.get("offer_to_checkout_rate",0), is_pct=True)
        variant_rows += row("  Checkout→Purchase%", curr_v.get("checkout_to_purchase_rate",0), prev_v.get("checkout_to_purchase_rate",0), is_pct=True)

    # Overall rows
    overall_rows = f"""
        <tr style="background:#f9fafb">
          <td colspan="4" style="padding:6px 10px;font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.06em">Overall</td>
        </tr>"""
    overall_rows += row("  FE Sales",       snap.get("frontend_sales",0),          prev_snap.get("frontend_sales",0))
    overall_rows += row("  Revenue (USD)",  snap.get("total_revenue_usd",0),        prev_snap.get("total_revenue_usd",0),
                        fmt_curr=f"${snap.get('total_revenue_usd',0):,.2f}",
                        fmt_prev=f"${prev_snap.get('total_revenue_usd',0):,.2f}")
    overall_rows += row("  Avg/Buyer",      snap.get("avg_revenue_per_buyer_usd",0), prev_snap.get("avg_revenue_per_buyer_usd",0),
                        fmt_curr=f"${snap.get('avg_revenue_per_buyer_usd',0):,.2f}",
                        fmt_prev=f"${prev_snap.get('avg_revenue_per_buyer_usd',0):,.2f}")

    return f"""
  <div style="background:#fff;padding:24px 28px;border-bottom:1px solid #f0f0f0">
    <h2 style="margin:0 0 6px;font-size:13px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.08em">Week-over-Week Comparison</h2>
    {partial_note}
    <table style="width:100%;border-collapse:collapse">
      <thead><tr style="background:#f9fafb">
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:left;font-weight:600;text-transform:uppercase;letter-spacing:.06em">Metric</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em">{prev_label}</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em">{period}</th>
        <th style="padding:8px 10px;font-size:11px;color:#6b7280;text-align:right;font-weight:600;text-transform:uppercase;letter-spacing:.06em">Change</th>
      </tr></thead>
      <tbody>{overall_rows}{variant_rows}</tbody>
    </table>
  </div>"""


# ── Main email builder ────────────────────────────────────────────────────────

def build_email_html(data: dict, analysis: dict) -> str:
    snap       = data.get("funnel_snapshot", {})
    cpv        = data.get("cpv_tracking", {})
    cpv_totals = cpv.get("funnel_totals", {})
    backend    = data.get("funnel_backend", {})
    cross      = data.get("cross_check", {})
    a          = analysis.get("analysis", {})
    period     = f"{data['meta']['period_start']} → {data['meta']['period_end']}"

    # Load previous period data for WoW comparison (from saved history)
    prev_data = _load_prev_data(data["meta"]["period_start"])

    scorecard   = a.get("funnel_scorecard", {})
    variants    = a.get("variant_comparison", [])
    leaks       = a.get("funnel_leaks", [])
    upsell      = a.get("upsell_analysis", {})
    needle_mvrs = a.get("needle_movers", [])
    discrepancy = a.get("cpv_cb_discrepancy", {})
    data_notes  = a.get("data_notes", "")

    cross_color = {"ok": "#16a34a", "warn": "#d97706", "alert": "#dc2626"}.get(
        cross.get("status", "ok"), "#6b7280")
    cross_block = (
        f'<p style="font-size:12px;color:{cross_color};margin:8px 0 0">'
        f'⚠ Data check: {cross.get("note","")}</p>'
    ) if cross.get("status") != "ok" else ""

    disc_color  = {"ok": "#16a34a", "warn": "#d97706", "alert": "#dc2626"}.get(
        discrepancy.get("status", "ok"), "#6b7280")

    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{period} — Astro Lover Sketch Funnel Report</title>
</head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif">
<div style="max-width:700px;margin:0 auto;padding:24px 16px">

  <!-- Header -->
  <div style="background:#111;border-radius:10px 10px 0 0;padding:24px 28px">
    <p style="margin:0 0 4px;font-size:11px;color:#9ca3af;letter-spacing:.1em;text-transform:uppercase">Astro Lover Sketch · Funnel Performance</p>
    <h1 style="margin:0 0 4px;font-size:22px;color:#fff;font-weight:700">Weekly Report</h1>
    <p style="margin:0;font-size:13px;color:#6b7280">{period}</p>
  </div>

  <!-- Snapshot -->
  <div style="background:#fff;padding:24px 28px;border-bottom:1px solid #f0f0f0">
    <h2 style="margin:0 0 16px;font-size:13px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.08em">Funnel Snapshot</h2>
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:16px">
      <div>
        <p style="margin:0 0 4px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em">Revenue</p>
        <p style="margin:0;font-size:22px;font-weight:700;color:#111">${snap.get('total_revenue_usd',0):,.2f}</p>
      </div>
      <div>
        <p style="margin:0 0 4px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em">FE Sales</p>
        <p style="margin:0;font-size:22px;font-weight:700;color:#111">{snap.get('frontend_sales',0)}</p>
      </div>
      <div>
        <p style="margin:0 0 4px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em">Avg/Buyer</p>
        <p style="margin:0;font-size:22px;font-weight:700;color:#111">${snap.get('avg_revenue_per_buyer_usd',0):,.2f}</p>
      </div>
      <div>
        <p style="margin:0 0 4px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em">Checkout Clicks</p>
        <p style="margin:0;font-size:22px;font-weight:700;color:#111">{snap.get('checkout_clicks',0)}</p>
      </div>
    </div>
    <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-top:16px;padding-top:16px;border-top:1px solid #f0f0f0">
      <div>
        <p style="margin:0 0 4px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em">Landing Views</p>
        <p style="margin:0;font-size:18px;font-weight:600;color:#374151">{snap.get('landing_page_views',0):,}</p>
      </div>
      <div>
        <p style="margin:0 0 4px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em">Optin Submits</p>
        <p style="margin:0;font-size:18px;font-weight:600;color:#374151">{snap.get('optin_completions',0):,}</p>
      </div>
      <div>
        <p style="margin:0 0 4px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em">Offer Page Views</p>
        <p style="margin:0;font-size:18px;font-weight:600;color:#374151">{snap.get('offer_page_views',0):,}</p>
      </div>
    </div>
    {cross_block}
  </div>

  <!-- Period summary -->
  <div style="background:#fff;padding:20px 28px;border-bottom:1px solid #f0f0f0">
    <p style="margin:0;font-size:14px;color:#374151;line-height:1.6">{a.get('period_summary','')}</p>
  </div>

  <!-- Funnel scorecard -->
  {_funnel_scorecard_section(snap, scorecard, variants)}

  <!-- Variant comparison -->
  {_variant_section(variants)}

  <!-- Funnel leaks -->
  {_funnel_leaks_section(leaks)}

  <!-- Needle movers -->
  {_needle_movers_section(needle_mvrs)}

  <!-- CPV Labs campaigns -->
  {_cpv_backend_section(cpv)}

  <!-- Backend monetisation -->
  {_upsell_section(upsell)}

  <!-- Week-over-week comparison -->
  {_wow_section(snap, cpv_totals, prev_data, period)}

  <!-- CPV vs ClickBank discrepancy -->
  <div style="background:#fff;padding:16px 28px;border-bottom:1px solid #f0f0f0">
    <h2 style="margin:0 0 8px;font-size:13px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.08em">CPV Labs vs ClickBank</h2>
    <p style="margin:0;font-size:13px;color:{disc_color};font-weight:600">
      {discrepancy.get('status','—').upper()} — {discrepancy.get('note','')}
    </p>
    <p style="margin:4px 0 0;font-size:12px;color:#6b7280">
      CPV conversions: {discrepancy.get('cpv_purchases', discrepancy.get('cpv_conversions','—'))} &nbsp;·&nbsp;
      CB frontend sales: {discrepancy.get('cb_frontend_sales','—')} &nbsp;·&nbsp;
      CPV revenue: SGD {cross.get('cpv_revenue_sgd', cross.get('cpv_revenue','—'))} ≈ USD {cross.get('cpv_revenue_usd','—')}
    </p>
    <p style="margin:4px 0 0;font-size:11px;color:#9ca3af">{cross.get('currency_note','CPV revenue in SGD, CB revenue in USD (net vendor).')}</p>
  </div>

  <!-- Footer -->
  <div style="background:#fff;border-radius:0 0 10px 10px;padding:20px 28px;text-align:center">
    <p style="margin:0;font-size:11px;color:#9ca3af">
      Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC · Astro Lover Sketch Funnel Reporter
    </p>
    {"<p style='margin:4px 0 0;font-size:11px;color:#9ca3af'>Data notes: " + data_notes + "</p>" if data_notes else ""}
  </div>

</div>
</body>
</html>"""


# ── Google Sheets ─────────────────────────────────────────────────────────────

SHEET_HEADERS = [
    "Week Start", "Week End",
    "Revenue USD", "FE Sales", "Avg/Buyer USD",
    "Landing Views", "Optin Submits", "Offer Views", "Checkout Clicks",
    "Land→Optin%", "Offer→Checkout%", "Checkout→Sale%", "Overall Rate%",
    "OTO1 Take%", "OTO2 Take%",
    "CPV Purchases", "CB FE Sales", "Revenue Diff USD",
    "Weakest Step",
]

def _ensure_headers(services):
    sheet = services["sheets"].spreadsheets()
    meta  = sheet.get(spreadsheetId=SHEET_ID).execute()
    existing = [s["properties"]["title"] for s in meta["sheets"]]
    if SHEET_TAB not in existing:
        sheet.batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": SHEET_TAB}}}]}
        ).execute()
    result = sheet.values().get(spreadsheetId=SHEET_ID, range=f"{SHEET_TAB}!A1:A1").execute()
    if not result.get("values"):
        sheet.values().update(
            spreadsheetId=SHEET_ID, range=f"{SHEET_TAB}!A1",
            valueInputOption="RAW", body={"values": [SHEET_HEADERS]}
        ).execute()


def append_sheets_row(services, data: dict, analysis: dict):
    snap  = data.get("funnel_snapshot", {})
    a     = analysis.get("analysis", {})
    sc    = a.get("funnel_scorecard", {})
    ua    = a.get("upsell_analysis", {})
    cross = data.get("cross_check", {})

    row = [
        data["meta"]["period_start"],
        data["meta"]["period_end"],
        snap.get("total_revenue_usd", 0),
        snap.get("frontend_sales", 0),
        snap.get("avg_revenue_per_buyer_usd", 0),
        snap.get("landing_page_views", 0),
        snap.get("optin_completions", 0),
        snap.get("offer_page_views", 0),
        snap.get("checkout_clicks", 0),
        snap.get("landing_to_optin_pct", 0),
        snap.get("offer_to_checkout_pct", 0),
        snap.get("checkout_to_sale_pct", 0),
        snap.get("overall_rate_pct", 0),
        ua.get("oto1_take_rate_pct", 0),
        ua.get("oto2_take_rate_pct", 0) or 0,
        a.get("cpv_cb_discrepancy", {}).get("cpv_purchases", 0),
        a.get("cpv_cb_discrepancy", {}).get("cb_frontend_sales", 0),
        cross.get("revenue_diff", 0) if cross else 0,
        sc.get("weakest_step", ""),
    ]

    _ensure_headers(services)
    services["sheets"].spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_TAB}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()
    print(f"[report_astro] ✓ Row appended to Google Sheets")


# ── Send email ────────────────────────────────────────────────────────────────

def send_email(html: str, subject: str):
    sendgrid_key = os.environ.get("SENDGRID_API_KEY", "")
    if not sendgrid_key:
        print("[report_astro] ⚠ SENDGRID_API_KEY not set — skipping send")
        return
    resp = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={"Authorization": f"Bearer {sendgrid_key}", "Content-Type": "application/json"},
        json={
            "personalizations": [{"to": [{"email": EMAIL_TO}]}],
            "from":    {"email": EMAIL_FROM, "name": EMAIL_NAME},
            "subject": subject,
            "content": [{"type": "text/html", "value": html}],
        },
        timeout=30,
    )
    if resp.status_code == 202:
        print(f"[report_astro] ✓ Email sent to {EMAIL_TO}")
    else:
        raise Exception(f"SendGrid {resp.status_code}: {resp.text[:200]}")


# ── Index ─────────────────────────────────────────────────────────────────────

def build_index_html(output_dir: str):
    files = sorted(glob.glob(f"{output_dir}/email_*.html"), reverse=True)
    if not files:
        return
    rows = ""
    for i, filepath in enumerate(files):
        filename  = Path(filepath).name
        date_part = filename.replace("email_", "").replace(".html", "")
        try:
            from datetime import datetime as dt
            label = dt.strptime(date_part, "%Y_%m_%d").strftime("%B %d, %Y")
        except Exception:
            label = date_part.replace("_", "-")
        is_latest = i == 0
        badge = ('<span style="background:#16a34a;color:#fff;font-size:10px;font-weight:700;'
                 'padding:2px 8px;border-radius:3px;margin-left:8px;letter-spacing:.05em">LATEST</span>'
                 ) if is_latest else ""
        rows += f"""
        <tr style="border-bottom:1px solid #f0f0f0">
          <td style="padding:12px 16px;background:{'#f0fdf4' if is_latest else '#fff'}">
            <a href="{filename}" style="font-size:15px;font-weight:{'700' if is_latest else '400'};color:#111;text-decoration:none">{label}</a>
            {badge}
          </td>
          <td style="padding:12px 16px;text-align:right;background:{'#f0fdf4' if is_latest else '#fff'}">
            <a href="{filename}" style="font-size:13px;color:#2563eb;text-decoration:none">View →</a>
          </td>
        </tr>"""

    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
    <title>Astro Lover Sketch — Weekly Reports</title></head>
    <body style="margin:0;padding:0;background:#f3f4f6;font-family:-apple-system,sans-serif">
    <div style="max-width:600px;margin:60px auto;padding:0 16px">
      <div style="background:#111;border-radius:10px 10px 0 0;padding:24px 28px">
        <p style="color:#9ca3af;font-size:11px;text-transform:uppercase;letter-spacing:.1em;margin:0 0 4px">Astro Lover Sketch · Funnel Performance</p>
        <h1 style="color:#fff;font-size:22px;font-weight:700;margin:0">Weekly Reports</h1>
      </div>
      <div style="background:#fff;border-radius:0 0 10px 10px;overflow:hidden">
        <table style="width:100%;border-collapse:collapse">{rows}</table>
      </div>
      <p style="text-align:center;font-size:11px;color:#9ca3af;padding:20px 0">{len(files)} report{'s' if len(files)!=1 else ''} · Auto-generated each Monday</p>
    </div></body></html>"""

    index_path = Path(output_dir) / "index.html"
    index_path.write_text(html)
    print(f"[report_astro] ✓ Index updated → {index_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    if not args.data:
        files     = sorted(glob.glob("output/astroloversketch/report_data_*.json"))
        args.data = files[-1] if files else "output/astroloversketch/report_data.json"
        print(f"[report_astro] Auto-detected data: {args.data}")

    if not args.analysis:
        files         = sorted(glob.glob("output/astroloversketch/analysis_*.json"))
        args.analysis = files[-1] if files else "output/astroloversketch/analysis.json"
        print(f"[report_astro] Auto-detected analysis: {args.analysis}")

    with open(args.data) as f:
        data = json.load(f)
    with open(args.analysis) as f:
        analysis = json.load(f)

    period    = f"{data['meta']['period_start']} → {data['meta']['period_end']}"
    subject   = f"Astro Lover Sketch — Funnel Report {period}"
    date_part = Path(args.data).stem.replace("report_data_", "")
    out_dir   = "docs/astroloversketch"
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    email_path = f"{out_dir}/email_{date_part}.html"

    html = build_email_html(data, analysis)
    Path(email_path).write_text(html)
    print(f"[report_astro] ✓ Email HTML saved to {email_path}")

    if args.mock:
        print("[report_astro] Mock mode — skipping Google API calls and email send")
        build_index_html(out_dir)
        return

    if not args.no_sheets:
        if not SHEET_ID:
            print("[report_astro] ⚠ ASTRO_SHEET_ID not set — skipping Sheets")
        else:
            print("[report_astro] Appending to Google Sheets...")
            services = get_google_services()
            append_sheets_row(services, data, analysis)

    if not args.no_email:
        if not EMAIL_TO or not EMAIL_FROM:
            print("[report_astro] ⚠ ASTRO_REPORT_EMAIL_TO or FROM not set — skipping send")
        else:
            print("[report_astro] Sending email...")
            send_email(html, subject)

    build_index_html(out_dir)
    print(f"\n[report_astro] Done ✓")


if __name__ == "__main__":
    main()