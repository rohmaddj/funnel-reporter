"""
AstroLover Sketch — AI Analysis Layer
Sends collected funnel data to Claude and returns structured funnel insights.

Usage:
    python analyse_astro.py                        # reads latest report_data
    python analyse_astro.py --input output/astroloversketch/report_data_2026_04_06.json
    python analyse_astro.py --mock                 # uses mock JSON, skips API call

Output: output/astroloversketch/analysis_YYYY_MM_DD.json

Funnel data source:
  CPV Labs landing-stats is now the primary source for funnel page metrics.
  GA4 is retained in the payload for future use but not used in analysis prompts.
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
load_dotenv()

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL      = "claude-sonnet-4-6"
MAX_TOKENS = 8192


def parse_args():
    p = argparse.ArgumentParser(description="Run AI funnel analysis for AstroLover Sketch")
    p.add_argument("--input", default="", help="Path to report_data JSON")
    p.add_argument("--out",   default="", help="Output path for analysis JSON")
    p.add_argument("--mock",  action="store_true", help="Skip API call, return mock analysis")
    return p.parse_args()


# ── System prompt ─────────────────────────────────────────────────────────────

def build_system_prompt() -> str:
    return """You are a funnel conversion analyst for Astro Lover Sketch (astroloversketch.com),
a text sales letter (TSL) funnel selling astrology-themed digital products via ClickBank.

Your job is to identify where the funnel is leaking, which step needs the most urgent
attention, and what specific actions are most likely to improve conversion.

Funnel structure (5 steps):
  Landing page → Opt-in page (form shown) → Form submit → Offer/Sales page → ClickBank checkout → Purchase
  After purchase: OTO1 (Blueprint $67) → OTO1 Downsell ($47) → OTO2 (Timeline $47) → OTO2 Downsell ($37)

Funnel flows are detected dynamically from actual CPV Labs URL tracking.
Each key in funnel_totals is a flow identifier derived from URL paths
(e.g. "c→offer_v4", "a_b→offer_v2", "c→offer_v4_v2").
Each flow entry includes:
  - flow_label: human-readable path string, e.g. "/c/ → /offer/v4/"
  - landing_paths: list of landing page paths in this flow
  - offer_path: the offer/sales page path
Use flow_label as the display label. There may be 1, 2, or more flows depending
on how many landing/offer combinations are currently running (including split tests).

Data source responsibilities:
  funnel_totals    → CPV Labs landing-stats aggregated per variant. PRIMARY source.
                     landing_page_views    = Views on landing page URL rows
                     optin_completions     = Subscribers on landing page rows (= form submissions)
                     offer_page_views      = Views on offer page URL rows (≈ optin_completions)
                     checkout_clicks       = Clicks on offer page rows (= ClickBank CTA clicked)
                     purchases             = Conversion on offer page rows (≈ ClickBank FE sales)
                     Rates are pre-computed — use them directly.
  funnel_variants  → GA4 supplement. optin_page_views = view_opt_in_page event (the form PAGE
                     before submission). When > 0, use to compute optin_page_to_submit_pct.
                     landing_to_optin_page_pct = how many landing visitors saw the opt-in form.
                     has_custom_events = false means GA4 events not yet active for this period.
  cpv_campaigns    → CPV Labs per-campaign summary (views, conversions, checkout clicks, revenue).
                     IMPORTANT: CPV Labs revenue is always in SGD (Singapore dollar) — the account
                     currency. Do NOT compare CPV revenue directly to ClickBank USD revenue.
  funnel_backend   → ClickBank: actual sales, revenue (USD net after ClickBank fees), upsell/downsell take rates.
  cross_check      → CPV purchases vs ClickBank frontend sales reconciliation.
                     cross_check.cpv_revenue_sgd  = CPV revenue in SGD
                     cross_check.cpv_revenue_usd  = approximate USD equivalent (SGD × sgd_to_usd_rate)
                     cross_check.cb_revenue       = ClickBank net revenue in USD

CRITICAL metric rules:
  - Use funnel_totals as the sole source for funnel step volumes and rates.
  - GA4 optin_page_views is a bonus metric — use it only when has_custom_events=true.
  - frontend_sales from ClickBank is the revenue source of truth.
  - Do not mix CPV view counts with ClickBank sales in the same rate.
  - optin_completions ≈ offer_page_views (same people, small rounding diff is normal).

CRITICAL rate benchmarks:
  - landing_to_optin_rate (CPV SR%):  < 10% needs attention, 15-25% is healthy for TSL
  - offer_to_checkout_rate:           < 60% is low, 80%+ is strong
  - checkout_to_purchase_rate:        < 15% is critical, 20-30% is target
  - OTO1 take rate:                   40%+ strong, 25-39% average, < 25% weak
  - OTO2 take rate:                   25%+ strong, 15-24% average, < 15% weak
  - optin_page_to_submit_pct (GA4):   < 30% is low for a focused opt-in page

CRITICAL reasoning rules:
  - Classify findings as: observed | inferred | hypothesis
  - observed   = directly shown by the data
  - inferred   = reasonable conclusion from multiple signals
  - hypothesis = plausible but not proven from this dataset
  - For small sample sizes (< 20 checkout clicks or < 5 sales), flag uncertainty.
  - v4 and v2 are separate traffic sources — compare directionally, not as a split test.
  - If funnel_totals is empty or all zeros, state that CPV data is unavailable.

CRITICAL output rules:
  - Output valid JSON only.
  - No markdown, no prose outside JSON, no comments, no trailing commas.
"""


# ── User prompt ───────────────────────────────────────────────────────────────

def build_user_prompt(data: dict) -> str:
    funnel_totals   = data.get("cpv_tracking", {}).get("funnel_totals", {})
    cpv_campaigns   = data.get("cpv_tracking", {}).get("campaigns", {})
    funnel_variants = data.get("funnel_variants", {})   # GA4 supplement
    backend         = data.get("funnel_backend", {})
    cross           = data.get("cross_check", {})
    meta            = data.get("meta", {})

    # Slim GA4 supplement — only include if custom events fired this period
    ga4_supplement = {}
    for vkey, vd in funnel_variants.items():
        if isinstance(vd, dict) and vd.get("has_custom_events"):
            ga4_supplement[vkey] = {
                "optin_page_views":          vd.get("optin_page_views", 0),
                "lead_submit_count":         vd.get("lead_submit_count", 0),
                "landing_to_optin_page_pct": vd.get("landing_to_optin_page_pct", 0),
                "optin_page_to_submit_pct":  vd.get("optin_page_to_submit_pct", 0),
            }

    slim = {
        "period":          f"{meta.get('period_start')} to {meta.get('period_end')}",
        "funnel_totals":   funnel_totals,     # CPV landing-stats per variant (primary)
        "ga4_supplement":  ga4_supplement,    # GA4 optin page metrics (when available)
        "cpv_campaigns":   cpv_campaigns,     # per-campaign overview
        "funnel_backend":  {
            "frontend_sales_count": backend.get("frontend_sales_count", 0),
            "total_revenue":        backend.get("total_revenue", 0),
            "sku_breakdown":        backend.get("sku_breakdown", {}),
            "variant_sales":        backend.get("variant_sales", {}),
        },
        "cross_check": cross,
    }

    return f"""Here is this week's AstroLover Sketch funnel performance data:

{json.dumps(slim, indent=2)}

Data source notes:
- funnel_totals: CPV Labs landing-stats per variant. All funnel step volumes and rates come from here.
  - landing_page_views  = Views on landing page rows (primary entry metric)
  - optin_completions   = Subscribers on landing rows (opt-in form submissions = offer page entry)
  - offer_page_views    = Views on offer page rows (≈ optin_completions)
  - checkout_clicks     = Clicks on offer page rows (ClickBank checkout CTA clicked)
  - purchases           = Conversion on offer page rows (≈ ClickBank frontend sales)
  - All rates are pre-computed — use them directly.
- ga4_supplement: GA4 custom events. optin_page_views = view_opt_in_page (people who SAW the
  form page, before deciding to submit). Only present when has_custom_events=true.
- funnel_backend: ClickBank is the revenue source of truth (USD net after ClickBank fees).
- cpv_campaigns[x].revenue: CPV Labs revenue is always in SGD — do not compare directly to CB USD amounts.
  Use cross_check.cpv_revenue_usd for an approximate USD equivalent.

Analyse this data and return a JSON object with exactly this structure:

{{
  "period_summary": "2-3 sentence plain-English summary. Include total revenue, frontend sales count, and the single most critical funnel finding this week.",

  "funnel_scorecard": {{
    "upsell1_take_rate_pct":   <float: oto1 new_sales / frontend_sales_count * 100>,
    "downsell1_take_rate_pct": <float or null>,
    "upsell2_take_rate_pct":   <float or null>,
    "downsell2_take_rate_pct": <float or null>,
    "weakest_step":            "short label of the step with the biggest drop-off across all flows",
    "weakest_step_rate_pct":   <float>,
    "weakest_flow":            "flow_label of the underperforming flow, or 'all' if affects all",
    "commentary":              "1-2 sentences on overall funnel health."
  }},

  "variant_comparison": [
    {{
      "variant":                   "<flow key from funnel_totals, e.g. c→offer_v4>",
      "label":                     "<flow_label from funnel_totals, e.g. /c/ → /offer/v4/>",
      "landing_page_views":        <int from funnel_totals>,
      "optin_completions":         <int: Subscribers / form submissions>,
      "offer_page_views":          <int from funnel_totals>,
      "checkout_clicks":           <int from funnel_totals>,
      "purchases":                 <int from funnel_totals>,
      "landing_to_optin_rate":     <float>,
      "offer_to_checkout_rate":    <float>,
      "checkout_to_purchase_rate": <float>,
      "revenue":                   <float from funnel_backend.variant_sales if available, else null>,
      "status":                    "stronger | weaker | insufficient_data",
      "weakest_step":              "step label with lowest pass-through rate for this flow",
      "action":                    "protect | fix | test | monitor"
    }}
  ],

  "funnel_leaks": [
    {{
      "variant":        "<flow key e.g. c→offer_v4, or 'all' if affects every flow>",
      "step":           "short label e.g. checkout → purchase",
      "rate_pct":       <float>,
      "severity":       "critical | high | medium",
      "evidence_type":  "observed | inferred",
      "why_it_matters": "1 sentence: commercial impact of fixing this step"
    }}
  ],

  "upsell_analysis": {{
    "frontend_sales":        <int>,
    "oto1_sales":            <int>,
    "oto1_take_rate_pct":    <float>,
    "oto1_downsell_sales":   <int>,
    "oto2_sales":            <int or null>,
    "oto2_take_rate_pct":    <float or null>,
    "oto2_downsell_sales":   <int or null>,
    "total_backend_revenue": <float>,
    "avg_revenue_per_buyer": <float>,
    "commentary":            "1-2 sentences on backend monetization health."
  }},

  "needle_movers": [
    {{
      "rank":           <int 1-5>,
      "priority":       "critical | high | medium",
      "area":           "short label",
      "variant":        "<flow key e.g. c→offer_v4, or 'all' if affects every flow>",
      "evidence_type":  "observed | inferred | hypothesis",
      "why":            "1-2 sentences: what the data shows and why it matters.",
      "what":           ["specific action step 1", "specific action step 2", "specific action step 3"],
      "revenue_impact": "quantified estimate where funnel math permits, otherwise directional"
    }}
  ],

  "cpv_cb_discrepancy": {{
    "cpv_purchases":     <int: sum of purchases across all flows in funnel_totals>,
    "cb_frontend_sales": <int: from funnel_backend.frontend_sales_count>,
    "revenue_diff_usd":  <float: from cross_check>,
    "status":            "ok | warn | alert",
    "note":              "1 sentence explanation"
  }},

  "data_notes": "Any measurement anomalies, data gaps, or period-specific notes. null if nothing significant."
}}

Rules:
  - funnel_leaks: 2-5 items ordered by severity, flag per flow where relevant
  - needle_movers: 3-5 items max, ranked by estimated revenue impact
  - landing_to_optin_rate < 10%: flag as high severity
  - checkout_to_purchase_rate < 15%: flag as critical
  - OTO1 take rate: 40%+ strong, 25-39% average, < 25% weak
  - OTO2 take rate: 25%+ strong, 15-24% average, < 15% weak — if 0 sales, flag broken redirect
  - For any flow with < 20 checkout_clicks, note insufficient data but analyse directionally
  - variant_comparison: emit one entry per flow key in funnel_totals (no merging, no renaming)
  - variant field must be the exact funnel_totals key; label must be the flow_label value
  - cpv_cb_discrepancy: use cross_check values where available
  - Revenue impact: use funnel math (checkout_clicks × rate_improvement × AOV) when possible

Return JSON only.
"""


# ── API call ──────────────────────────────────────────────────────────────────

def call_claude(user_prompt: str, system_prompt: str) -> dict:
    if not ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY not set — add it to .env")

    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key":         ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type":      "application/json",
        },
        json={
            "model":      MODEL,
            "max_tokens": MAX_TOKENS,
            "system":     system_prompt,
            "messages":   [{"role": "user", "content": user_prompt}],
        },
        timeout=120,
    )
    resp.raise_for_status()

    resp_json = resp.json()
    if resp_json.get("stop_reason") == "max_tokens":
        raise ValueError(
            f"Claude response was truncated (hit max_tokens={MAX_TOKENS}). "
            "Increase MAX_TOKENS or reduce the prompt size."
        )
    raw = resp_json["content"][0]["text"].strip()

    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()

    start = raw.find("{")
    end   = raw.rfind("}") + 1
    if start != -1 and end > start:
        raw = raw[start:end]

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        print(f"[analyse_astro] Raw Claude response:\n{raw[:500]}")
        raise


# ── Mock output ───────────────────────────────────────────────────────────────

def mock_analysis() -> dict:
    return {
        "period_summary": "The funnel generated $3,330 from 90 front-end sales across both versions this week. Funnel v4 is outperforming v2 at every step, with the checkout-to-purchase rate being the weakest link across both variants at under 20%.",

        "funnel_scorecard": {
            "upsell1_take_rate_pct":   33.3,
            "downsell1_take_rate_pct": 10.0,
            "upsell2_take_rate_pct":   15.0,
            "downsell2_take_rate_pct": 6.7,
            "weakest_step":            "checkout → purchase",
            "weakest_step_rate_pct":   18.95,
            "weakest_flow":            "/c/ → /offer/v4/",
            "commentary": "Both flows show healthy optin rates, but checkout-to-purchase at 18.95% for /c/ → /offer/v4/ is the primary bottleneck — 4 in 5 people who click to ClickBank don't buy."
        },

        "variant_comparison": [
            {
                "variant":                   "c→offer_v4",
                "label":                     "/c/ → /offer/v4/",
                "landing_page_views":        5600,
                "optin_completions":         1428,
                "offer_page_views":          721,
                "checkout_clicks":           343,
                "purchases":                 65,
                "landing_to_optin_rate":     25.5,
                "offer_to_checkout_rate":    47.57,
                "checkout_to_purchase_rate": 18.95,
                "revenue":                   2405.00,
                "status":                    "stronger",
                "weakest_step":              "checkout → purchase",
                "action":                    "protect"
            },
            {
                "variant":                   "a_b→offer_v2",
                "label":                     "/a/ /b/ → /offer/v2/",
                "landing_page_views":        2400,
                "optin_completions":         528,
                "offer_page_views":          245,
                "checkout_clicks":           112,
                "purchases":                 25,
                "landing_to_optin_rate":     22.0,
                "offer_to_checkout_rate":    45.71,
                "checkout_to_purchase_rate": 22.32,
                "revenue":                   925.00,
                "status":                    "weaker",
                "weakest_step":              "landing → optin",
                "action":                    "fix"
            }
        ],

        "funnel_leaks": [
            {
                "variant":        "c→offer_v4",
                "step":           "checkout → purchase",
                "rate_pct":       18.95,
                "severity":       "critical",
                "evidence_type":  "observed",
                "why_it_matters": "343 people clicked to ClickBank but only 65 bought — fixing this single step recovers the most revenue per change made."
            },
            {
                "variant":        "a_b→offer_v2",
                "step":           "landing → optin",
                "rate_pct":       22.0,
                "severity":       "high",
                "evidence_type":  "observed",
                "why_it_matters": "/a/ /b/ loses 3.5 percentage points more visitors at the first step vs /c/ — compounding across 2,400 landing views this costs an estimated 84 optin page visits per week."
            },
            {
                "variant":        "all",
                "step":           "offer → checkout",
                "rate_pct":       46.8,
                "severity":       "medium",
                "evidence_type":  "observed",
                "why_it_matters": "Roughly half of sales page visitors don't click to checkout — a stronger CTA or urgency element could move a meaningful share of these visitors forward."
            }
        ],

        "upsell_analysis": {
            "frontend_sales":        90,
            "oto1_sales":            30,
            "oto1_take_rate_pct":    33.3,
            "oto1_downsell_sales":   6,
            "oto2_sales":            13,
            "oto2_take_rate_pct":    15.0,
            "oto2_downsell_sales":   4,
            "total_backend_revenue": 3330.0,
            "avg_revenue_per_buyer": 37.0,
            "commentary": "OTO1 at 33% is below the 40% strong benchmark — worth testing a video testimonial or more specific outcome promise on the upsell page. OTO2 at 15% is weak; verify the redirect before assuming a copy issue."
        },

        "needle_movers": [
            {
                "rank":           1,
                "priority":       "critical",
                "area":           "Checkout → Purchase (/c/ → /offer/v4/)",
                "variant":        "c→offer_v4",
                "evidence_type":  "observed",
                "why":            "v4 checkout-to-purchase is 18.95% — below the 20% flag threshold. At 343 checkout clicks this week, moving to 25% adds ~21 sales.",
                "what": [
                    "Review ClickBank order form — confirm price and product name exactly match the offer page",
                    "Add a clear guarantee or risk-reversal above the buy button on the offer page",
                    "Check if urgency or scarcity element is visible without scrolling on the offer page"
                ],
                "revenue_impact": "~$777 additional revenue per week at current checkout click volume and $37 AOV"
            },
            {
                "rank":           2,
                "priority":       "high",
                "area":           "Landing → Optin (/a/ /b/ → /offer/v2/)",
                "variant":        "a_b→offer_v2",
                "evidence_type":  "observed",
                "why":            "/a/ /b/ landing-to-optin is 22% vs 25.5% for /c/ — the gap suggests the v2 landing page hook or CTA is weaker, not a traffic quality issue.",
                "what": [
                    "Compare the above-the-fold headline and CTA between /c/ and /a/ /b/ landing pages",
                    "Test bringing the /c/ headline variant to /a/ /b/ landing pages",
                    "Ensure the CTA button on /a/ /b/ pages is above the fold on mobile"
                ],
                "revenue_impact": "Directional — closing the 3.5pt gap on 2,400 landing views adds ~84 optin visitors per week, flowing through at current downstream rates"
            },
            {
                "rank":           3,
                "priority":       "high",
                "area":           "OTO2 take rate (15%)",
                "variant":        "all",
                "evidence_type":  "observed",
                "why":            "OTO2 at 15% is well below the 25% average benchmark — first verify the redirect from OTO1 decline works, then evaluate the offer.",
                "what": [
                    "Make a test purchase and manually decline OTO1 — confirm OTO2 page loads correctly",
                    "If redirect is broken, fix immediately — this loses pure backend revenue on every buyer",
                    "If page loads: lead with the specific outcome benefit rather than product features"
                ],
                "revenue_impact": "Moving from 15% to 25% on 90 buyers = 9 additional OTO2 sales at $97 = $873 per week"
            },
            {
                "rank":           4,
                "priority":       "medium",
                "area":           "Offer → Checkout (all flows)",
                "variant":        "all",
                "evidence_type":  "observed",
                "why":            "Both variants show ~46-48% of sales page visitors clicking to checkout — roughly half leave without clicking, which at this volume is significant.",
                "what": [
                    "Check if the checkout CTA is visible above the fold on mobile for both variants",
                    "Test adding a second CTA button midway through the sales page copy",
                    "Review if the price is revealed before the CTA — consider moving it closer to the CTA"
                ],
                "revenue_impact": "Directional — each percentage point improvement in sales-to-checkout across both variants adds ~10 checkout clicks at current traffic"
            }
        ],

        "cpv_cb_discrepancy": {
            "cpv_conversions":   90,
            "cb_frontend_sales": 90,
            "revenue_diff_usd":  0.0,
            "status":            "ok",
            "note":              "CPV conversions match ClickBank frontend sales exactly — tracking is clean."
        },

        "data_notes": "CPV Labs landing-stats is now the primary funnel data source. All page view and click metrics are real tracked events. GA4 data is retained in the payload but not used in this analysis due to insufficient data."
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    if args.mock:
        print("[analyse_astro] Mock mode — skipping API call")
        analysis = mock_analysis()
    else:
        if not args.input:
            files      = sorted(glob.glob("output/astroloversketch/report_data_*.json"))
            args.input = files[-1] if files else "output/astroloversketch/report_data.json"
            print(f"[analyse_astro] Auto-detected input: {args.input}")

        if not args.out:
            date_part = Path(args.input).stem.replace("report_data_", "")
            args.out  = f"output/astroloversketch/analysis_{date_part}.json"

        input_path = Path(args.input)
        if not input_path.exists():
            print(f"Error: {input_path} not found. Run collect_astro.py first.", file=sys.stderr)
            sys.exit(1)

        with open(input_path) as f:
            data = json.load(f)

        print(f"[analyse_astro] Loaded {input_path} ({data['meta']['period_start']} → {data['meta']['period_end']})")
        print(f"[analyse_astro] Sending to Claude ({MODEL})...")

        analysis = call_claude(build_user_prompt(data), build_system_prompt())
        print("[analyse_astro] ✓ Analysis received")

    output = {
        "meta": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "model":        MODEL,
            "mode":         "mock" if args.mock else "live",
        },
        "analysis": analysis,
    }

    if not args.out:
        args.out = "output/astroloversketch/analysis.json"

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"[analyse_astro] Output written to {out_path}")

    nm = analysis.get("needle_movers", [])
    print(f"\n── {len(nm)} needle movers this week ──")
    for item in nm:
        icon = {"critical": "🔴", "high": "🟡", "medium": "⚪"}.get(item.get("priority", ""), "")
        print(f"  {icon} #{item['rank']} [{item.get('variant','?')}] {item['area']}")
    print()


if __name__ == "__main__":
    main()