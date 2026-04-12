"""
AstroLover Sketch — Funnel Data Collector
Run manually or via cron/GitHub Actions every Monday.

Usage:
    python collect_astro.py                        # live API pull
    python collect_astro.py --mock                 # mock data (for testing)
    python collect_astro.py --start 2026-03-31 --end 2026-04-06

Output: output/astroloversketch/report_data_YYYY_MM_DD.json

Sources used:
  ✓ GA4          — page-level funnel views per step and variant
  ✓ CPV Labs     — campaign views, conversions, revenue, checkout clicks
  ✓ ClickBank    — sales, upsell/downsell take rates, revenue
  ✗ Facebook     — not used (no paid traffic yet)
  ✗ Maropost     — not used (email attribution via CPV Labs if/when active)
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from collectors.ga4_astro import GA4AstroCollector
from collectors.cpvlabs_astro import CPVLabsAstroCollector
from collectors.clickbank_astro import ClickBankAstroCollector
from utils.date_helpers import get_week_range
from utils.cross_check import verify_totals
from utils.logger import log


def parse_args():
    p = argparse.ArgumentParser(description="Collect AstroLover Sketch funnel data")
    p.add_argument("--mock",  action="store_true", help="Use mock data instead of live APIs")
    p.add_argument("--start", type=str, help="Start date YYYY-MM-DD (default: last Monday)")
    p.add_argument("--end",   type=str, help="End date YYYY-MM-DD (default: last Sunday)")
    p.add_argument("--out",   type=str, default="", help="Output file path override")
    return p.parse_args()


def _load_project_env():
    """Map ASTRO_* prefixed env vars to the unprefixed names collectors expect."""
    mappings = {
        "CLICKBANK_API_KEY":   "ASTRO_CLICKBANK_API_KEY",
        "CLICKBANK_VENDOR":    "ASTRO_CLICKBANK_VENDOR",
        "CPVLABS_API_KEY":     "ASTRO_CPVLABS_API_KEY",
        "CPVLABS_BASE_URL":    "ASTRO_CPVLABS_BASE_URL",
        "CPVLABS_GROUP":       "ASTRO_CPVLABS_GROUP",
        "GA4_PROPERTY_ID":     "ASTRO_GA4_PROPERTY_ID",
        "CONFIG_FILE":         "ASTRO_CONFIG_FILE",
    }
    for target, source in mappings.items():
        val = os.environ.get(source, "")
        if val:
            os.environ[target] = val


def build_funnel_snapshot(ga4: dict, cpv: dict, cb: dict) -> dict:
    """
    Roll up top-line funnel numbers into a single snapshot dict.

    Primary data source: CPV Labs landing-stats (funnel_totals per variant).
    Supplement: GA4 optin_page_views (custom event, available after Apr 2026).
    Revenue / sales: ClickBank.

    Funnel steps:
      landing_page_views   — CPV landing rows Views (sum across all detected flows)
      optin_page_views     — GA4 view_opt_in_page (the opt-in FORM page view, pre-submit)
      optin_completions    — CPV Subscribers on landing rows (= form submissions)
      offer_page_views     — CPV offer rows Views (= people who completed opt-in)
      checkout_clicks      — CPV offer rows Clicks (= offer CTA clicked)
      frontend_sales       — ClickBank new_sales on frontend SKUs (revenue truth)

    Rates (all CPV-sourced):
      landing_to_optin_pct   = optin_completions / landing_page_views * 100
      optin_to_offer_pct     = offer_page_views  / optin_completions  * 100  (≈100%)
      offer_to_checkout_pct  = checkout_clicks   / offer_page_views   * 100
      checkout_to_sale_pct   = frontend_sales    / checkout_clicks    * 100
      overall_rate_pct       = frontend_sales    / landing_page_views * 100
    """
    def rate(num, den):
        return round(num / den * 100, 2) if den else 0.0

    # ── CPV funnel_totals (primary source) ───────────────────────────────────
    funnel_totals = (cpv or {}).get("funnel_totals", {})

    total_landing    = 0
    total_optin_comp = 0
    total_offer      = 0
    total_checkout   = 0

    # Sum across all detected flows (fully dynamic — no hardcoded v4/v2 keys)
    for vd in funnel_totals.values():
        total_landing    += vd.get("landing_page_views", 0)
        total_optin_comp += vd.get("optin_completions",  0)
        total_offer      += vd.get("offer_page_views",   0)
        total_checkout   += vd.get("checkout_clicks",    0)

    # ── GA4 optin_page_views (supplement — 0 if events not yet active) ───────
    total_optin_page = sum(
        v.get("optin_page_views", 0) for v in (ga4 or {}).values()
        if isinstance(v, dict) and "error" not in v
    )

    # ── ClickBank (revenue truth) ─────────────────────────────────────────────
    frontend_sales = (cb or {}).get("frontend_sales_count", 0)
    total_revenue  = (cb or {}).get("total_revenue", 0.0)
    avg_per_buyer  = round(total_revenue / frontend_sales, 2) if frontend_sales else 0.0

    return {
        # Volume — CPV primary
        "landing_page_views":          total_landing,
        "optin_page_views":            total_optin_page,    # GA4 supplement
        "optin_completions":           total_optin_comp,
        "offer_page_views":            total_offer,
        "checkout_clicks":             total_checkout,
        "frontend_sales":              frontend_sales,
        "total_revenue_usd":           total_revenue,
        "avg_revenue_per_buyer_usd":   avg_per_buyer,
        # Rates — CPV-based
        "landing_to_optin_pct":        rate(total_optin_comp, total_landing),
        "optin_to_offer_pct":          rate(total_offer,      total_optin_comp),
        "offer_to_checkout_pct":       rate(total_checkout,   total_offer),
        "checkout_to_sale_pct":        rate(frontend_sales,   total_checkout),
        "overall_rate_pct":            rate(frontend_sales,   total_landing),
    }


def main():
    args = parse_args()
    _load_project_env()

    log("Project: astroloversketch")

    if args.start and args.end:
        start_date, end_date = args.start, args.end
    else:
        start_date, end_date = get_week_range()

    log(f"Collecting data for {start_date} → {end_date}")
    log(f"Mode: {'MOCK' if args.mock else 'LIVE'}")

    if not args.out:
        args.out = f"output/astroloversketch/report_data_{end_date.replace('-', '_')}.json"

    # ── Run collectors ────────────────────────────────────────────────────────
    collectors = {
        "ga4":       GA4AstroCollector(mock=args.mock),
        "cpvlabs":   CPVLabsAstroCollector(mock=args.mock),
        "clickbank": ClickBankAstroCollector(mock=args.mock),
    }

    results = {}
    errors  = []

    for name, collector in collectors.items():
        log(f"  Fetching {name}...")
        try:
            results[name] = collector.fetch(start_date, end_date)
            log(f"  ✓ {name}")
        except Exception as e:
            log(f"  ✗ {name}: {e}", level="error")
            errors.append({"source": name, "error": str(e)})
            results[name] = None

    ga4 = results.get("ga4") or {}
    cpv = results.get("cpvlabs") or {}
    cb  = results.get("clickbank") or {}

    # ── Cross-check CPV vs ClickBank ──────────────────────────────────────────
    cross_check = None
    if cb and cpv:
        cross_check = verify_totals(cb, cpv)
        log(
            f"  Cross-check: CPV={cross_check['cpv_conversions']} "
            f"| CB={cross_check['cb_new_sales']} "
            f"| diff=${cross_check['revenue_diff']:.2f}"
        )

    # ── Assemble payload ──────────────────────────────────────────────────────
    payload = {
        "meta": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "period_start": start_date,
            "period_end":   end_date,
            "project":      "astroloversketch",
            "mode":         "mock" if args.mock else "live",
            "errors":       errors,
        },
        "funnel_snapshot":  build_funnel_snapshot(ga4, cpv, cb),
        "funnel_variants":  ga4,      # per-variant page-view funnel data
        "cpv_tracking":     cpv,      # campaign views, conversions, checkout clicks
        "funnel_backend":   cb,       # ClickBank SKU breakdown, upsells, revenue
        "cross_check":      cross_check,
    }

    # ── Write output ──────────────────────────────────────────────────────────
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(payload, f, indent=2, default=str)

    log(f"Output written to {out_path}")

    if errors:
        log(f"Completed with {len(errors)} error(s) — check output.meta.errors", level="warn")
        sys.exit(1)
    else:
        log("Collection complete ✓")


if __name__ == "__main__":
    main()