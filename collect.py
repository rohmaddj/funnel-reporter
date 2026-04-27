"""
Weekly Funnel Data Collector
Run manually or via cron/GitHub Actions every Sunday.

Usage:
    python collect.py                        # live API pull
    python collect.py --mock                 # use mock data (for testing)
    python collect.py --start 2026-03-01 --end 2026-03-14  # custom date range
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from collectors.clickbank import ClickBankCollector
# CPVLabsAstroCollector extends the base CPV collector with landing-stats /
# funnel_totals (per-flow funnel step aggregation). Despite the name, it is
# project-agnostic — Ask Sabrina uses it too.
from collectors.cpvlabs_astro import CPVLabsAstroCollector as CPVLabsExtendedCollector
# Facebook ad-spend collector intentionally not imported — Ask Sabrina has no
# active paid traffic, so we don't run the FB Marketing API call. CPV Labs
# still surfaces any Facebook-tagged campaigns via the cpvlabs collector.
# from collectors.facebook import FacebookCollector
from collectors.maropost import MaropostCollector
from collectors.ga4 import GA4Collector
from utils.date_helpers import get_week_range
from utils.logger import log


def parse_args():
    parser = argparse.ArgumentParser(description="Collect weekly funnel performance data")
    parser.add_argument("--mock", action="store_true", help="Use mock data instead of live APIs")
    parser.add_argument("--start", type=str, help="Start date YYYY-MM-DD (default: last Monday)")
    parser.add_argument("--end", type=str, help="End date YYYY-MM-DD (default: last Sunday)")
    parser.add_argument("--out", type=str, default="", help="Output file path")
    parser.add_argument("--project", type=str, default="", help="Project name: asksabrina or astroloversketch")
    return parser.parse_args()

def _load_project_env(prefix: str):
    """Re-map prefixed env vars to the unprefixed names collectors expect."""
    mappings = {
        "CLICKBANK_API_KEY":   f"{prefix}_CLICKBANK_API_KEY",
        "CLICKBANK_VENDOR":    f"{prefix}_CLICKBANK_VENDOR",
        "CPVLABS_API_KEY":     f"{prefix}_CPVLABS_API_KEY",
        "CPVLABS_BASE_URL":    f"{prefix}_CPVLABS_BASE_URL",
        "CPVLABS_GROUP":       f"{prefix}_CPVLABS_GROUP",
        "FB_ACCESS_TOKEN":     f"{prefix}_FB_ACCESS_TOKEN",
        "FB_AD_ACCOUNT_ID":    f"{prefix}_FB_AD_ACCOUNT_ID",
        "FB_SGD_TO_USD_RATE":  f"{prefix}_FB_SGD_TO_USD_RATE",
        "MAROPOST_API_KEY":    f"{prefix}_MAROPOST_API_KEY",
        "MAROPOST_ACCOUNT_ID": f"{prefix}_MAROPOST_ACCOUNT_ID",
        "USE_CPV_FOR_EMAIL":   f"{prefix}_USE_CPV_FOR_EMAIL",
        "GA4_PROPERTY_ID":     f"{prefix}_GA4_PROPERTY_ID",
        "CONFIG_FILE":         f"{prefix}_CONFIG_FILE",
    }
    for target, source in mappings.items():
        val = os.environ.get(source, "")
        if val:
            os.environ[target] = val

def main():
    args = parse_args()

    # Load project-specific env vars
    project = args.project or os.environ.get("PROJECT", "asksabrina")

    # Delegate to project-specific script when applicable
    if project == "astroloversketch":
        import subprocess
        cmd = [sys.executable, "collect_astro.py"]
        if args.start: cmd += ["--start", args.start]
        if args.end:   cmd += ["--end",   args.end]
        if args.mock:  cmd += ["--mock"]
        sys.exit(subprocess.run(cmd).returncode)

    prefix  = "ASTRO" if project == "astroloversketch" else "ASKSABRINA"
    _load_project_env(prefix)
    log(f"Project: {project}")

    # Date range: default to the last full week (Mon–Sun)
    if args.start and args.end:
        start_date = args.start
        end_date = args.end
    else:
        start_date, end_date = get_week_range()

    log(f"Collecting data for {start_date} → {end_date}")
    log(f"Mode: {'MOCK' if args.mock else 'LIVE'}")

    if not args.out:
        args.out = f"output/{project}/report_data_{end_date.replace('-', '_')}.json"

    # ── Run each collector ───────────────────────────────────────────────────
    collectors = {
        "clickbank": ClickBankCollector(mock=args.mock),
        "cpvlabs":   CPVLabsExtendedCollector(mock=args.mock),
        # "facebook":  FacebookCollector(mock=args.mock),  # disabled — no paid traffic
        "maropost":  MaropostCollector(mock=args.mock),
        "ga4":       GA4Collector(mock=args.mock),
    }

    results = {}
    errors = []

    for name, collector in collectors.items():
        log(f"  Fetching {name}...")
        try:
            results[name] = collector.fetch(start_date, end_date)
            log(f"  ✓ {name}")
        except Exception as e:
            log(f"  ✗ {name}: {e}", level="error")
            errors.append({"source": name, "error": str(e)})
            results[name] = None

    # ── Cross-check CPV vs ClickBank ────────────────────────────────────────
    cross_check = None
    if results["clickbank"] and results["cpvlabs"]:
        from utils.cross_check import verify_totals
        cross_check = verify_totals(results["clickbank"], results["cpvlabs"])
        log(f"  Cross-check: CPV={cross_check['cpv_conversions']} | CB={cross_check['cb_new_sales']} | diff=${cross_check['revenue_diff']:.2f}")

    # ── Assemble final payload ───────────────────────────────────────────────
    payload = {
        "meta": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "period_start": start_date,
            "period_end": end_date,
            "project": project,
            "mode": "mock" if args.mock else "live",
            "errors": errors,
        },
        "funnel_snapshot": build_funnel_snapshot(results),
        "traffic_sources": {
            "email":    results.get("maropost"),
            "paid":     results.get("facebook"),  # None when FB collector disabled
            "tracking": results.get("cpvlabs"),
        },
        "funnel_backend": results.get("clickbank"),
        "cross_check": cross_check,
        "funnel_variants": results.get("ga4"),
    }

    # ── Write output ─────────────────────────────────────────────────────────
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


def build_funnel_snapshot(results: dict) -> dict:
    """
    Roll up top-line numbers from all sources into a single snapshot dict.
    This is what populates the 'Funnel Snapshot' table in your report.

    funnel_totals (CPV landing-stats) provides per-flow funnel step volumes
    (landing → optin → offer → checkout → purchase) which we sum across all
    detected flows. New CPV campaigns / sales pages get picked up automatically.
    """
    cb  = results.get("clickbank") or {}
    fb  = results.get("facebook")  or {}
    cpv = results.get("cpvlabs")   or {}
    cpv_email = cpv.get("totals_by_source", {}).get("email", {})

    total_revenue  = cb.get("total_revenue", 0)
    frontend_sales = cb.get("frontend_sales_count", 0)
    avg_per_buyer  = round(total_revenue / frontend_sales, 2) if frontend_sales else 0

    fb_spend_sgd = fb.get("total_spend_sgd", 0)
    fb_spend_usd = fb.get("total_spend_usd", 0)

    # ── Sum funnel step volumes across all flows (dynamic) ──────────────────
    funnel_totals = cpv.get("funnel_totals", {})
    total_landing    = sum(v.get("landing_page_views", 0) for v in funnel_totals.values())
    total_optin_comp = sum(v.get("optin_completions",  0) for v in funnel_totals.values())
    total_offer      = sum(v.get("offer_page_views",   0) for v in funnel_totals.values())
    total_checkout   = sum(v.get("checkout_clicks",    0) for v in funnel_totals.values())

    def rate(num, den):
        return round(num / den * 100, 2) if den else 0.0

    return {
        "total_revenue_usd": total_revenue,
        "frontend_sales": frontend_sales,
        "avg_revenue_per_buyer_usd": avg_per_buyer,
        "paid_traffic_spend_sgd": fb_spend_sgd,
        "paid_traffic_spend_usd": fb_spend_usd,
        "email_total_views":       cpv_email.get("views", 0),
        "email_total_conversions": cpv_email.get("conversions", 0),
        "email_total_revenue":     cpv_email.get("revenue", 0),
        # Funnel step volumes (CPV landing-stats, summed across all flows)
        "landing_page_views":   total_landing,
        "optin_completions":    total_optin_comp,
        "offer_page_views":     total_offer,
        "checkout_clicks":      total_checkout,
        # Funnel rates
        "landing_to_optin_pct":   rate(total_optin_comp, total_landing),
        "offer_to_checkout_pct":  rate(total_checkout,   total_offer),
        "checkout_to_sale_pct":   rate(frontend_sales,   total_checkout),
        "overall_rate_pct":       rate(frontend_sales,   total_landing),
    }


if __name__ == "__main__":
    main()