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
from collectors.cpvlabs import CPVLabsCollector
from collectors.facebook import FacebookCollector
from collectors.maropost import MaropostCollector
from utils.date_helpers import get_week_range
from utils.logger import log


def parse_args():
    parser = argparse.ArgumentParser(description="Collect weekly funnel performance data")
    parser.add_argument("--mock", action="store_true", help="Use mock data instead of live APIs")
    parser.add_argument("--start", type=str, help="Start date YYYY-MM-DD (default: last Monday)")
    parser.add_argument("--end", type=str, help="End date YYYY-MM-DD (default: last Sunday)")
    parser.add_argument("--out", type=str, default="output/report_data.json", help="Output file path")
    return parser.parse_args()


def main():
    args = parse_args()

    # Date range: default to the last full week (Mon–Sun)
    if args.start and args.end:
        start_date = args.start
        end_date = args.end
    else:
        start_date, end_date = get_week_range()

    log(f"Collecting data for {start_date} → {end_date}")
    log(f"Mode: {'MOCK' if args.mock else 'LIVE'}")

    # ── Run each collector ───────────────────────────────────────────────────
    collectors = {
        "clickbank": ClickBankCollector(mock=args.mock),
        "cpvlabs":   CPVLabsCollector(mock=args.mock),
        "facebook":  FacebookCollector(mock=args.mock),
        "maropost":  MaropostCollector(mock=args.mock),
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
            "mode": "mock" if args.mock else "live",
            "errors": errors,
        },
        "funnel_snapshot": build_funnel_snapshot(results),
        "traffic_sources": {
            "email":    results["maropost"],
            "paid":     results["facebook"],
            "tracking": results["cpvlabs"],
        },
        "funnel_backend": results["clickbank"],
        "cross_check": cross_check,
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
    """
    cb = results.get("clickbank") or {}
    fb = results.get("facebook") or {}
    mp = results.get("maropost") or {}
    cpv = results.get("cpvlabs") or {}

    total_revenue = cb.get("total_revenue", 0)
    frontend_sales = cb.get("frontend_sales_count", 0)
    avg_per_buyer = round(total_revenue / frontend_sales, 2) if frontend_sales else 0

    fb_spend_sgd = fb.get("total_spend_sgd", 0)
    fb_spend_usd = fb.get("total_spend_usd", 0)

    cpv_email = cpv.get("totals_by_source", {}).get("email", {})

    return {
        "total_revenue_usd": total_revenue,
        "frontend_sales": frontend_sales,
        "avg_revenue_per_buyer_usd": avg_per_buyer,
        "paid_traffic_spend_sgd": fb_spend_sgd,
        "paid_traffic_spend_usd": fb_spend_usd,
        "email_total_views":       cpv_email.get("views", 0),
        "email_total_conversions": cpv_email.get("conversions", 0),
        "email_total_revenue":     cpv_email.get("revenue", 0),
    }


if __name__ == "__main__":
    main()