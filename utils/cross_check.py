"""
Cross-check: CPV Labs vs ClickBank reconciliation.

Currency note:
  CPV Labs revenue is reported in SGD (Singapore dollar) — the currency of the
  ClickBank vendor account. ClickBank API `accountAmount` is in USD (net vendor
  revenue after ClickBank's ~10% fee). Direct revenue comparison is unreliable.

  Status is therefore based on CONVERSION COUNT alignment, not revenue amount.
  A CPV/CB conversion ratio within 0-20% is expected (attribution lag, OTO postbacks).
  Revenue diff is shown for reference only, with the currency note attached.

CPV conversion counting:
  CPV fires a postback for each ClickBank transaction (FE, OTO1, OTO2 = 3 postbacks).
  ClickBank frontend_sales_count = unique FE transactions only.
  Expected: CPV conversions >= CB frontend_sales by the number of upsell transactions.
"""

import os


def verify_totals(cb_data: dict, cpv_data: dict) -> dict:
    """
    Returns a cross-check summary dict included in the final JSON payload.

    Status is based on conversion count alignment:
      ok    — CPV conversions within 0-30% above CB frontend sales (normal for upsell postbacks)
      warn  — CPV conversions < CB frontend sales (postbacks missing — investigate)
      alert — CPV/CB diff > 30% above expected upsell ratio, or CPV < CB
    """
    # ClickBank side: frontend sales count and total revenue (USD net)
    sku_breakdown   = cb_data.get("sku_breakdown", {})
    cb_new_sales    = sum(
        v.get("new_sales", 0) for v in sku_breakdown.values()
        if v.get("stage") == "frontend"
    )
    # Count upsell SKUs to estimate expected extra postbacks
    upsell_sales    = sum(
        v.get("new_sales", 0) for v in sku_breakdown.values()
        if v.get("stage") not in ("frontend", "unknown")
    )
    cb_revenue_usd  = cb_data.get("total_revenue", 0)

    # CPV side: all conversions (FE + OTO postbacks) and revenue (SGD)
    cpv_conversions = cpv_data.get("total_conversions", 0)
    cpv_revenue_sgd = cpv_data.get("total_revenue", 0)

    # Currency conversion for display (SGD → USD)
    # Rate from env or default 0.74 (1 SGD ≈ 0.74 USD as of 2026)
    sgd_rate        = float(os.environ.get("CPV_SGD_TO_USD_RATE", "0.74"))
    cpv_revenue_usd = round(cpv_revenue_sgd * sgd_rate, 2)

    # Expected CPV conversions = FE sales + upsell transactions
    expected_cpv    = cb_new_sales + upsell_sales
    conv_diff_abs   = cpv_conversions - cb_new_sales
    conv_diff_pct   = round(conv_diff_abs / cb_new_sales * 100, 1) if cb_new_sales else 0

    # Status based on conversion alignment
    if cpv_conversions < cb_new_sales:
        status = "warn"
        note   = f"CPV conversions ({cpv_conversions}) < CB frontend sales ({cb_new_sales}) — postbacks may be missing."
    elif conv_diff_abs <= max(expected_cpv * 0.3, 5):
        status = "ok"
        note   = f"CPV {cpv_conversions} convs vs CB {cb_new_sales} FE sales — within expected range (includes OTO postbacks)."
    else:
        status = "alert"
        note   = f"CPV {cpv_conversions} convs vs CB {cb_new_sales} FE sales ({conv_diff_pct}% gap) — check tracking setup."

    return {
        "cb_new_sales":      cb_new_sales,
        "cb_revenue":        cb_revenue_usd,
        "cpv_conversions":   cpv_conversions,
        "cpv_revenue_sgd":   round(cpv_revenue_sgd, 2),
        "cpv_revenue_usd":   cpv_revenue_usd,   # approximate (SGD × rate)
        "sgd_to_usd_rate":   sgd_rate,
        "revenue_diff":      round(abs(cb_revenue_usd - cpv_revenue_usd), 2),
        "diff_pct":          round(abs(cb_revenue_usd - cpv_revenue_usd) / cb_revenue_usd * 100, 1) if cb_revenue_usd else 0,
        "status":            status,
        "note":              note,
        "currency_note":     f"CPV revenue in SGD (×{sgd_rate} → USD). CB revenue is net vendor USD after ClickBank fees.",
    }
