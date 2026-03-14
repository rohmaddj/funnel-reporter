"""
Cross-check: CPV Labs vs ClickBank reconciliation.

CPV counts every ClickBank postback as a "conversion" — one buyer who takes
frontend + OTO1 + OTO2 fires 3 postbacks = 3 CPV conversions.
ClickBank counts unique transactions per SKU.

This module reconciles the two so the report can flag discrepancies.
A diff under ~$20 is expected (legacy rebills, timing delays).
A larger diff needs manual investigation.
"""


def verify_totals(cb_data: dict, cpv_data: dict) -> dict:
    """
    Returns a cross-check summary dict included in the final JSON payload.
    """
    # ClickBank side: sum all new sales across SKUs
    sku_breakdown = cb_data.get("sku_breakdown", {})
    # Only count frontend SKUs for buyer count — upsells are separate line items
    cb_new_sales  = sum(
        v.get("new_sales", 0) for v in sku_breakdown.values()
        if v.get("stage") == "frontend"
    )
    cb_revenue    = cb_data.get("total_revenue", 0)

    # CPV side
    cpv_conversions = cpv_data.get("total_conversions", 0)
    cpv_revenue     = cpv_data.get("total_revenue", 0)

    revenue_diff    = abs(cb_revenue - cpv_revenue)
    diff_pct        = round(revenue_diff / cb_revenue * 100, 2) if cb_revenue else 0

    # Status: within $20 and 2% is normal
    if revenue_diff <= 150 and diff_pct <= 5:
        status = "ok"
        note   = "Difference within expected range (rebills / timing)."
    elif revenue_diff <= 50:
        status = "warn"
        note   = f"${revenue_diff:.2f} gap — check for legacy rebills or delayed postbacks."
    else:
        status = "alert"
        note   = f"${revenue_diff:.2f} gap ({diff_pct}%) — manual investigation needed."

    return {
        "cb_new_sales":     cb_new_sales,
        "cb_revenue":       cb_revenue,
        "cpv_conversions":  cpv_conversions,
        "cpv_revenue":      round(cpv_revenue, 2),
        "revenue_diff":     round(revenue_diff, 2),
        "diff_pct":         diff_pct,
        "status":           status,
        "note":             note,
    }