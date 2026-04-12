"""
ClickBank Collector — Astro Lover Sketch
Vendor account: ASTROSKETC

SKU layout:
  Frontend:        alsv1-37 | alsv1 | alsv1-47
  OTO1:            alsv1-blueprint
  OTO1 Downsell:   alsv1-blueprint-discount
  OTO2:            alsv1-timeline
  OTO2 Downsell:   alsv1-timeline-discount

Auth / endpoint identical to AskSabrina — only vendor, SKU map, and
output shape differ (no basic/advanced mix, upsell take rates use
frontend_sales_count as denominator).
"""

import os
import requests
from .base import BaseCollector
from utils.config import get_sku_map, get_variant_map, get_variants


class ClickBankAstroCollector(BaseCollector):

    BASE_URL = "https://api.clickbank.com/rest/1.3"

    def __init__(self, mock=False):
        super().__init__(mock=mock)
        self.api_key     = os.environ.get("ASTRO_CLICKBANK_API_KEY", "")
        self.vendor      = os.environ.get("ASTRO_CLICKBANK_VENDOR", "ASTROSKETC")
        self.sku_map     = get_sku_map()
        self.variant_map = get_variant_map()

    def fetch(self, start_date: str, end_date: str) -> dict:
        if self.mock:
            return self._mock_data()
        orders = self._get_all_orders(start_date, end_date)
        return self._process_orders(orders)

    # ── API ───────────────────────────────────────────────────────────────────

    def _get_all_orders(self, start_date: str, end_date: str) -> list:
        all_orders = []
        for txn_type in ["SALE", "BILL", "RFND", "CGBK"]:
            page = 1
            while True:
                resp, status_code = self._request(
                    "GET", "/orders2/list",
                    params={
                        "vendor":    self.vendor,
                        "startDate": start_date,
                        "endDate":   end_date,
                        "type":      txn_type,
                        "role":      "VENDOR",
                    },
                    extra_headers={"Page": str(page)},
                )
                if not resp:
                    break
                if isinstance(resp, list):
                    all_orders.extend(resp)
                    break
                orders_raw = resp.get("orderData", [])
                # ClickBank returns a dict (not list) when there's exactly one order
                orders = [orders_raw] if isinstance(orders_raw, dict) else (orders_raw or [])
                all_orders.extend(orders)
                if status_code != 206 or len(orders) < 100:
                    break
                page += 1
        return all_orders

    def _request(self, method, path, params=None, extra_headers=None):
        headers = {"Authorization": self.api_key, "Accept": "application/json"}
        if extra_headers:
            headers.update(extra_headers)
        resp = requests.request(
            method, self.BASE_URL + path,
            headers=headers, params=params, timeout=30,
        )
        if not resp.ok:
            raise Exception(
                f"{resp.status_code} {resp.reason} — {resp.text[:300]}\n  URL: {resp.url}"
            )
        if not resp.text.strip():
            return {}, 200
        return resp.json(), resp.status_code

    @staticmethod
    def _extract_campaign_id(tracking_id: str) -> str:
        """trackingId = {code}_{campaignId}_{clickId} → middle segment."""
        try:
            parts = tracking_id.split("_")
            if len(parts) >= 2:
                return parts[1]
        except Exception:
            pass
        return "unknown"

    # ── Processing ────────────────────────────────────────────────────────────

    def _process_orders(self, orders: list) -> dict:
        sku_breakdown        = {}
        total_revenue        = 0.0
        frontend_sales_count = 0

        # Variant attribution (populated from config variant_map)
        variant_sales = {k: {"sales": 0, "revenue": 0.0} for k in get_variants().keys()}
        variant_sales.setdefault("other", {"sales": 0, "revenue": 0.0})

        for order in orders:
            txn_type       = order.get("transactionType")
            if txn_type not in ("SALE", "BILL", "RFND", "CGBK"):
                continue
            is_rebill  = txn_type == "BILL"
            is_refund  = txn_type in ("RFND", "CGBK")

            tracking_id = order.get("trackingId", "")
            campaign_id = self._extract_campaign_id(tracking_id)
            variant     = self.variant_map.get(campaign_id, "other")

            line_raw = order.get("lineItemData", {})
            lines    = line_raw if isinstance(line_raw, list) else [line_raw]

            for line in lines:
                sku       = line.get("itemNo", "unknown")
                amount    = float(line.get("accountAmount", 0))
                line_type = line.get("lineItemType", "")
                is_line_rebill = is_rebill or line_type == "REBILL"

                info = self.sku_map.get(sku, {"label": sku, "price": 0, "stage": "unknown"})

                if sku not in sku_breakdown:
                    sku_breakdown[sku] = {
                        "label":     info["label"],
                        "stage":     info["stage"],
                        "price":     info["price"],
                        "new_sales": 0,
                        "rebills":   0,
                        "refunds":   0,
                        "revenue":   0.0,
                    }

                if is_line_rebill:
                    sku_breakdown[sku]["rebills"] += 1
                    sku_breakdown[sku]["revenue"] += amount
                    total_revenue += amount
                elif is_refund:
                    sku_breakdown[sku]["refunds"] += 1
                    sku_breakdown[sku]["refund_amount"] = round(
                        sku_breakdown[sku].get("refund_amount", 0.0) + abs(amount), 2
                    )
                    total_revenue += amount  # negative, reduces net
                else:
                    sku_breakdown[sku]["new_sales"] += 1
                    sku_breakdown[sku]["revenue"]   += amount
                    total_revenue += amount
                    if info["stage"] == "frontend":
                        frontend_sales_count += 1
                        if variant not in variant_sales:
                            variant_sales[variant] = {"sales": 0, "revenue": 0.0}
                        variant_sales[variant]["sales"]   += 1
                        variant_sales[variant]["revenue"] += amount

        # Upsell take rates against frontend buyers
        fe_buyers = max(frontend_sales_count, 1)
        for sku, data in sku_breakdown.items():
            if data["stage"] not in ("frontend", "unknown"):
                data["take_rate_pct"] = round(data["new_sales"] / fe_buyers * 100, 1)

        # Revenue rounding
        for v in variant_sales.values():
            v["revenue"] = round(v["revenue"], 2)

        gross_revenue = sum(s["revenue"] for s in sku_breakdown.values() if s["revenue"] > 0)
        refund_total  = sum(abs(s["revenue"]) for s in sku_breakdown.values() if s["revenue"] < 0)
        refund_count  = sum(s.get("refunds", 0) for s in sku_breakdown.values())

        # Aggregate all frontend SKU sales for a single frontend count
        # (alsv1-37, alsv1, alsv1-47 are all frontend — already handled via stage tag)
        return {
            "total_revenue":        round(total_revenue, 2),
            "gross_revenue":        round(gross_revenue + refund_total, 2),
            "total_refunds":        refund_count,
            "total_refund_amount":  round(refund_total, 2),
            "frontend_sales_count": frontend_sales_count,
            "variant_sales":        variant_sales,
            "sku_breakdown":        sku_breakdown,
        }

    # ── Mock ──────────────────────────────────────────────────────────────────

    def _mock_data(self) -> dict:
        return {
            "total_revenue":        3330.00,
            "gross_revenue":        3367.00,
            "total_refunds":        1,
            "total_refund_amount":  37.00,
            "frontend_sales_count": 90,
            "variant_sales": {
                "funnel_v4": {"sales": 38, "revenue": 1406.00},
                "funnel_v2": {"sales": 52, "revenue": 1924.00},
                "other":     {"sales": 0,  "revenue": 0.0},
            },
            "sku_breakdown": {
                "alsv1-37": {
                    "label": "Front-end $37", "stage": "frontend",
                    "price": 37, "new_sales": 60, "rebills": 0,
                    "refunds": 1, "refund_amount": 37.00, "revenue": 2183.00,
                },
                "alsv1": {
                    "label": "Front-end (default)", "stage": "frontend",
                    "price": 37, "new_sales": 20, "rebills": 0,
                    "refunds": 0, "revenue": 740.00,
                },
                "alsv1-47": {
                    "label": "Front-end $47", "stage": "frontend",
                    "price": 47, "new_sales": 10, "rebills": 0,
                    "refunds": 0, "revenue": 407.00,
                },
                "alsv1-blueprint": {
                    "label": "OTO1 Blueprint", "stage": "oto1",
                    "price": 67, "new_sales": 30, "rebills": 0,
                    "refunds": 0, "revenue": 2010.00,
                    "take_rate_pct": 33.3,
                },
                "alsv1-blueprint-discount": {
                    "label": "OTO1 Downsell (Blueprint Discount)", "stage": "oto1_downsell",
                    "price": 47, "new_sales": 6, "rebills": 0,
                    "refunds": 0, "revenue": 282.00,
                    "take_rate_pct": 10.0,
                },
                "alsv1-timeline": {
                    "label": "OTO2 Timeline", "stage": "oto2",
                    "price": 97, "new_sales": 13, "rebills": 0,
                    "refunds": 0, "revenue": 1261.00,
                    "take_rate_pct": 15.0,
                },
                "alsv1-timeline-discount": {
                    "label": "OTO2 Downsell (Timeline Discount)", "stage": "oto2_downsell",
                    "price": 77, "new_sales": 4, "rebills": 0,
                    "refunds": 0, "revenue": 308.00,
                    "take_rate_pct": 6.7,
                },
            },
        }