"""
ClickBank Collector
Auth: Authorization header = clerk key
Endpoint: /orders2/list
Revenue: accountAmount (what you actually receive)
trackingId: {code}_{campaignId}_{clickId} — middle segment = CPV campaign ID
"""

import os
import requests
from .base import BaseCollector


SKU_MAP = {
    "abdt-basic":    {"label": "Front-end Basic",            "price": 37,  "stage": "frontend"},
    "abdt-advanced": {"label": "Front-end Advanced",         "price": 54,  "stage": "frontend"},
    "SPR-OB1":       {"label": "Order Bump ($14.99/mo)","price": 14.99,  "stage": "order_bump"},
    "SPR-OB2":       {"label": "Order Bump (free trial - $14.99/mo)",     "price": 0,   "stage": "order_bump"},
    "SSR":           {"label": "OTO1 Soul Signature $67",    "price": 67,  "stage": "oto1"},
    "SSR-D":         {"label": "OTO1 Downsell $47",          "price": 47,  "stage": "oto1_downsell"},
    "dhr":           {"label": "OTO2 Divine Helper $97",     "price": 97,  "stage": "oto2"},
    "dhr-d":         {"label": "OTO2 Downsell $77",          "price": 77,  "stage": "oto2_downsell"},
}

# CPV campaign ID → funnel variant
VARIANT_MAP = {
    "77": "destiny_v1", "78": "destiny_v1",
    "87": "destiny_v2", "88": "destiny_v2",
}


class ClickBankCollector(BaseCollector):

    BASE_URL = "https://api.clickbank.com/rest/1.3"

    def __init__(self, mock=False):
        super().__init__(mock=mock)
        self.api_key = os.environ.get("CLICKBANK_API_KEY", "")
        self.vendor    = os.environ.get("CLICKBANK_VENDOR", "SABRINAPSY")

    def fetch(self, start_date: str, end_date: str) -> dict:
        if self.mock:
            return self._mock_data()
        orders = self._get_all_orders(start_date, end_date)
        return self._process_orders(orders)

    def _get_all_orders(self, start_date: str, end_date: str) -> list:
        all_orders = []
        page = 1
        while True:
            resp, status_code = self._request("GET", "/orders2/list", params={
                "vendor":    self.vendor,
                "startDate": start_date,
                "endDate":   end_date,
                "type":      "SALE",
                "role":      "VENDOR",
            }, extra_headers={"Page": str(page)})

            if not resp:
                break
            if isinstance(resp, list):
                all_orders.extend(resp)
                break
            orders = resp.get("orderData", [])
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
            raise Exception(f"{resp.status_code} {resp.reason} — {resp.text[:300]}\n  URL: {resp.url}")
        if not resp.text.strip():
            return {}, 200
        return resp.json(), resp.status_code

    @staticmethod
    def _extract_campaign_id(tracking_id: str) -> str:
        """trackingId = {code}_{campaignId}_{clickId} → middle segment"""
        try:
            parts = tracking_id.split("_")
            if len(parts) >= 2:
                return parts[1]
        except Exception:
            pass
        return "unknown"

    def _process_orders(self, orders: list) -> dict:
        sku_breakdown = {}
        total_revenue = 0.0
        frontend_sales_count = 0

        variant_sales = {
            "destiny_v1": {"sales": 0, "revenue": 0.0},
            "destiny_v2": {"sales": 0, "revenue": 0.0},
            "other":      {"sales": 0, "revenue": 0.0},
        }

        for order in orders:
            if order.get("transactionType") != "SALE":
                continue

            tracking_id = order.get("trackingId", "")
            campaign_id = self._extract_campaign_id(tracking_id)
            variant     = VARIANT_MAP.get(campaign_id, "other")

            line_raw = order.get("lineItemData", {})
            lines    = line_raw if isinstance(line_raw, list) else [line_raw]

            for line in lines:
                sku       = line.get("itemNo", "unknown")
                amount    = float(line.get("accountAmount", 0))
                line_type = line.get("lineItemType", "")
                is_rebill = line_type == "REBILL"

                info = SKU_MAP.get(sku, {"label": sku, "price": 0, "stage": "unknown"})

                if sku not in sku_breakdown:
                    sku_breakdown[sku] = {
                        "label":     info["label"],
                        "stage":     info["stage"],
                        "price":     info["price"],
                        "new_sales": 0,
                        "rebills":   0,
                        "revenue":   0.0,
                    }

                if is_rebill:
                    sku_breakdown[sku]["rebills"] += 1
                else:
                    sku_breakdown[sku]["new_sales"] += 1
                    if info["stage"] == "frontend":
                        frontend_sales_count += 1
                        variant_sales[variant]["sales"]   += 1
                        variant_sales[variant]["revenue"] += amount

                sku_breakdown[sku]["revenue"] += amount
                total_revenue += amount

        frontend_buyers = max(frontend_sales_count, 1)
        for sku, data in sku_breakdown.items():
            if data["stage"] not in ("frontend", "order_bump", "unknown"):
                data["take_rate_pct"] = round(data["new_sales"] / frontend_buyers * 100, 1)

        basic_sales    = sku_breakdown.get("abdt-basic",    {}).get("new_sales", 0)
        advanced_sales = sku_breakdown.get("abdt-advanced", {}).get("new_sales", 0)
        total_fe       = basic_sales + advanced_sales

        for v in variant_sales.values():
            v["revenue"] = round(v["revenue"], 2)

        return {
            "total_revenue":        round(total_revenue, 2),
            "frontend_sales_count": frontend_sales_count,
            "variant_sales":        variant_sales,
            "sku_breakdown":        sku_breakdown,
            "frontend_mix": {
                "basic_count":    basic_sales,
                "advanced_count": advanced_sales,
                "advanced_pct":   round(advanced_sales / total_fe * 100, 1) if total_fe else 0,
            },
        }

    def _mock_data(self) -> dict:
        return {
            "total_revenue": 1576.05,
            "frontend_sales_count": 10,
            "variant_sales": {
                "destiny_v1": {"sales": 3, "revenue": 201.35},
                "destiny_v2": {"sales": 4, "revenue": 280.14},
                "other":      {"sales": 3, "revenue": 115.70},
            },
            "sku_breakdown": {
                "abdt-basic":    {"label": "Front-end Basic",            "stage": "frontend",      "price": 37, "new_sales": 3, "rebills": 0, "revenue": 115.70},
                "abdt-advanced": {"label": "Front-end Advanced",         "stage": "frontend",      "price": 54, "new_sales": 7, "rebills": 0, "revenue": 402.98},
                "SPR-OB2":       {"label": "Order Bump ($14.99/mo)",     "stage": "order_bump",    "price": 0,  "new_sales": 4, "rebills": 9, "revenue": 377.47},
                "SSR":           {"label": "OTO1 Soul Signature $67",    "stage": "oto1",          "price": 67, "new_sales": 5, "rebills": 0, "revenue": 355.75, "take_rate_pct": 50.0},
                "SSR-D":         {"label": "OTO1 Downsell $47",          "stage": "oto1_downsell", "price": 47, "new_sales": 0, "rebills": 0, "revenue": 0.0,    "take_rate_pct": 0.0},
                "dhr":           {"label": "OTO2 Divine Helper $97",     "stage": "oto2",          "price": 97, "new_sales": 3, "rebills": 0, "revenue": 309.16, "take_rate_pct": 60.0},
                "dhr-d":         {"label": "OTO2 Downsell $77",          "stage": "oto2_downsell", "price": 77, "new_sales": 0, "rebills": 0, "revenue": 0.0,    "take_rate_pct": 0.0},
            },
            "frontend_mix": {"basic_count": 3, "advanced_count": 7, "advanced_pct": 70.0},
        }