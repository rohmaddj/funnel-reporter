"""
CPV Lab Pro Collector (CPV One cloud)
Base URL: https://cli-XXXX.clprdr.com/api/v2

Endpoints:
  GET /api/v2/campaigns/                              → list campaigns, filter by CampaignGroup
  GET /api/v2/stats/:campaign_id/performance-stats   → date-filtered stats per campaign

Auth: API-Key header
"""

import os
import requests
from .base import BaseCollector


CPV_GROUP  = os.environ.get("CPVLABS_GROUP", "Ask Sabrina")
MIN_VISITS = 5

SOURCE_KEYWORDS = {
    "email":    ["maropost", "email", "broadcast", "sequence", "welcome", "abandoned",
                 "reactivation", "discount", "scarcity", "urgency", "education", "low intent"],
    "facebook": ["facebook", "fb", "paid", "adv+", "img", "chatbot", "chat bot", "mvlabs"],
}


class CPVLabsCollector(BaseCollector):

    def __init__(self, mock=False):
        super().__init__(mock=mock)
        self.api_key  = os.environ.get("CPVLABS_API_KEY", "")
        self.base_url = os.environ.get("CPVLABS_BASE_URL", "").rstrip("/")

    def fetch(self, start_date: str, end_date: str) -> dict:
        if self.mock:
            return self._mock_data()

        if not self.api_key:
            raise Exception("CPVLABS_API_KEY is empty — set it in .env")
        if not self.base_url:
            raise Exception("CPVLABS_BASE_URL is empty — set it in .env")

        # Step 1: get all campaigns, filter to Ask Sabrina group
        all_campaigns = self._get_campaigns()
        group_campaigns = [c for c in all_campaigns if c.get("CampaignGroup") == CPV_GROUP]

        if not group_campaigns:
            available = list({c.get("CampaignGroup", "?") for c in all_campaigns})
            raise Exception(
                f"No campaigns found for group '{CPV_GROUP}'.\n"
                f"  Available groups: {available}\n"
                f"  Update CPVLABS_GROUP in .env to match exactly (case sensitive)"
            )

        # Step 2: fetch date-filtered performance stats per campaign
        stats = {}
        for camp in group_campaigns:
            cid   = str(camp.get("CampaignID", ""))
            cname = camp.get("CampaignName", f"Campaign {cid}")
            if not cid:
                continue
            try:
                perf = self._get_performance_stats(cid, start_date, end_date)
                if perf is not None:
                    stats[cid] = {**perf, "_name": cname}
            except Exception as e:
                print(f"  [cpv] Warning: campaign {cid} ({cname}): {e}")

        return self._process(stats)

    # ── API calls ─────────────────────────────────────────────────────────────

    def _get(self, path: str, params: dict = None) -> any:
        resp = requests.get(
            f"{self.base_url}{path}",
            headers={"API-Key": self.api_key, "Accept": "application/json"},
            params=params or {},
            timeout=30,
        )
        if not resp.ok:
            raise Exception(f"{resp.status_code} {resp.reason}\n  URL: {resp.url}\n  Body: {resp.text[:300]}")
        if not resp.text.strip():
            return {}
        return resp.json()

    def _get_campaigns(self) -> list:
        data = self._get("/api/v2/campaigns/")
        if isinstance(data, list):
            return data
        for key in ("campaigns", "Campaigns", "data", "results"):
            if key in data:
                return data[key]
        return []

    def _get_performance_stats(self, campaign_id: str, start_date: str, end_date: str) -> dict:
        rows = self._get(
            f"/api/v2/stats/{campaign_id}/ad-stats",
            params={
                "interval":   "custom",
                "start_date": start_date,
                "end_date":   end_date,
                "filter[start_date]": start_date,
                "filter[end_date]":   end_date,
                "filter[interval]":   "custom",
            },
        )
        if not rows or not isinstance(rows, list):
            return {}
        return {
            "Views":      sum(int(r.get("Views", 0))      for r in rows),
            "Conversion": sum(int(r.get("Conversion", 0)) for r in rows),
            "Revenue":    sum(float(r.get("Revenue", 0))  for r in rows),
        }

    # ── Processing ─────────────────────────────────────────────────────────────

    def _process(self, raw: dict) -> dict:
        campaigns = {}
        totals_by_source = {}

        for cid, row in raw.items():
            cname  = row.get("_name", f"Campaign {cid}")
            # Try multiple field name variations
            visits = int(row.get("Visits") or row.get("visits") or row.get("Views") or row.get("views") or 0)
            convs  = int(row.get("Conversion") or row.get("Conversions") or row.get("conversions") or row.get("Sales") or row.get("sales") or 0)
            rev    = float(row.get("Revenue") or row.get("revenue") or row.get("Profit") or 0)
            cr     = round(convs / visits * 100, 2) if visits else 0

            if visits < MIN_VISITS:
                continue

            source = self._detect_source(cname)

            campaigns[cid] = {
                "label":       cname,
                "source":      source,
                "views":       visits,
                "conversions": convs,
                "revenue":     round(rev, 2),
                "cr_pct":      cr,
                "status":      self._status(cr, visits, rev),
            }

            if source not in totals_by_source:
                totals_by_source[source] = {"views": 0, "conversions": 0, "revenue": 0.0}
            totals_by_source[source]["views"]       += visits
            totals_by_source[source]["conversions"] += convs
            totals_by_source[source]["revenue"]     += rev

        return {
            "campaigns":         campaigns,
            "totals_by_source":  {k: {**v, "revenue": round(v["revenue"], 2)} for k, v in totals_by_source.items()},
            "total_conversions": sum(c["conversions"] for c in campaigns.values()),
            "total_revenue":     round(sum(c["revenue"] for c in campaigns.values()), 2),
        }

    def _detect_source(self, name: str) -> str:
        name_lower = name.lower()
        for source, keywords in SOURCE_KEYWORDS.items():
            if any(k in name_lower for k in keywords):
                return source
        return "unknown"

    def _status(self, cr, visits, revenue):
        if revenue == 0 and visits >= 20:
            return "dead"
        if cr >= 2.0:
            return "strong"
        if cr >= 0.5:
            return "active"
        return "weak"

    def _mock_data(self) -> dict:
        return {
            "campaigns": {
                "87": {"label": "Maropost - Interactive VSL V2", "source": "email",    "views": 3822, "conversions": 17, "revenue": 920.69,  "cr_pct": 0.44, "status": "active"},
                "77": {"label": "Maropost - Interactive VSL V1", "source": "email",    "views": 759,  "conversions": 3,  "revenue": 376.91,  "cr_pct": 0.40, "status": "weak"},
                "69": {"label": "Maropost - Abandoned",          "source": "email",    "views": 47,   "conversions": 2,  "revenue": 77.20,   "cr_pct": 4.26, "status": "strong"},
                "78": {"label": "Facebook - Interactive VSL V1", "source": "facebook", "views": 118,  "conversions": 2,  "revenue": 201.35,  "cr_pct": 1.69, "status": "weak"},
                "88": {"label": "Facebook - Interactive VSL V2", "source": "facebook", "views": 15,   "conversions": 1,  "revenue": 157.61,  "cr_pct": 6.67, "status": "strong"},
            },
            "totals_by_source": {
                "email":    {"views": 4628, "conversions": 22, "revenue": 1374.80},
                "facebook": {"views": 133,  "conversions": 3,  "revenue": 358.96},
            },
            "total_conversions": 25,
            "total_revenue":     1733.76,
        }