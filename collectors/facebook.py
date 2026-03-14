"""
Facebook / Instagram Ads Collector
Fetches spend, impressions, clicks, and landing-page CTR from the Marketing API.

Docs: https://developers.facebook.com/docs/marketing-api/insights
Auth: Long-lived Page Access Token (store in FB_ACCESS_TOKEN env var)
      Ad Account ID: act_XXXXXXX (store in FB_AD_ACCOUNT_ID env var)

What we pull:
- Spend, impressions, link clicks, and landing_page_view (LP clicks) per campaign
- Broken down by platform (facebook / instagram) via publisher_platform breakdown
- We cross-reference CPV campaign IDs via UTM parameters in the ad URL
  (or manually map ad names → CPV IDs in CAMPAIGN_TO_CPV below)

IMPORTANT ABOUT CURRENCY:
Your FB account bills in SGD. The API returns spend in your account currency.
We store both SGD and a USD estimate (using FB_SGD_TO_USD_RATE env var).
"""

import os
import requests
from .base import BaseCollector


# Manual mapping: FB campaign name fragment → CPV campaign ID
# Update this when you create new campaigns
CAMPAIGN_TO_CPV = {
    "VSL V1": "78",
    "VSL V2": "88",
}

FB_API_VERSION = "v19.0"
DEFAULT_SGD_TO_USD = 0.743  # update weekly or pull from an FX API


class FacebookCollector(BaseCollector):

    BASE_URL = f"https://graph.facebook.com/{FB_API_VERSION}"

    def __init__(self, mock=False):
        super().__init__(mock=mock)
        self.token      = os.environ.get("FB_ACCESS_TOKEN", "")
        self.account_id = os.environ.get("FB_AD_ACCOUNT_ID", "")  # e.g. "act_123456789"
        self.sgd_to_usd = float(os.environ.get("FB_SGD_TO_USD_RATE", DEFAULT_SGD_TO_USD))

    def fetch(self, start_date: str, end_date: str) -> dict:
        if self.mock:
            return self._mock_data()

        raw = self._get_insights(start_date, end_date)
        return self._process(raw)

    # ── API calls ─────────────────────────────────────────────────────────────

    def _get_insights(self, start_date: str, end_date: str) -> list:
        """
        Pulls campaign-level insights broken down by publisher_platform.
        landing_page_views is the metric that matches what CPV calls 'visits'.
        """
        resp = requests.get(
            f"{self.BASE_URL}/{self.account_id}/insights",
            params={
                "access_token":  self.token,
                "level":         "campaign",
                "fields":        ",".join([
                    "campaign_name",
                    "campaign_id",
                    "spend",
                    "impressions",
                    "clicks",             # all clicks
                    "actions",            # includes landing_page_view, purchase
                    "cost_per_action_type",
                ]),
                "breakdowns":    "publisher_platform",
                "time_range":    f'{{"since":"{start_date}","until":"{end_date}"}}',
                "filtering":     '[{"field":"ad.effective_status","operator":"IN","value":["ACTIVE","PAUSED","ARCHIVED"]}]',
                "limit":         500,
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("data", [])

    # ── Processing ─────────────────────────────────────────────────────────────

    def _process(self, rows: list) -> dict:
        campaigns = {}
        total_spend_sgd = 0.0

        for row in rows:
            cname    = row.get("campaign_name", "")
            platform = row.get("publisher_platform", "unknown")  # facebook / instagram
            spend    = float(row.get("spend", 0))

            # Extract landing_page_view count from actions array
            actions = {a["action_type"]: int(a["value"]) for a in row.get("actions", [])}
            lp_views   = actions.get("landing_page_view", 0)
            purchases  = actions.get("offsite_conversion.fb_pixel_purchase", 0)
            impressions = int(row.get("impressions", 0))

            # Link to CPV campaign ID
            cpv_id = next((v for k, v in CAMPAIGN_TO_CPV.items() if k in cname), None)
            ckey   = row.get("campaign_id", cname)

            if ckey not in campaigns:
                campaigns[ckey] = {
                    "campaign_name": cname,
                    "cpv_campaign_id": cpv_id,
                    "platforms": {},
                    "totals": {"spend_sgd": 0.0, "impressions": 0, "lp_views": 0, "purchases": 0},
                }

            campaigns[ckey]["platforms"][platform] = {
                "spend_sgd":   round(spend, 2),
                "spend_usd":   round(spend * self.sgd_to_usd, 2),
                "impressions": impressions,
                "lp_views":    lp_views,
                "lp_ctr_pct":  round(lp_views / impressions * 100, 2) if impressions else 0,
                "purchases":   purchases,
                "cr_pct":      round(purchases / impressions * 100, 2) if impressions else 0,
                "cpa_sgd":     round(spend / purchases, 2) if purchases else None,
            }

            t = campaigns[ckey]["totals"]
            t["spend_sgd"]   += spend
            t["impressions"] += impressions
            t["lp_views"]    += lp_views
            t["purchases"]   += purchases
            total_spend_sgd  += spend

        # Round totals and add USD
        for c in campaigns.values():
            t = c["totals"]
            t["spend_sgd"]  = round(t["spend_sgd"], 2)
            t["spend_usd"]  = round(t["spend_sgd"] * self.sgd_to_usd, 2)
            t["lp_ctr_pct"] = round(t["lp_views"] / t["impressions"] * 100, 2) if t["impressions"] else 0
            t["cr_pct"]     = round(t["purchases"] / t["impressions"] * 100, 2) if t["impressions"] else 0

        return {
            "campaigns":       campaigns,
            "total_spend_sgd": round(total_spend_sgd, 2),
            "total_spend_usd": round(total_spend_sgd * self.sgd_to_usd, 2),
            "sgd_to_usd_rate": self.sgd_to_usd,
        }

    # ── Mock data ─────────────────────────────────────────────────────────────

    def _mock_data(self) -> dict:
        return {
            "campaigns": {
                "camp_78": {
                    "campaign_name": "Interactive VSL V1 (FB+IG)",
                    "cpv_campaign_id": "78",
                    "platforms": {
                        "facebook":  {"spend_sgd": 118.63, "spend_usd": 88.14, "impressions": 3200, "lp_views": 2,  "lp_ctr_pct": 2.00, "purchases": 1, "cr_pct": 1.00, "cpa_sgd": 118.63},
                        "instagram": {"spend_sgd": 34.37,  "spend_usd": 25.53, "impressions": 580,  "lp_views": 1,  "lp_ctr_pct": 5.56, "purchases": 1, "cr_pct": 5.56, "cpa_sgd": 34.37},
                    },
                    "totals": {"spend_sgd": 152.99, "spend_usd": 113.67, "impressions": 3780, "lp_views": 3, "lp_ctr_pct": 2.54, "purchases": 2, "cr_pct": 1.69},
                },
                "camp_88": {
                    "campaign_name": "Interactive VSL V2 (FB+IG)",
                    "cpv_campaign_id": "88",
                    "platforms": {
                        "facebook":  {"spend_sgd": 21.60, "spend_usd": 16.05, "impressions": 110, "lp_views": 1, "lp_ctr_pct": 9.09, "purchases": 1, "cr_pct": 9.09, "cpa_sgd": 21.60},
                        "instagram": {"spend_sgd": 6.31,  "spend_usd": 4.69,  "impressions": 18,  "lp_views": 0, "lp_ctr_pct": 0.00, "purchases": 0, "cr_pct": 0.00, "cpa_sgd": None},
                    },
                    "totals": {"spend_sgd": 31.40, "spend_usd": 23.33, "impressions": 128, "lp_views": 1, "lp_ctr_pct": 6.67, "purchases": 1, "cr_pct": 6.67},
                },
            },
            "total_spend_sgd": 184.39,
            "total_spend_usd": 136.98,
            "sgd_to_usd_rate": 0.743,
        }