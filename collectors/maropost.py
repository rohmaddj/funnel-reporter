"""
Maropost Collector
Fetches email campaign send/click stats.

Docs: https://developers.maropost.com/documentation/
Auth: API Key + Account ID in headers

NOTE ON STRATEGY:
Since CPV Labs is already capturing every click from your email campaigns
via tracked links, you have two options:

  Option A (recommended): Skip Maropost entirely and use CPV data for email.
    CPV views = email link clicks (what matters for conversion tracking).
    Set USE_CPV_FOR_EMAIL = True below.

  Option B: Pull from Maropost for list-level metrics (sends, open rate,
    click rate) and use CPV for conversion data. Useful if you want to
    track email deliverability separately.

The mock data below mirrors Option B output.
"""

import os
import requests
from .base import BaseCollector


# Set to True to skip this collector (CPV handles email attribution)
USE_CPV_FOR_EMAIL = os.environ.get("USE_CPV_FOR_EMAIL", "true").lower() == "true"


class MaropostCollector(BaseCollector):

    BASE_URL = "https://api.maropost.com/accounts"

    def __init__(self, mock=False):
        super().__init__(mock=mock)
        self.api_key    = os.environ.get("MAROPOST_API_KEY", "")
        self.account_id = os.environ.get("MAROPOST_ACCOUNT_ID", "")

    def fetch(self, start_date: str, end_date: str) -> dict:
        if USE_CPV_FOR_EMAIL:
            # Return a stub — the funnel_snapshot builder will pull email
            # totals from CPV data instead. This avoids double-counting.
            return {
                "note": "Email data sourced from CPV Labs (USE_CPV_FOR_EMAIL=true)",
                "total_views":       None,
                "total_conversions": None,
                "total_revenue":     None,
                "campaigns":         {},
            }

        if self.mock:
            return self._mock_data()

        campaigns = self._get_campaign_reports(start_date, end_date)
        return self._process(campaigns)

    # ── API calls ─────────────────────────────────────────────────────────────

    def _get_campaign_reports(self, start_date: str, end_date: str) -> list:
        """
        Maropost uses a reports endpoint per campaign, so we first list
        campaigns sent in the range, then fetch stats per campaign.
        """
        # 1. List campaigns
        list_resp = requests.get(
            f"{self.BASE_URL}/{self.account_id}/campaigns.json",
            params={
                "api_key":    self.api_key,
                "sent_after": start_date,
                "sent_before": end_date,
            },
            timeout=30,
        )
        list_resp.raise_for_status()
        campaigns = list_resp.json()

        # 2. Fetch report for each campaign
        results = []
        for camp in campaigns:
            cid = camp.get("id")
            try:
                rpt = requests.get(
                    f"{self.BASE_URL}/{self.account_id}/campaigns/{cid}/report.json",
                    params={"api_key": self.api_key},
                    timeout=30,
                )
                rpt.raise_for_status()
                results.append({**camp, "report": rpt.json()})
            except Exception:
                results.append({**camp, "report": None})

        return results

    # ── Processing ─────────────────────────────────────────────────────────────

    def _process(self, campaigns: list) -> dict:
        processed = {}
        total_sends   = 0
        total_opens   = 0
        total_clicks  = 0

        for c in campaigns:
            report = c.get("report") or {}
            name   = c.get("name", "Unknown")
            cid    = str(c.get("id", ""))

            sends  = int(report.get("total_sent", 0))
            opens  = int(report.get("unique_opens", 0))
            clicks = int(report.get("unique_clicks", 0))

            processed[cid] = {
                "label":        name,
                "sends":        sends,
                "opens":        opens,
                "clicks":       clicks,
                "open_rate":    round(opens  / sends * 100, 2) if sends else 0,
                "click_rate":   round(clicks / sends * 100, 2) if sends else 0,
            }
            total_sends  += sends
            total_opens  += opens
            total_clicks += clicks

        return {
            "total_sends":  total_sends,
            "total_opens":  total_opens,
            "total_clicks": total_clicks,   # unique clicks — same as CPV views approximately
            "campaigns":    processed,
        }

    # ── Mock data ─────────────────────────────────────────────────────────────

    def _mock_data(self) -> dict:
        return {
            "total_sends":  18400,
            "total_opens":  3100,
            "total_clicks": 4822,  # should match CPV total email views
            "campaigns": {
                "camp_vsl2":  {"label": "Interactive VSL V2",  "sends": 9200,  "opens": 1540, "clicks": 3822, "open_rate": 16.7, "click_rate": 41.5},
                "camp_vsl1":  {"label": "Interactive VSL V1",  "sends": 3800,  "opens": 510,  "clicks": 759,  "open_rate": 13.4, "click_rate": 20.0},
                "camp_aband": {"label": "Abandoned",           "sends": 620,   "opens": 88,   "clicks": 47,   "open_rate": 14.2, "click_rate": 7.6},
                "camp_react": {"label": "Reactivation",        "sends": 750,   "opens": 92,   "clicks": 58,   "open_rate": 12.3, "click_rate": 7.7},
                "camp_disc":  {"label": "Discount",            "sends": 400,   "opens": 48,   "clicks": 18,   "open_rate": 12.0, "click_rate": 4.5},
            },
        }