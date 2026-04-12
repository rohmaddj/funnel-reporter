"""
GA4 Collector — Astro Lover Sketch
Uses custom GA4 events (not page_view proxies) to measure optin page views.

Custom events tracked on astroloversketch.com:
  view_landing_page  → someone lands on a landing page variant
  view_opt_in_page   → someone sees the opt-in form page
  lead_submit        → someone submits the opt-in form (= optin completion)
  view_offer_page    → someone lands on the offer/sales page

These events were added after April 2026. For older reporting periods they
will return 0 — fall back to CPV landing-stats data in that case.

CPV landing-stats is the PRIMARY source for all funnel step volumes.
GA4 is a SUPPLEMENT used only for optin_page_views (the step between
landing and form submission that CPV doesn't track separately).

Funnel path mapping (updated to match actual site URLs):
  funnel_v4: /c/  → /opt-in-v4  → /offer/v4/
  funnel_v2: /a/  → /opt-in-v2  → /offer/v2/
             /b/  → /opt-in-v2  → /offer/v2/
"""

import os
from google.oauth2 import service_account
from googleapiclient.discovery import build

from .base import BaseCollector
from utils.config import load_config

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]


class GA4AstroCollector(BaseCollector):

    def __init__(self, mock=False):
        super().__init__(mock=mock)
        self.property_id      = os.environ.get("GA4_PROPERTY_ID", "")
        self.credentials_file = os.environ.get("GOOGLE_CREDENTIALS_FILE", "google_credentials.json")
        cfg                   = load_config()
        self.variants         = cfg.get("variants", {})

    def fetch(self, start_date: str, end_date: str) -> dict:
        if self.mock:
            return self._mock_data()

        if not self.property_id:
            raise Exception(
                "GA4_PROPERTY_ID not set — add ASTRO_GA4_PROPERTY_ID to .env"
            )

        service    = self._get_service()
        date_range = [{"startDate": start_date, "endDate": end_date}]
        results    = {}

        for key, variant in self.variants.items():
            if not variant.get("homepage_paths"):
                continue
            try:
                results[key] = self._fetch_variant(service, key, variant, date_range)
            except Exception as e:
                print(f"  [ga4_astro] Warning {key}: {e}")
                results[key] = {"label": variant.get("label", key), "error": str(e)}

        return results

    # ── Auth ──────────────────────────────────────────────────────────────────

    def _get_service(self):
        creds = service_account.Credentials.from_service_account_file(
            self.credentials_file, scopes=SCOPES
        )
        return build("analyticsdata", "v1beta", credentials=creds, cache_discovery=False)

    # ── Per-variant fetch ─────────────────────────────────────────────────────

    def _fetch_variant(self, service, key: str, variant: dict, date_range: list) -> dict:
        prop           = f"properties/{self.property_id}"
        label          = variant.get("label", key)
        homepage_paths = variant["homepage_paths"]    # e.g. ["/c/"]
        optin_path     = variant["optin_path"]         # e.g. "/opt-in-v4"
        offer_path     = variant["offer_path"]         # e.g. "/offer/v4"

        # Custom events (added Apr 2026 — returns 0 for older periods)
        landing_views  = self._event_on_paths(service, prop, date_range, "view_landing_page",  homepage_paths)
        optin_pv       = self._event_on_paths(service, prop, date_range, "view_opt_in_page",   [optin_path])
        lead_submits   = self._event_on_paths(service, prop, date_range, "lead_submit",        [optin_path])
        offer_pv       = self._event_on_paths(service, prop, date_range, "view_offer_page",    [offer_path])

        # Fallback: page_view on landing paths (always available, less precise)
        page_view_landing = self._page_views(service, prop, date_range, homepage_paths)

        def rate(num, den):
            return round(num / den * 100, 2) if den else 0.0

        # Use custom events when available, page_view as fallback
        effective_landing = landing_views if landing_views > 0 else page_view_landing

        return {
            "label":                     label,
            # Custom event counts (primary)
            "landing_page_views":        effective_landing,
            "optin_page_views":          optin_pv,       # unique GA4 metric (CPV doesn't have this)
            "lead_submit_count":         lead_submits,
            "offer_page_views":          offer_pv,
            # Rates (only meaningful when GA4 events are active)
            "landing_to_optin_page_pct": rate(optin_pv,    effective_landing),
            "optin_page_to_submit_pct":  rate(lead_submits, optin_pv),
            "has_custom_events":         landing_views > 0 or optin_pv > 0,
        }

    # ── GA4 query helpers ─────────────────────────────────────────────────────

    def _event_on_paths(self, service, prop: str, date_range: list,
                        event_name: str, paths: list) -> int:
        """Count a specific event across one or more page path prefixes."""
        total = 0
        for path in paths:
            resp = service.properties().runReport(
                property=prop,
                body={
                    "dateRanges": date_range,
                    "metrics":    [{"name": "eventCount"}],
                    "dimensionFilter": {
                        "andGroup": {"expressions": [
                            {"filter": {"fieldName": "eventName",
                                        "stringFilter": {"matchType": "EXACT", "value": event_name}}},
                            {"filter": {"fieldName": "pagePath",
                                        "stringFilter": {"matchType": "BEGINS_WITH",
                                                         "value": path,
                                                         "caseSensitive": False}}},
                        ]}
                    },
                }
            ).execute()
            total += self._int_val(resp)
        return total

    def _page_views(self, service, prop: str, date_range: list, paths: list) -> int:
        """Sum page_view events across path prefixes (fallback for older periods)."""
        total = 0
        for path in paths:
            resp = service.properties().runReport(
                property=prop,
                body={
                    "dateRanges": date_range,
                    "metrics":    [{"name": "eventCount"}],
                    "dimensionFilter": {
                        "andGroup": {"expressions": [
                            {"filter": {"fieldName": "eventName",
                                        "stringFilter": {"matchType": "EXACT", "value": "page_view"}}},
                            {"filter": {"fieldName": "pagePath",
                                        "stringFilter": {"matchType": "BEGINS_WITH",
                                                         "value": path,
                                                         "caseSensitive": False}}},
                        ]}
                    },
                }
            ).execute()
            total += self._int_val(resp)
        return total

    def _int_val(self, resp: dict) -> int:
        try:
            return int(float(resp["rows"][0]["metricValues"][0]["value"]))
        except (KeyError, IndexError):
            return 0

    # ── Mock ──────────────────────────────────────────────────────────────────

    def _mock_data(self) -> dict:
        return {
            "funnel_v4": {
                "label":                     "Funnel v4 (/c/)",
                "landing_page_views":        10658,
                "optin_page_views":          3200,    # GA4 view_opt_in_page
                "lead_submit_count":         1733,
                "offer_page_views":          1734,
                "landing_to_optin_page_pct": 30.03,
                "optin_page_to_submit_pct":  54.16,
                "has_custom_events":         True,
            },
            "funnel_v2": {
                "label":                     "Funnel v2 (/a/ /b/)",
                "landing_page_views":        270,
                "optin_page_views":          110,
                "lead_submit_count":         24,
                "offer_page_views":          20,
                "landing_to_optin_page_pct": 40.74,
                "optin_page_to_submit_pct":  21.82,
                "has_custom_events":         True,
            },
        }
