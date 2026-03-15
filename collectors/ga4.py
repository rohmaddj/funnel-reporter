"""
GA4 Collector
Pulls front-end funnel event data per variant using Google Analytics Data API.

Variants:
  /destiny    = CPV campaigns 77, 78  (full-screen full-screen)
  /destiny/v2 = CPV campaigns 87, 88  (page version with video)

Auth: Same Google service account as Sheets/Docs
  GA4 → Admin → Property Access Management → Add service account email as Viewer

Setup:
  GA4_PROPERTY_ID=123456789  (just the number from GA4 Admin → Property Settings)
"""

import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from .base import BaseCollector


SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]

def _build_variants() -> dict:
    from utils.config import get_variants, get_campaign_map
    variant_defs = get_variants()
    camp_map     = get_campaign_map()

    cpv_ids_by_variant = {}
    for cid, data in camp_map.items():
        v = data.get("variant", "other")
        cpv_ids_by_variant.setdefault(v, []).append(cid)

    result = {}
    for key, vdef in variant_defs.items():
        if vdef.get("path"):
            result[key] = {
                "label":   vdef["label"],
                "path":    vdef["path"],
                "exclude": vdef.get("exclude"),
                "cpv_ids": cpv_ids_by_variant.get(key, []),
            }
    return result

# Built lazily per instance — not at module load time

FUNNEL_EVENTS = [
    "choice_selected",
    "optin_step_1_completed",
    "optin_step_2_completed",
    "optin_step_3_completed",
    "optin_flow_completed",
]


class GA4Collector(BaseCollector):

    def __init__(self, mock=False):
        super().__init__(mock=mock)
        self.property_id    = os.environ.get("GA4_PROPERTY_ID", "")
        self.credentials_file = os.environ.get("GOOGLE_CREDENTIALS_FILE", "google_credentials.json")
        self.variants = _build_variants()

    def fetch(self, start_date: str, end_date: str) -> dict:
        if self.mock:
            return self._mock_data()

        if not self.property_id:
            raise Exception(
                "GA4_PROPERTY_ID not set — add to .env\n"
                "  Find it: GA4 → Admin → Property Settings → Property ID"
            )

        service = self._get_service()
        results = {}

        for key, variant in self.variants.items():
            try:
                data = self._fetch_variant(service, variant, start_date, end_date)
                results[key] = {"label": variant["label"], "cpv_ids": variant["cpv_ids"], **data}
            except Exception as e:
                print(f"  [ga4] Warning {key}: {e}")
                results[key] = {"label": variant["label"], "cpv_ids": variant["cpv_ids"], "error": str(e)}

        return results

    # ── Auth ──────────────────────────────────────────────────────────────────

    def _get_service(self):
        creds = service_account.Credentials.from_service_account_file(
            self.credentials_file, scopes=SCOPES
        )
        return build("analyticsdata", "v1beta", credentials=creds, cache_discovery=False)

    # ── Per-variant fetch ─────────────────────────────────────────────────────

    def _fetch_variant(self, service, variant: dict, start_date: str, end_date: str) -> dict:
        prop          = f"properties/{self.property_id}"
        path_filter   = self._path_filter(variant["path"], variant.get("exclude"))
        date_range    = [{"startDate": start_date, "endDate": end_date}]

        # ── Sessions + engagement ─────────────────────────────────────────────
        s = service.properties().runReport(
            property=prop,
            body={
                "dateRanges": date_range,
                "metrics": [
                    {"name": "sessions"},
                    {"name": "totalUsers"},
                    {"name": "averageSessionDuration"},
                    {"name": "bounceRate"},
                ],
                "dimensionFilter": path_filter,
            }
        ).execute()

        sessions     = self._val(s, 0)
        total_users  = self._val(s, 1)
        avg_duration = round(self._fval(s, 2), 1)
        bounce_rate  = round(self._fval(s, 3) * 100, 1)

        # ── Funnel events ─────────────────────────────────────────────────────
        e = service.properties().runReport(
            property=prop,
            body={
                "dateRanges": date_range,
                "dimensions": [{"name": "eventName"}],
                "metrics":    [{"name": "activeUsers"}],
                "dimensionFilter": {
                    "andGroup": {
                        "expressions": [
                            path_filter,
                            {"filter": {"fieldName": "eventName", "inListFilter": {"values": FUNNEL_EVENTS}}}
                        ]
                    }
                },
            }
        ).execute()

        event_counts = {row["dimensionValues"][0]["value"]: int(row["metricValues"][0]["value"])
                        for row in e.get("rows", [])}

        # ── Checkout events (prefix match) ────────────────────────────────────
        c = service.properties().runReport(
            property=prop,
            body={
                "dateRanges": date_range,
                "dimensions": [{"name": "eventName"}],
                "metrics":    [{"name": "activeUsers"}],
                "dimensionFilter": {
                    "andGroup": {
                        "expressions": [
                            path_filter,
                            {"orGroup": {"expressions": [
                                {"filter": {"fieldName": "eventName", "stringFilter": {"matchType": "ENDS_WITH", "value": "_clicked"}}},
                            ]}}
                        ]
                    }
                },
            }
        ).execute()

        checkout_basic    = sum(int(row["metricValues"][0]["value"]) for row in c.get("rows", []) if row["dimensionValues"][0]["value"] == "checkout_basic_clicked")
        checkout_advanced = sum(int(row["metricValues"][0]["value"]) for row in c.get("rows", []) if row["dimensionValues"][0]["value"] == "checkout_advanced_clicked")
        checkout_total    = checkout_basic + checkout_advanced

        # ── Derived rates ─────────────────────────────────────────────────────
        def rate(num, den):
            return round(num / den * 100, 2) if den else 0

        choice_selected      = event_counts.get("choice_selected", 0)
        optin_s1             = event_counts.get("optin_step_1_completed", 0)
        optin_s2             = event_counts.get("optin_step_2_completed", 0)
        optin_s3             = event_counts.get("optin_step_3_completed", 0)
        optin_completed      = event_counts.get("optin_flow_completed", 0)

        return {
            "sessions":           sessions,
            "total_users":        total_users,
            "avg_session_sec":    avg_duration,
            "bounce_rate_pct":    bounce_rate,
            "choice_selected":    choice_selected,
            "choice_rate_pct":    rate(choice_selected, sessions),
            "optin_step1":        optin_s1,
            "optin_step2":        optin_s2,
            "optin_step3":        optin_s3,
            "optin_completed":    optin_completed,
            "optin_rate_pct":     rate(optin_completed, sessions),
            "checkout_basic":     checkout_basic,
            "checkout_advanced":  checkout_advanced,
            "checkout_total":     checkout_total,
            "checkout_rate_pct":  rate(checkout_total, sessions),
            "checkout_from_optin_pct": rate(checkout_total, optin_completed),  # can exceed 100% if users click multiple checkout buttons
            # Step-to-step drop-off rates
            "dropoff": {
                "session_to_choice":  rate(choice_selected, sessions),
                "choice_to_optin1":   rate(optin_s1, choice_selected),
                "optin1_to_optin2":   rate(optin_s2, optin_s1),
                "optin2_to_optin3":   rate(optin_s3, optin_s2),
                "optin3_to_complete": rate(optin_completed, optin_s3),
                "complete_to_checkout": min(rate(checkout_total, optin_completed), 100.0),  # capped at 100% for display
            },
        }

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _path_filter(self, path: str, exclude: str = None) -> dict:
        base = {"filter": {"fieldName": "pagePath", "stringFilter": {"matchType": "BEGINS_WITH", "value": path, "caseSensitive": False}}}
        if not exclude:
            return base
        return {
            "andGroup": {
                "expressions": [
                    base,
                    {"notExpression": {"filter": {"fieldName": "pagePath", "stringFilter": {"matchType": "BEGINS_WITH", "value": exclude, "caseSensitive": False}}}}
                ]
            }
        }

    def _val(self, resp, idx):
        try:
            return int(float(resp["rows"][0]["metricValues"][idx]["value"]))
        except (KeyError, IndexError):
            return 0

    def _fval(self, resp, idx):
        try:
            return float(resp["rows"][0]["metricValues"][idx]["value"])
        except (KeyError, IndexError):
            return 0.0

    # ── Mock ──────────────────────────────────────────────────────────────────

    def _mock_data(self) -> dict:
        return {
            "destiny_v1": {
                "label": "/destiny (full-screen)", "cpv_ids": ["77","78"],
                "sessions": 2840, "total_users": 2610, "avg_session_sec": 187.4, "bounce_rate_pct": 42.1,
                "choice_selected": 1240, "choice_rate_pct": 43.7,
                "optin_step1": 890, "optin_step2": 720, "optin_step3": 580, "optin_completed": 490,
                "optin_rate_pct": 17.3,
                "checkout_basic": 142, "checkout_advanced": 198, "checkout_total": 340,
                "checkout_rate_pct": 12.0, "checkout_from_optin_pct": 69.4,
                "dropoff": {"session_to_choice": 43.7, "choice_to_optin1": 71.8, "optin1_to_optin2": 80.9, "optin2_to_optin3": 80.6, "optin3_to_complete": 84.5, "complete_to_checkout": 69.4},
            },
            "destiny_v2": {
                "label": "/destiny/v2 (with-headline)", "cpv_ids": ["87","88"],
                "sessions": 3920, "total_users": 3540, "avg_session_sec": 214.8, "bounce_rate_pct": 38.6,
                "choice_selected": 1980, "choice_rate_pct": 50.5,
                "optin_step1": 1420, "optin_step2": 1180, "optin_step3": 940, "optin_completed": 810,
                "optin_rate_pct": 20.7,
                "checkout_basic": 198, "checkout_advanced": 312, "checkout_total": 510,
                "checkout_rate_pct": 13.0, "checkout_from_optin_pct": 63.0,
                "dropoff": {"session_to_choice": 50.5, "choice_to_optin1": 71.7, "optin1_to_optin2": 83.1, "optin2_to_optin3": 79.7, "optin3_to_complete": 86.2, "complete_to_checkout": 63.0},
            },
        }