"""
CPV Labs Collector — Astro Lover Sketch
Extends the base CPV Labs logic with landing-stats (per-URL funnel step views)
and offer-stats (checkout URL clicks per campaign).

landing-stats endpoint:
  GET /api/v2/stats/:campaign_id/landing-stats
  Returns rows per traffic segment. Each URL appears as multiple rows (one per
  tracking param combo). Two URL types appear per campaign:
    - Landing page rows  ("Landing Page" in Offer name): Views, Subscribers (optin completions)
    - Offer page rows    ("Offer Page"   in Offer name): Views, Clicks (checkout), Conversion (purchases)

Flow detection is fully dynamic — derived from actual URL paths in the Url field.
Flow key = "{landing_slug}→{offer_slug}" e.g. "c→offer_v4" or "c→offer_v4_v2".
No static variant config required. Split tests on the offer page are detected
automatically (multiple offer URLs per campaign → attributed proportionally).

offer-stats endpoint (kept for backward compat checkout_clicks_total):
  GET /api/v2/stats/:campaign_id/offer-stats
  Returns Clicks = ClickBank checkout URL clicks per campaign.
"""

import os
from collections import defaultdict
from .cpvlabs import CPVLabsCollector
from utils.config import get_cpv_group

MIN_VISITS = 5


class CPVLabsAstroCollector(CPVLabsCollector):
    """
    Extends CPVLabsCollector for astroloversketch.
    Adds landing-stats aggregation to produce per-variant funnel_totals.
    """

    def fetch(self, start_date: str, end_date: str) -> dict:
        if self.mock:
            return self._mock_data()

        if not self.api_key:
            raise Exception("CPVLABS_API_KEY is empty — set it in .env")
        if not self.base_url:
            raise Exception("CPVLABS_BASE_URL is empty — set it in .env")

        all_campaigns   = self._get_campaigns()
        CPV_GROUP       = get_cpv_group()
        group_campaigns = [c for c in all_campaigns if c.get("CampaignGroup") == CPV_GROUP]

        if not group_campaigns:
            available = list({c.get("CampaignGroup", "?") for c in all_campaigns})
            raise Exception(
                f"No campaigns found for group '{CPV_GROUP}'.\n"
                f"  Available groups: {available}\n"
                f"  Update CPVLABS_GROUP in .env to match exactly (case sensitive)"
            )

        ad_stats      = {}
        offer_stats   = {}
        landing_stats = {}   # cid → list of raw rows

        for camp in group_campaigns:
            cid   = str(camp.get("CampaignID", ""))
            cname = camp.get("CampaignName", f"Campaign {cid}")
            if not cid:
                continue

            # ── ad-stats (views, conversions, revenue) ────────────────────────
            try:
                perf = self._get_performance_stats(cid, start_date, end_date)
                if perf is not None:
                    ad_stats[cid] = {**perf, "_name": cname}
            except Exception as e:
                print(f"  [cpv_astro] Warning ad-stats {cid} ({cname}): {e}")

            # ── landing-stats (per-URL funnel step views) ─────────────────────
            try:
                rows = self._get_landing_stats(cid, start_date, end_date)
                if rows:
                    landing_stats[cid] = rows
            except Exception as e:
                print(f"  [cpv_astro] Warning landing-stats {cid} ({cname}): {e}")

            # ── offer-stats (checkout URL clicks, kept for total) ─────────────
            try:
                clicks = self._get_offer_clicks(cid, start_date, end_date)
                offer_stats[cid] = {"_name": cname, "checkout_clicks": clicks}
            except Exception as e:
                print(f"  [cpv_astro] Warning offer-stats {cid} ({cname}): {e}")
                offer_stats[cid] = {"_name": cname, "checkout_clicks": 0}

        return self._process_astro(ad_stats, offer_stats, landing_stats)

    # ── Landing-stats endpoint ────────────────────────────────────────────────

    def _get_landing_stats(self, campaign_id: str, start_date: str, end_date: str) -> list:
        """
        Calls /api/v2/stats/:campaign_id/landing-stats and returns raw rows.
        Each row represents one traffic-source segment for one landing/offer URL.
        Aggregate by URL to get total Views, Subscribers, Clicks, Conversion.
        """
        rows = self._get(
            f"/api/v2/stats/{campaign_id}/landing-stats",
            params={
                "interval":           "custom",
                "start_date":         start_date,
                "end_date":           end_date,
                "filter[start_date]": start_date,
                "filter[end_date]":   end_date,
                "filter[interval]":   "custom",
            },
        )
        if not rows or not isinstance(rows, list):
            return []
        return rows

    # ── Offer-stats endpoint (checkout clicks) ────────────────────────────────

    def _get_offer_clicks(self, campaign_id: str, start_date: str, end_date: str) -> int:
        rows = self._get(
            f"/api/v2/stats/{campaign_id}/offer-stats",
            params={
                "interval":           "custom",
                "start_date":         start_date,
                "end_date":           end_date,
                "filter[start_date]": start_date,
                "filter[end_date]":   end_date,
                "filter[interval]":   "custom",
            },
        )
        if not rows or not isinstance(rows, list):
            return 0
        return sum(int(r.get("Clicks", 0)) for r in rows)

    def _is_landing_row(self, offer_name: str) -> bool:
        return "landing page" in offer_name.lower()

    def _is_offer_row(self, offer_name: str) -> bool:
        return "offer page" in offer_name.lower()

    # ── Funnel totals builder ─────────────────────────────────────────────────

    def _build_funnel_totals(self, all_landing_rows: dict) -> dict:
        """
        Aggregate landing-stats rows into per-flow funnel metrics.

        Flow key = "{landing_slug}→{offer_slug}" derived from actual URL paths.
        Example: "/c/" + "/offer/v4/" → "c→offer_v4"

        If a campaign has multiple offer-page URLs (offer-page split test), landing
        metrics (views, optin completions) are attributed proportionally based on
        each offer page's share of total offer-page views for that campaign.

        Landing page rows:  Views = page views, Subscribers = optin form submissions
        Offer page rows:    Views = offer page views, Clicks = checkout clicks,
                            Conversion = purchases
        """
        from urllib.parse import urlparse

        # Step 1: per-campaign, aggregate by URL and classify row type
        per_campaign = {}
        for cid, rows in all_landing_rows.items():
            by_url = defaultdict(lambda: {
                "Views": 0, "Subscribers": 0, "Clicks": 0, "Conversion": 0,
                "Offer": "", "is_landing": False, "is_offer": False,
            })
            for row in rows:
                url  = row.get("Url", "?")
                name = row.get("Offer", "")
                by_url[url]["Views"]       += int(row.get("Views", 0))
                by_url[url]["Subscribers"] += int(row.get("Subscribers", 0))
                by_url[url]["Clicks"]      += int(row.get("Clicks", 0))
                by_url[url]["Conversion"]  += int(row.get("Conversion", 0))
                by_url[url]["Offer"]        = name
                if self._is_landing_row(name):
                    by_url[url]["is_landing"] = True
                elif self._is_offer_row(name):
                    by_url[url]["is_offer"] = True

            landings = {u: d for u, d in by_url.items() if d["is_landing"]}
            offers   = {u: d for u, d in by_url.items() if d["is_offer"]}
            if landings or offers:
                per_campaign[cid] = {"landings": landings, "offers": offers}

        # Step 2: build flow-keyed accumulators
        acc = {}

        for cid, data in per_campaign.items():
            landings = data["landings"]
            offers   = data["offers"]

            total_landing_views = sum(d["Views"]       for d in landings.values())
            total_optin_compls  = sum(d["Subscribers"] for d in landings.values())
            total_offer_views   = sum(d["Views"]        for d in offers.values())

            landing_paths = sorted(urlparse(u).path for u in landings)
            landing_slug  = "_".join(self._path_slug(p) for p in landing_paths) or "landing"

            if offers:
                for offer_url, offer_data in offers.items():
                    offer_path = urlparse(offer_url).path
                    offer_slug = self._path_slug(offer_path) or "offer"
                    flow_key   = f"{landing_slug}→{offer_slug}"

                    # Proportional attribution for split-test campaigns (multiple offer URLs)
                    if len(offers) > 1:
                        ratio              = offer_data["Views"] / total_offer_views if total_offer_views else 1 / len(offers)
                        attributed_landing = round(total_landing_views * ratio)
                        attributed_optins  = round(total_optin_compls  * ratio)
                    else:
                        attributed_landing = total_landing_views
                        attributed_optins  = total_optin_compls

                    if flow_key not in acc:
                        acc[flow_key] = {
                            "landing_paths":      landing_paths,
                            "offer_path":         offer_path,
                            "flow_label":         f"{' '.join(landing_paths)} → {offer_path}",
                            "landing_page_views": 0,
                            "optin_completions":  0,
                            "offer_page_views":   0,
                            "checkout_clicks":    0,
                            "purchases":          0,
                        }

                    acc[flow_key]["landing_page_views"] += attributed_landing
                    acc[flow_key]["optin_completions"]  += attributed_optins
                    acc[flow_key]["offer_page_views"]   += offer_data["Views"]
                    acc[flow_key]["checkout_clicks"]    += offer_data["Clicks"]
                    acc[flow_key]["purchases"]          += offer_data["Conversion"]

            elif landings:
                # Landing traffic with no matching offer rows (campaign still warming up)
                flow_key = f"{landing_slug}→?"
                if flow_key not in acc:
                    acc[flow_key] = {
                        "landing_paths": landing_paths, "offer_path": "unknown",
                        "flow_label": f"{' '.join(landing_paths)} → ?",
                        "landing_page_views": 0, "optin_completions": 0,
                        "offer_page_views": 0, "checkout_clicks": 0, "purchases": 0,
                    }
                acc[flow_key]["landing_page_views"] += total_landing_views
                acc[flow_key]["optin_completions"]  += total_optin_compls

        # Step 3: compute rates
        def rate(num, den):
            return round(num / den * 100, 2) if den else 0.0

        return {
            fk: {
                **v,
                "landing_to_optin_rate":     rate(v["optin_completions"], v["landing_page_views"]),
                "offer_to_checkout_rate":    rate(v["checkout_clicks"],   v["offer_page_views"]),
                "checkout_to_purchase_rate": rate(v["purchases"],         v["checkout_clicks"]),
                "overall_rate":              rate(v["purchases"],         v["landing_page_views"]),
            }
            for fk, v in acc.items()
        }

    @staticmethod
    def _path_slug(path: str) -> str:
        """Convert a URL path to a compact slug: /offer/v4/ → offer_v4"""
        return path.strip("/").replace("/", "_").replace("-", "_")

    # ── Processing ────────────────────────────────────────────────────────────

    def _process_astro(self, ad_stats: dict, offer_stats: dict, landing_stats: dict) -> dict:
        """
        Merges ad-stats, offer-stats, and landing-stats into the final payload.
        """
        # Base: campaign views/conversions/revenue from ad-stats
        base = self._process(ad_stats)

        # Inject checkout_clicks per campaign (from offer-stats)
        total_checkout_clicks      = 0
        checkout_clicks_by_campaign = {}

        for cid, offer in offer_stats.items():
            clicks = offer.get("checkout_clicks", 0)
            checkout_clicks_by_campaign[cid] = {
                "label":           offer.get("_name", f"Campaign {cid}"),
                "checkout_clicks": clicks,
            }
            total_checkout_clicks += clicks
            if cid in base["campaigns"]:
                base["campaigns"][cid]["checkout_clicks"] = clicks

        base["checkout_clicks_total"]        = total_checkout_clicks
        base["checkout_clicks_by_campaign"]  = checkout_clicks_by_campaign

        # Funnel totals from landing-stats (primary funnel step data)
        base["funnel_totals"] = self._build_funnel_totals(landing_stats)

        return base

    # ── Mock ─────────────────────────────────────────────────────────────────

    def _mock_data(self) -> dict:
        return {
            "campaigns": {
                "82": {
                    "label":           "astroloversketch.com - Organic Traffic",
                    "source":          "unknown",
                    "views":           2967,
                    "conversions":     7,
                    "revenue":         68.14,
                    "cr_pct":          0.24,
                    "status":          "weak",
                    "checkout_clicks": 190,
                },
                "101": {
                    "label":           "astroloversketch.com - Maropost Traffic - Variant C - Offer V4 - OTO TSL",
                    "source":          "email",
                    "views":           7467,
                    "conversions":     88,
                    "revenue":         6874.25,
                    "cr_pct":          1.18,
                    "status":          "active",
                    "checkout_clicks": 1223,
                },
            },
            "totals_by_source": {
                "unknown": {"views": 2967,  "conversions": 7,  "revenue": 68.14},
                "email":   {"views": 7467,  "conversions": 88, "revenue": 6874.25},
            },
            "total_conversions": 95,
            "total_revenue":     6942.39,
            "checkout_clicks_total": 1413,
            "checkout_clicks_by_campaign": {
                "82":  {"label": "Organic Traffic",             "checkout_clicks": 190},
                "101": {"label": "Variant C - Offer V4 - OTO",  "checkout_clicks": 1223},
            },
            "funnel_totals": {
                "c→offer_v4": {
                    "landing_paths":      ["/c/"],
                    "offer_path":         "/offer/v4/",
                    "flow_label":         "/c/ → /offer/v4/",
                    "landing_page_views": 10658,
                    "optin_completions":   1733,
                    "offer_page_views":    1734,
                    "checkout_clicks":     1448,
                    "purchases":            107,
                    "landing_to_optin_rate":     16.26,
                    "offer_to_checkout_rate":    83.51,
                    "checkout_to_purchase_rate":  7.39,
                    "overall_rate":               1.00,
                },
                "a_b→offer_v2": {
                    "landing_paths":      ["/a/", "/b/"],
                    "offer_path":         "/offer/v2/",
                    "flow_label":         "/a/ /b/ → /offer/v2/",
                    "landing_page_views":  270,
                    "optin_completions":    24,
                    "offer_page_views":     20,
                    "checkout_clicks":      12,
                    "purchases":             3,
                    "landing_to_optin_rate":     8.89,
                    "offer_to_checkout_rate":   60.0,
                    "checkout_to_purchase_rate": 25.0,
                    "overall_rate":              1.11,
                },
            },
        }
