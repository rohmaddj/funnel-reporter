"""
AI Analysis Layer
Sends collected funnel data to Claude API and returns structured insights.

Usage:
    python analyse.py                          # reads output/report_data.json
    python analyse.py --input output/report_data.json
    python analyse.py --mock                   # uses mock JSON, skips API call

Output: output/analysis.json
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL = "claude-sonnet-4-5"
MAX_TOKENS = 6000


def parse_args():
    parser = argparse.ArgumentParser(description="Run AI analysis on collected funnel data")
    parser.add_argument("--input",  default="")
    parser.add_argument("--out",    default="",    help="Output path (default: output/analysis_YYYY_MM_DD.json)")
    parser.add_argument("--project", type=str, default="", help="Project: asksabrina or astroloversketch")
    parser.add_argument("--mock",   action="store_true",               help="Skip API call, return mock analysis")
    return parser.parse_args()


# ── Prompt ────────────────────────────────────────────────────────────────────

def build_system_prompt(data: dict) -> str:
    from utils.config import load_config

    try:
        cfg = load_config()
        project_meta = cfg.get("project", {})
        project_name = project_meta.get("name", data.get("meta", {}).get("project", "Ask Sabrina"))
        funnel_type = project_meta.get("funnel_type", "VSL")
        paid_platform_label = project_meta.get("paid_platform_label", "Facebook + Instagram")
    except Exception:
        project_name = data.get("meta", {}).get("project", "Ask Sabrina")
        funnel_type = "VSL"
        paid_platform_label = "Facebook + Instagram"

    skus = data.get("funnel_backend", {}).get("sku_breakdown", {})
    stage_order = ["frontend", "order_bump", "oto1", "oto1_downsell", "oto2", "oto2_downsell"]
    sorted_skus = sorted(
        skus.items(),
        key=lambda x: stage_order.index(x[1]["stage"]) if x[1].get("stage") in stage_order else 99
    )
    funnel_lines = [f"  → {s.get('label', k)} ${s.get('price', 0)}" for k, s in sorted_skus]
    funnel_str = "\n".join(funnel_lines) if funnel_lines else "  See funnel_backend data"

    variants = data.get("funnel_variants") or {}
    variant_lines = []
    for vkey, vdata in variants.items():
        if isinstance(vdata, dict):
            cpv_ids = ", ".join(vdata.get("cpv_ids", [])) if vdata.get("cpv_ids") else "n/a"
            label = vdata.get("label", vkey)
            variant_lines.append(f"  {label} = CPV campaigns {cpv_ids}")
    variant_str = "\n".join(variant_lines) if variant_lines else "  No variant data present"

    snap = data.get("funnel_snapshot", {})
    avg_value = snap.get("avg_revenue_per_buyer_usd", 0) or 0
    cpa_target = round(avg_value * 0.5, 2) if avg_value else 80.0

    return f"""You are a direct-response funnel analyst for {project_name}, a {funnel_type} funnel.

Your job is to produce a weekly commercial funnel brief focused on:
1. What is driving or hurting revenue right now
2. Which actions are most likely to move growth next
3. Which findings are supported directly by tracked data vs inference

Funnel structure (keep this in mind):
  Traffic → Landing Page → {funnel_type}
{funnel_str}

Variant context:
{variant_str}

Traffic context:
- Email traffic is warm and typically higher intent
- {paid_platform_label} traffic is colder and should be judged on CTR, conversion efficiency, and CPA
- CPV Labs tracks ClickBank postbacks, so CPV "conversions" can exceed unique frontend buyers
- ClickBank / funnel_backend is the source of truth for actual buyer count and backend stage revenue
- GA4 funnel_variants is the source of truth for session-based variant progression
- Do NOT invent page-level heatmap, scroll-depth, or click-map findings unless that data explicitly exists in the payload

Commercial benchmarks to use carefully, not blindly:
- Healthy cold LP CTR: ~4–6%
- Healthy high-volume email CR: ~0.3–0.5%
- Strong OTO1 take rate: 40%+
- OTO2 under 10% is weak and worth attention
- Paid CPA target: below ${cpa_target:.2f} USD unless backend economics clearly justify higher
- RPV is a commercial efficiency lens, not the only decision-maker

CRITICAL metric rules:
- Use funnel_backend / ClickBank numbers for true frontend sales, true backend sales, revenue, refunds, take rates
- Use traffic_sources.tracking for source-level views, conversions, and revenue by CPV tracking bucket
- Use traffic_sources.paid for paid spend, platform impressions, LP views, LP CTR, and platform purchase proxies
- Never mix sessions, views, LP views, platform purchases, CPV conversions, and ClickBank frontend sales as if they are the same thing
- If a metric uses one source and denominator, keep it consistent and label it clearly
- If the data only supports a proxy metric, say it is a proxy

CRITICAL RPV rules:
- email_rpv = tracking email revenue / tracking email views
- paid_rpv_usd = tracking facebook revenue / tracking facebook views when facebook source totals exist in tracking
- If paid tracking totals are missing or unusable, return null and explain why
- Do NOT calculate paid RPV from spend, sessions, or LP views unless explicitly labeled as a different metric

CRITICAL CPA rules:
- Prefer paid CPA based on paid spend / paid attributed conversions from tracking if available
- If only platform purchases are available for a specific paid campaign, you may use that as a proxy, but label it as proxy_cpa
- Do not present proxy CPA as if it were exact buyer CPA

CRITICAL variant rules:
- variant_sales in funnel_backend understates true revenue by variant because unattributed / organic sales fall into "other"
- Do NOT use variant_sales revenue or variant_sales count as the sole basis to declare a winner
- Primary variant signals are:
  1. checkout_total / sessions
  2. checkout_from_optin_pct
  3. bounce_rate_pct
  4. major drop-off steps
- variant_sales may be mentioned only as secondary supporting context, with attribution caveat
- If session-based funnel signals and attributed sales signals disagree, classify the variant read as "mixed"
- Never say a variant is broken or losing unless multiple important signals are weak at the same time
- Never contradict yourself inside the same recommendation

CRITICAL reasoning rules:
- Separate findings into:
  - observed = directly supported by the data
  - inferred = a reasonable conclusion from multiple tracked signals
  - hypothesis = plausible but not proven from this dataset
- Needle movers must be actions that are practical, specific, and applicable from the current data
- Do not include trivial actions with tiny projected impact unless they stop active spend waste
- Prioritize:
  1. active revenue leaks
  2. paid spend waste
  3. scalable winners
  4. backend monetization gaps
  5. mixed-signal areas that deserve controlled testing
- When platform purchase counts are used for paid campaign efficiency, treat them as proxy signals, not source-of-truth buyer counts
- Do not use proxy purchase counts as the main headline unless no better attributed paid-buyer view exists
- For small-sample campaigns (fewer than 20 views or fewer than 2 conversions), prefer "protect" or "test scale" over aggressive "scale"
- Revenue impact estimates should be framed as directional unless directly supported by the underlying funnel math
- Prefer downstream efficiency metrics that match the funnel stage being discussed
- Do not frame impressions-to-purchase as a core funnel conversion metric unless clearly labeled as a media proxy
- Do not imply exact UX friction (e.g. headline issue, checkout friction, VSL drop-off cause) unless the dataset directly isolates that step
- For score-style judgments, compare the observed metric against the benchmark named in this prompt
- Do not mark a step as green/protect when it is materially below its stated benchmark, even if it is functional
- For OTO1 specifically: below 30% should not be green, and below the 40% strong benchmark should usually be yellow unless there is a strong reason otherwise
- For OTO2 specifically: below 10% should be red or yellow depending on severity, not green
- A monetization step can be functioning and still be underperforming; treat that as yellow/test or red/fix depending on the size of the gap
- When unsure between green and yellow for a below-benchmark monetization step, choose yellow
- Check arithmetic carefully before writing summary lines, especially campaign counts, total views, and combined totals
- When comparing bounce rate, lower bounce is better; do not describe a higher bounce rate as lower
- For variant comparison, explicitly verify directional statements against the numeric values before finalizing the explanation

CRITICAL output rules:
- Output valid JSON only
- No markdown
- No prose outside the JSON
- No comments
- No trailing commas
"""


def build_user_prompt(data: dict) -> str:
    ts = data.get("traffic_sources", {}) or {}
    tracking = ts.get("tracking") or {}
    paid     = ts.get("paid")     or {}
    slim = {
        "period": f"{data['meta']['period_start']} to {data['meta']['period_end']}",
        "funnel_snapshot": data.get("funnel_snapshot"),
        "email_campaigns": tracking.get("campaigns"),
        "paid_campaigns": paid.get("campaigns"),
        "tracking_totals_by_source": tracking.get("totals_by_source"),
        "cpv_funnel_totals": tracking.get("funnel_totals"),
        "funnel_backend": data.get("funnel_backend"),
        "cross_check": data.get("cross_check"),
        "funnel_variants": data.get("funnel_variants"),
    }

    return f"""Here is this week's tracked funnel performance data:

{json.dumps(slim, indent=2)}

Analyse this data and return a JSON object with exactly this structure:

{{
  "period_summary": "2–3 sentence plain-English summary of the week. Include total revenue, frontend sales, and the single most important commercial takeaway.",

  "northstar": {{
    "operating_focus": "tracked funnel performance and revenue efficiency",
    "commercial_metric": "Revenue Per Visitor (RPV)",
    "email_rpv": <float or null>,
    "paid_rpv_usd": <float or null>,
    "avg_funnel_value": <float or null>,
    "commentary": "1 sentence explaining what these efficiency numbers mean right now."
  }},

  "funnel_leaks": [
    {{
      "area": "short label",
      "metric": "the specific metric showing the leak",
      "value": "actual figure",
      "why_it_matters": "1 sentence"
    }}
  ],

  "needle_movers": [
    {{
      "rank": 1,
      "priority": "critical | high | medium",
      "area": "short label",
      "evidence_type": "observed | inferred | hypothesis",
      "confidence": "high | medium | low",
      "why": "1–2 sentences: what the data shows and why this matters in commercial terms.",
      "what": ["action step 1", "action step 2", "action step 3"],
      "revenue_impact": "Quantified estimate where possible; otherwise state why it cannot be quantified cleanly from this data."
    }}
  ],

  "variant_read": [
    {{
      "variant": "variant key",
      "label": "variant label",
      "status": "strong | weak | mixed",
      "why": "1–2 sentences using only sessions, bounce, funnel progression, checkout efficiency, and any carefully caveated attributed-sales context.",
      "action": "protect | test scale | test | audit"
    }}
  ],

  "scorecard": [
    {{
      "area": "short label",
      "cpv_id_or_sku": "e.g. #88 Facebook or SSR-D",
      "status": "green | yellow | red",
      "status_reason": "one short phrase",
      "action": "protect | scale | test scale | fix | pause | audit | test"
    }}
  ],

  "data_notes": "Anomalies, attribution caveats, or measurement limitations worth flagging. null if clean."
}}

Rules:
- needle_movers: 3–6 items max, ranked by commercial impact
- Only include needle movers that are practical and applicable from this dataset
- Prefer actions that either stop active waste or unlock meaningful revenue
- Do not include speculative UX claims unless explicitly labeled as hypothesis
- scorecard: include one row per campaign or funnel stage with enough data to judge
- Exclude campaigns with fewer than 10 tracked views unless they represent active paid spend waste
- Be specific with numbers and sources
- If cross_check is clean, say so briefly in data_notes
- If a downsell or backend stage has very weak take rate, flag it as monetization gap
- If a paid campaign spends meaningfully and produces 0 tracked conversions or 0 platform purchases, that can be flagged as spend waste
- Do not mix tracking views with GA4 sessions in the same rate calculation
- Do not mix CPV conversions with ClickBank buyers unless you explicitly explain it
- If using platform purchases for paid campaign judgment, explicitly treat them as proxy signals
- Do not headline the weekly summary with proxy metrics if tracked source-level economics tell the story more reliably
- For very small-volume winners, prefer "protect" or "test scale" instead of "scale"
- Revenue impact should be labeled directional when based on scenario math rather than observed realized lift
- Do not diagnose exact on-page friction from this dataset alone; frame those as hypotheses or validation tasks

For paid CPA:
- Prefer spend / tracking attributed conversions when possible
- If using platform purchases instead, label the logic as a proxy
- Do not treat proxy CPA as exact buyer CPA

For paid RPV:
- Use tracking facebook revenue / tracking facebook views

For email RPV:
- Use tracking email revenue / tracking email views

For frontend sales:
- Use funnel_backend.frontend_sales_count

For avg_funnel_value:
- Use funnel_snapshot.avg_revenue_per_buyer_usd

For funnel_leaks:
- Prefer spend efficiency, tracked-view efficiency, checkout efficiency, conversion-to-checkout efficiency, or take rate metrics
- Do not use impressions-to-purchase as the main funnel leak metric unless explicitly labeled as an ad-delivery proxy
- Do not use purchases-from-impressions as a main funnel leak metric when better downstream metrics exist in the payload
- Prefer LP-view efficiency, spend efficiency, tracked-view efficiency, or take-rate metrics over impression-level purchase rates
- Choose leak metrics that match the stage being discussed

For variant analysis:
- Do NOT use funnel_backend.variant_sales revenue or variant_sales count as the primary performance evidence
- Use sessions, bounce_rate_pct, choice_rate_pct, optin_rate_pct, checkout_rate_pct, and checkout_from_optin_pct as the primary basis
- variant_sales may be mentioned only as secondary supporting context, with clear attribution caveat
- If one variant has better top-of-funnel engagement but worse checkout efficiency, classify it as mixed, not winner/loser
- If session-based funnel signals and attributed-sales signals disagree, classify the result as mixed unless one side is overwhelmingly stronger
- If signals conflict, recommend a test or controlled scale, not a definitive verdict
- Verify directional statements against the numeric values before writing the explanation
- Lower bounce is better; do not describe a higher bounce rate as better or lower

For scorecard:
- green = clearly healthy and supported by the benchmark, or worth protecting
- yellow = mixed, functioning but below benchmark, or worth testing
- red = active waste, severe underperformance, or obvious monetization gap
- A functioning but below-benchmark monetization step should usually be yellow, not green
- Use "test scale" for promising but still low-sample winners
- Use "scale" only when the signal is both strong and sufficiently supported
- For OTO1 main offer:
  - 40%+ take rate can be green
  - 30% to 39.9% is usually yellow
  - below 30% should not be green
- For OTO1 downsell:
  - functioning but below benchmark should usually be yellow with action "test" or "fix"
- For OTO2:
  - below 10% is weak and should not be green

- Verify combined counts and totals before writing any multi-campaign summary

If revenue impact math would be misleading, be honest and say it is directional only.

Return JSON only.
"""


# ── API call ──────────────────────────────────────────────────────────────────

def call_claude(prompt: str, system_prompt: str = "") -> dict:
    if not ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY not set — add it to your .env file")

    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key":         ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type":      "application/json",
        },
        json={
            "model":      MODEL,
            "max_tokens": MAX_TOKENS,
            "system":     system_prompt,
            "messages":   [{"role": "user", "content": prompt}],
        },
        timeout=120,
    )
    resp.raise_for_status()

    raw_text = resp.json()["content"][0]["text"].strip()

    # Strip markdown fences
    if raw_text.startswith("```"):
        raw_text = raw_text.split("```")[1]
        if raw_text.startswith("json"):
            raw_text = raw_text[4:]
        raw_text = raw_text.strip()

    # Find the outermost JSON object in case there's surrounding text
    start = raw_text.find("{")
    end   = raw_text.rfind("}") + 1
    if start != -1 and end > start:
        raw_text = raw_text[start:end]

    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        # Last resort — print what Claude returned for debugging
        print(f"[analyse] Raw Claude response:\n{raw_text[:500]}")
        raise


# ── Mock output ───────────────────────────────────────────────────────────────

def mock_analysis() -> dict:
    return {
        "period_summary": "Strong email week driven by VSL V2 (#87) carrying 22 of 25 total conversions. Total revenue $1,576.05 from 10 front-end buyers averaging $157.61 each. Paid traffic is split — #88 Facebook is the only profitable placement but volume is too small to draw conclusions yet.",
        "northstar": {
            "metric": "Revenue Per Visitor (RPV)",
            "email_rpv": 0.29,
            "paid_rpv_usd": 1.05,
            "avg_funnel_value": 157.61,
            "commentary": "Paid RPV looks strong but is based on only 133 visitors — scale #88 Facebook to 300+ before trusting the number."
        },
        "needle_movers": [
            {
                "rank": 1,
                "priority": "critical",
                "area": "Scale #88 Facebook",
                "why": "Campaign #88 Facebook placement is showing 9.09% CR and 188.5% ROI on 11 visitors. Every dollar in is returning $2.88. Volume is far too small to be conclusive but the signal is the strongest in the account.",
                "what": [
                    "Increase #88 Facebook daily budget by $20–30/day on Facebook placement only",
                    "Do NOT touch Instagram placement, creative, audience, or landing page",
                    "Monitor CPV Lab daily — if CR holds above 4% past 50 visitors, increase budget again",
                    "Target 100+ visitors before drawing conclusions"
                ],
                "revenue_impact": "At 9.09% CR and $157 avg funnel value, every 100 visitors = ~9 buyers = ~$1,413 revenue at ~$200–300 ad cost."
            },
            {
                "rank": 2,
                "priority": "critical",
                "area": "Fix #78 landing page hook",
                "why": "118 cold visitors, 3 engaged with the landing page (2.54% LP CTR). 97.5% of paid spend leaves before hearing a word of the VSL. Of the 3 who did click, 2 bought — the checkout is fine, the hook is broken.",
                "what": [
                    "Open the landing page on mobile as a cold visitor — can you answer 'what is this and why should I care?' in 5 seconds?",
                    "Rewrite above-the-fold: headline must address a pain the cold audience already feels, not what Sabrina offers",
                    "Remove everything above the fold except headline, subheadline, and play button",
                    "Pause Facebook placement on #78 and shift its budget to #88 while fixing",
                    "Create a new CPV campaign ID for the new variant — do not overwrite #78"
                ],
                "revenue_impact": "Moving LP CTR from 2.54% to 5% on 118 visitors doubles sales from the same spend. At 9% (matching #88) = 10 sales per cycle — profitable paid traffic."
            },
            {
                "rank": 3,
                "priority": "critical",
                "area": "Test downsell redirects (SSR-D, dhr-d)",
                "why": "Both OTO downsells show $0 revenue across the full 14-day period despite 5 buyers declining OTO1 and 2 declining OTO2. Either the redirects are broken or the pages are not loading. This is instant lost revenue on every single buyer.",
                "what": [
                    "Make a test purchase right now and manually click through the full OTO flow",
                    "OTO1 → click 'No thanks' → does SSR-D at $47 appear?",
                    "OTO2 → click 'No thanks' → does dhr-d at $77 appear?",
                    "If redirect is broken, fix immediately — this loses money on every buyer",
                    "If page loads but no buys: rewrite downsell copy with urgency and a clear reason the discounted version still makes sense"
                ],
                "revenue_impact": "5 declined OTO1 → 2 take SSR-D = +$94. 2 declined OTO2 → 1 takes dhr-d = +$77. That's +$171 from the current buyer pool with zero new traffic."
            },
            {
                "rank": 4,
                "priority": "high",
                "area": "Audit dead email campaigns",
                "why": "6 email campaigns (#80, #70, #74, #79, #81, #19) generated 194 views and $0 revenue. These are warm subscribers going to waste on every send.",
                "what": [
                    "Click each campaign link as a subscriber — does the page match what the email promised?",
                    "If link is dead: update to correct funnel URL and reactivate",
                    "If page loads but doesn't convert: these are retargeting sequences — copy must address why the person didn't buy the first time, not repeat the generic pitch"
                ],
                "revenue_impact": "1% CR on 194 views = 2 sales at $157 avg = +$314 from traffic you already paid to send."
            }
        ],
        "scorecard": [
            {"area": "Scale winning paid campaign", "cpv_id_or_sku": "#88 Facebook",       "status": "green",  "status_reason": "+188.5% ROI",       "action": "scale"},
            {"area": "LP hook — cold paid traffic",  "cpv_id_or_sku": "#78",               "status": "red",    "status_reason": "2.54% LP CTR",       "action": "fix"},
            {"area": "OTO1 downsell",                "cpv_id_or_sku": "SSR-D ($47)",        "status": "red",    "status_reason": "$0 — likely broken", "action": "fix"},
            {"area": "OTO2 downsell",                "cpv_id_or_sku": "dhr-d ($77)",        "status": "red",    "status_reason": "$0 — likely broken", "action": "fix"},
            {"area": "Dead email campaigns",         "cpv_id_or_sku": "#80,#70,#74,#79,#81,#19", "status": "red", "status_reason": "$0 on 194 views","action": "audit"},
            {"area": "OTO1 take rate",               "cpv_id_or_sku": "SSR ($67)",          "status": "green",  "status_reason": "50% take rate",      "action": "protect"},
            {"area": "OTO2 take rate",               "cpv_id_or_sku": "dhr ($97)",          "status": "green",  "status_reason": "60% of OTO1 buyers", "action": "protect"},
            {"area": "Order bump",                   "cpv_id_or_sku": "SPR-OB2",            "status": "green",  "status_reason": "40% new take rate",  "action": "protect"},
            {"area": "Advanced vs Basic split",      "cpv_id_or_sku": "abdt-advanced",      "status": "green",  "status_reason": "70% choosing $54",   "action": "protect"},
            {"area": "Main email driver",            "cpv_id_or_sku": "#87",                "status": "green",  "status_reason": "Carrying volume",    "action": "protect"},
            {"area": "#88 Instagram",                "cpv_id_or_sku": "#88 IG",             "status": "red",    "status_reason": "0% CR",              "action": "pause"},
        ],
        "data_notes": "Cross-check shows $157.71 gap between CPV revenue ($1,733.76) and ClickBank ($1,576.05). This is larger than the expected $20 buffer — likely due to CPV attributing upsell postbacks at list price while ClickBank net settlement differs. Recommend verifying CPV revenue field represents gross or net."
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    project = args.project or os.environ.get("PROJECT", "asksabrina")

    # Delegate to project-specific script when applicable
    if project == "astroloversketch":
        import subprocess
        cmd = [sys.executable, "analyse_astro.py"]
        if args.input: cmd += ["--input", args.input]
        if args.mock:  cmd += ["--mock"]
        sys.exit(subprocess.run(cmd).returncode)

    if not args.out:
        from datetime import date
        args.out = f"output/{project}/analysis_{date.today().isoformat()}.json"

    if args.mock:
        print("[analyse] Mock mode — skipping API call")
        analysis = mock_analysis()
    else:
        # Auto-detect latest dated file if no input specified
        if not args.input:
            import glob
            files = sorted(glob.glob(f"output/{project}/report_data_*.json"))
            args.input = files[-1] if files else f"output/{project}/report_data.json"
            print(f"[analyse] Auto-detected input: {args.input}")

        # Auto-generate dated output filename from input filename date
        if not args.out:
            date_part = Path(args.input).stem.replace("report_data_", "")
            args.out = f"output/{project}/analysis_{date_part}.json"

        input_path = Path(args.input)
        if not input_path.exists():
            print(f"Error: {input_path} not found. Run collect.py first.", file=sys.stderr)
            sys.exit(1)

        with open(input_path) as f:
            data = json.load(f)

        print(f"[analyse] Loaded {input_path} ({data['meta']['period_start']} → {data['meta']['period_end']})")
        print(f"[analyse] Sending to Claude ({MODEL})...")

        prompt   = build_user_prompt(data)
        system_prompt = build_system_prompt(data)
        analysis = call_claude(prompt, system_prompt)
        print("[analyse] ✓ Analysis received")

    # Wrap with metadata
    output = {
        "meta": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "model":        MODEL,
            "mode":         "mock" if args.mock else "live",
        },
        "analysis": analysis,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"[analyse] Output written to {out_path}")

    # Print a quick preview
    nm = analysis.get("needle_movers", [])
    print(f"\n── {len(nm)} needle movers this week ──")
    for item in nm:
        icon = {"critical": "🔴", "high": "🟡", "medium": "⚪"}.get(item["priority"], "")
        print(f"  {icon} #{item['rank']} {item['area']}")
    print()


if __name__ == "__main__":
    main()