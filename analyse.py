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
MAX_TOKENS = 4000


def parse_args():
    parser = argparse.ArgumentParser(description="Run AI analysis on collected funnel data")
    parser.add_argument("--input",  default="output/report_data.json", help="Path to report_data.json")
    parser.add_argument("--out",    default="output/analysis.json",    help="Output path")
    parser.add_argument("--mock",   action="store_true",               help="Skip API call, return mock analysis")
    return parser.parse_args()


# ── Prompt ────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a direct-response funnel analyst for Ask Sabrina, a spiritual coaching offer sold via ClickBank.

Funnel structure (always keep this in mind):
  Traffic → Landing Page → Video VSL → Basic $37 / Advanced $54
  → Order Bump (free trial → $14.99/mo)
  → OTO1: Soul Signature Compass $67
  → OTO1 Downsell: $47
  → OTO2: Divine Helper Blueprint $97
  → OTO2 Downsell: $77

Key benchmarks to apply:
- Healthy LP CTR for cold paid traffic: 4–6%
- Healthy email campaign CR: 0.3–0.5% (volume), 2%+ (retargeting)
- OTO take rates: 40%+ is strong, under 20% needs attention
- Downsell take rates: 0% for 10+ days = likely broken redirect
- Paid traffic: target CPA below $80 USD (half of $157 avg funnel value)
- Revenue per visitor (RPV): email ~$0.28–0.35, paid profitable above $1.00

Traffic sources:
- Email (Maropost, tracked via CPV Labs): warm audience, lower CR expected but high volume
- Facebook + Instagram (paid): cold audience, judge by LP CTR and CPA not just CR

CPV Labs tracks ClickBank postbacks — one buyer who takes frontend + OTO1 + OTO2 fires 3 postbacks.
So CPV "conversions" > ClickBank frontend sales is normal. Use ClickBank for true buyer count.

Your output must be valid JSON only — no markdown, no preamble, no explanation outside the JSON.
CRITICAL: Return only valid JSON. No trailing commas. No comments. No text outside the JSON object.
"""

def build_user_prompt(data: dict) -> str:
    # Slim the payload — remove pycache noise, keep what matters
    slim = {
        "period": f"{data['meta']['period_start']} to {data['meta']['period_end']}",
        "funnel_snapshot": data.get("funnel_snapshot"),
        "email_campaigns": data.get("traffic_sources", {}).get("tracking", {}).get("campaigns"),
        "paid_campaigns":  data.get("traffic_sources", {}).get("paid", {}).get("campaigns"),
        "funnel_backend":  data.get("funnel_backend"),
        "cross_check":     data.get("cross_check"),
    }

    return f"""Here is this week's funnel performance data:

{json.dumps(slim, indent=2)}

Analyse this data and return a JSON object with exactly this structure:

{{
  "period_summary": "2–3 sentence plain-English summary of the week. Include total revenue, front-end sales, and one headline observation.",

  "northstar": {{
    "metric": "Revenue Per Visitor (RPV)",
    "email_rpv": <float>,
    "paid_rpv_usd": <float>,
    "avg_funnel_value": <float>,
    "commentary": "1 sentence on what the RPV numbers mean right now."
  }},

  "needle_movers": [
    {{
      "rank": 1,
      "priority": "critical | high | medium",
      "area": "short label e.g. Scale #88 Facebook",
      "why": "1–2 sentences: what the data shows and why this matters in dollar terms.",
      "what": ["action step 1", "action step 2", "action step 3"],
      "revenue_impact": "Quantified estimate e.g. At X% CR and $157 avg value, Y visitors = $Z revenue."
    }}
  ],

  "scorecard": [
    {{
      "area": "short label",
      "cpv_id_or_sku": "e.g. #88 Facebook or SSR-D",
      "status": "green | yellow | red",
      "status_reason": "one short phrase",
      "action": "protect | scale | fix | pause | audit"
    }}
  ],

  "data_notes": "Any anomalies, cross-check flags, or data quality issues worth flagging. null if clean."
}}

Rules:
- needle_movers: 3–6 items max, ranked by revenue impact. Only include items with clear data backing.
- scorecard: one row per campaign or funnel stage that has data. Include both working and broken items.
- Be specific with numbers — pull actual figures from the data, don't generalise.
- If a downsell has $0 revenue for the full period, flag it as likely broken redirect.
- Do not include campaigns with fewer than 10 views.
"""


# ── API call ──────────────────────────────────────────────────────────────────

def call_claude(prompt: str) -> dict:
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
            "system":     SYSTEM_PROMPT,
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

    if args.mock:
        print("[analyse] Mock mode — skipping API call")
        analysis = mock_analysis()
    else:
        # Load collected data
        input_path = Path(args.input)
        if not input_path.exists():
            print(f"Error: {input_path} not found. Run collect.py first.", file=sys.stderr)
            sys.exit(1)

        with open(input_path) as f:
            data = json.load(f)

        print(f"[analyse] Loaded {input_path} ({data['meta']['period_start']} → {data['meta']['period_end']})")
        print(f"[analyse] Sending to Claude ({MODEL})...")

        prompt   = build_user_prompt(data)
        analysis = call_claude(prompt)
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