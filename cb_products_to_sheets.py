"""
ClickBank Product Lister → Google Sheets
Fetches all products for a ClickBank site and writes SKU, pitch page, and price
to a Google Sheet tab named after the site.

Usage:
    python cb_products_to_sheets.py --site sabrinapsy
    python cb_products_to_sheets.py --site ASTROSKETC

Requirements:
    pip install requests google-auth google-auth-httplib2 google-api-python-client

Environment variables (or edit the constants below):
    CLICKBANK_API_KEY       — your ClickBank clerk API key
    GOOGLE_CREDENTIALS_FILE — path to service account JSON (default: google_credentials.json)
    SHEET_ID                — Google Sheet ID (the long string from the URL)
"""

import argparse
import json
import os
import sys

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build


# ── Constants (override via env or edit here) ─────────────────────────────────

CLICKBANK_API_KEY    = os.environ.get("ASKSABRINA_CLICKBANK_API_KEY", "")
GOOGLE_CREDENTIALS   = os.environ.get("GOOGLE_CREDENTIALS_FILE", "google_credentials.json")
SHEET_ID             = os.environ.get("SHEET_ID", "")
CLICKBANK_BASE_URL   = "https://api.clickbank.com/rest/1.3"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


# ── ClickBank ─────────────────────────────────────────────────────────────────

def fetch_products(site: str) -> list[dict]:
    """Fetch all products for a given ClickBank site."""
    resp = requests.get(
        f"{CLICKBANK_BASE_URL}/products/list",
        headers={
            "Authorization": CLICKBANK_API_KEY,
            "Accept":        "application/json",
        },
        params={"site": site},
        timeout=30,
    )

    if not resp.ok:
        print(f"Error fetching products: {resp.status_code} {resp.reason}")
        print(resp.text[:300])
        sys.exit(1)

    data = resp.json()
    products_raw = data.get("products", {}).get("product", [])

    # API returns a dict (not list) when there's only one product
    if isinstance(products_raw, dict):
        products_raw = [products_raw]

    rows = []
    for p in products_raw:
        sku        = p.get("@sku", "")
        pitch_page = p.get("pitch_pages", {}).get("desktop", "")
        price      = (
            p.get("pricings", {})
             .get("pricing", {})
             .get("standard", {})
             .get("price", {})
             .get("usd", "")
        )
        rows.append([sku, pitch_page, price])

    return rows


# ── Google Sheets ─────────────────────────────────────────────────────────────

def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def ensure_tab(service, spreadsheet_id: str, tab_name: str):
    """Create the tab if it doesn't exist yet."""
    meta     = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    existing = [s["properties"]["title"] for s in meta["sheets"]]

    if tab_name not in existing:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
        ).execute()
        print(f"Created tab: '{tab_name}'")
    else:
        print(f"Tab '{tab_name}' already exists — will overwrite")


def write_to_sheet(service, spreadsheet_id: str, tab_name: str, rows: list[list]):
    """Clear the tab and write headers + data."""
    # Clear existing content
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A:Z",
    ).execute()

    # Write headers + rows
    headers = [["SKU", "Pitch Page", "Price (USD)"]]
    values  = headers + rows

    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A1",
        valueInputOption="RAW",
        body={"values": values},
    ).execute()

    print(f"Written {len(rows)} products to tab '{tab_name}'")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="ClickBank products → Google Sheets")
    parser.add_argument("--site",        required=True,  help="ClickBank site/vendor ID e.g. sabrinapsy")
    parser.add_argument("--sheet-id",    default="",     help="Google Sheet ID (overrides env SHEET_ID)")
    parser.add_argument("--api-key",     default="",     help="ClickBank API key (overrides env CLICKBANK_API_KEY)")
    parser.add_argument("--credentials", default="",     help="Path to service account JSON (overrides env)")
    args = parser.parse_args()

    # Apply overrides
    global CLICKBANK_API_KEY, GOOGLE_CREDENTIALS, SHEET_ID
    if args.api_key:     CLICKBANK_API_KEY  = args.api_key
    if args.credentials: GOOGLE_CREDENTIALS = args.credentials
    if args.sheet_id:    SHEET_ID           = args.sheet_id

    # Validate
    if not CLICKBANK_API_KEY:
        print("Error: CLICKBANK_API_KEY not set. Use --api-key or set env var.")
        sys.exit(1)
    if not SHEET_ID:
        print("Error: SHEET_ID not set. Use --sheet-id or set env var.")
        sys.exit(1)
    if not os.path.exists(GOOGLE_CREDENTIALS):
        print(f"Error: credentials file not found: {GOOGLE_CREDENTIALS}")
        sys.exit(1)

    site     = args.site.upper()
    tab_name = site  # tab named after the site e.g. "SABRINAPSY"

    print(f"Fetching products for site: {site}")
    rows = fetch_products(args.site)
    print(f"Found {len(rows)} products")

    for r in rows:
        print(f"  {r[0]:<30} ${r[2]:<10} {r[1]}")

    print(f"\nWriting to sheet: {SHEET_ID} → tab: {tab_name}")
    service = get_sheets_service()
    ensure_tab(service, SHEET_ID, tab_name)
    write_to_sheet(service, SHEET_ID, tab_name, rows)

    print(f"\nDone ✓ → https://docs.google.com/spreadsheets/d/{SHEET_ID}")


if __name__ == "__main__":
    main()