#!/usr/bin/env python3
"""
Brand Intelligence — Google Trends Fetcher
==========================================
Fetches brand + generic trend data for all country tabs.
Writes results directly into Google Sheets.

Primary  : pytrends (free, no key needed)
Fallback : SerpAPI  (free tier, 100 searches/month)

SETUP:
    pip install pytrends pandas gspread google-auth requests

RUN:
    python fetcher_to_sheets.py
"""

import os
import time
import json
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from pytrends.request import TrendReq
from datetime import datetime

# ═══════════════════════════════════════════════════════════
# ▼▼▼  EDIT THIS SECTION  ▼▼▼
# ═══════════════════════════════════════════════════════════

SHEET_ID         = "1CkCZeWpfzaFAhVDxqqK7qqrWj28ZXP3IhqKkX6Udefg"
CREDENTIALS_FILE = "credentials.json"

# SerpAPI key (free tier = 100 searches/month)
# Get yours free at: https://serpapi.com/users/sign_up
#
# ⚠️  NEVER hardcode your key here if this repo is public!
#     Set it as an environment variable or GitHub Secret instead:
#       export SERPAPI_KEY="your_key_here"
#     Or in GitHub Actions: Settings → Secrets → SERPAPI_KEY
#
SERPAPI_KEY = os.environ.get("SERPAPI_KEY", None)

# Timeframe: "today 12-m" | "today 6-m" | "today 3-m"
TIMEFRAME = "today 12-m"

# Set to True to skip pytrends and use SerpAPI directly (recommended when pytrends keeps getting 429)
SKIP_PYTRENDS = True

# Delay between API calls (seconds) — don't go below 2.0
API_DELAY = 2.5

# Delay between brand and generic fetches within a tab
TAB_DELAY = 10

# ═══════════════════════════════════════════════════════════
# COUNTRY TABS
# Format: (tab_name, geo, region, [brands], [generic_trends])
#
# brands        — up to 12 brand names to monitor
# generic_trends — broader category terms for context
#                  e.g. "online shopping", "digital banking"
# ═══════════════════════════════════════════════════════════

TABS = [
    (
        "CA-Ontario", "CA", "CA-ON",
        ["tooniebet", "swiper", "betmgm", "888casino", "powerplay", "fanduel", "draftkings", "playnow"],
        ["Online casino", "Casino", "online casino canada", "Slots", "Sports betting", "live casino"]
    ),
    (
        "CA-All", "CA", "",
        ["tooniebet", "swiper", "betmgm", "888casino", "powerplay", "fanduel", "draftkings", "playnow"],
        ["Online casino", "Casino", "online casino canada", "Slots", "Sports betting", "live casino"]
    ),
    (
        "Greece", "GR", "",
        ["elabet", "betsson", "stoiximan", "netbet", "pamestoixima", "novibet", "pokerstars", "sportingbet"],
        ["Online casino", "casino", "στοίχημα", "αθλητικό στοίχημα", "live casino"]
    ),
    (
        "Mexico", "MX", "",
        ["campobet", "novibet", "codere", "sportium", "betway", "betfair", "rushbet", "netbet"],
        ["ruleta online", "casino", "casino online", "apuestas deportivas", "slots", "juegos de casino", "live casino", "casino en vivo"]
    ),
    (
        "Sweden", "SE", "",
        ["betinia", "swiper", "campobet", "quick casino", "lodur", "unibet", "lucky casino", "paf"],
        ["casino", "casino online", "spela casino", "slots", "live casino", "casino bonus", "bet", "odds"]
    ),
    (
        "Denmark", "DK", "",
        ["betinia", "swiper", "campobet", "danske spil", "tivolicasino", "mariacasino", "unibet", "betsson"],
        ["bet", "Casino", "Online casino", "Odds", "slots"]
    ),
    (
        "Spain", "ES", "",
        [],   # Add brands when ready
        []    # Add generic trends when ready
    ),
    (
        "Romania", "RO", "",
        ["don ro", "topbet", "netbet", "Swiper", "betano", "casa pariurilor", "betfair", "888"],
        ["Casino Online", "Casino", "pacanele", "jocuri cazino", "pariuri sportive"]
    ),
    (
        "Italy", "IT", "",
        [],   # Add brands when ready
        []    # Add generic trends when ready
    ),
]

# ═══════════════════════════════════════════════════════════
# GOOGLE SHEETS CONNECTION
# ═══════════════════════════════════════════════════════════

def connect_sheets():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds  = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID)


def get_or_create_tab(spreadsheet, name):
    try:
        ws = spreadsheet.worksheet(name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=name, rows=500, cols=25)
    return ws


# ═══════════════════════════════════════════════════════════
# PYTRENDS FETCHER (primary)
# ═══════════════════════════════════════════════════════════

def fetch_pytrends(keywords, geo, region, timeframe):
    """Fetch via pytrends. Returns DataFrame or None on failure."""
    try:
        pt         = TrendReq(hl="en-US", tz=360)
        geo_target = region if region else geo
        anchor     = keywords[0]
        all_series = {}

        batches = [keywords[i:i+4] for i in range(0, len(keywords), 4)]

        for batch in batches:
            kw_list = list(dict.fromkeys([anchor] + batch))[:5]
            print(f"      [pytrends] {kw_list}")

            pt.build_payload(kw_list, timeframe=timeframe, geo=geo_target)
            df = pt.interest_over_time()

            if df.empty:
                print(f"      ⚠ Empty response for batch")
                time.sleep(API_DELAY)
                continue

            if "isPartial" in df.columns:
                df = df.drop(columns=["isPartial"])

            for kw in kw_list:
                if kw in df.columns and kw not in all_series:
                    all_series[kw] = df[kw]

            time.sleep(API_DELAY)

        if not all_series:
            return None

        result = pd.DataFrame(all_series)
        ordered = [k for k in keywords if k in result.columns]
        return result[ordered]

    except Exception as e:
        print(f"      ✗ pytrends error: {e}")
        return None


# ═══════════════════════════════════════════════════════════
# SERPAPI FETCHER (fallback)
# ═══════════════════════════════════════════════════════════

def fetch_serpapi(keywords, geo, timeframe):
    """Fetch via SerpAPI free tier. Returns DataFrame or None."""
    if not SERPAPI_KEY:
        return None

    # Map timeframe string to SerpAPI date format
    tf_map = {
        "today 12-m": "today 12-m",
        "today 6-m":  "today 6-m",
        "today 3-m":  "today 3-m",
    }
    date_param = tf_map.get(timeframe, "today 12-m")

    all_series = {}
    anchor     = keywords[0]

    batches = [keywords[i:i+4] for i in range(0, len(keywords), 4)]

    for batch in batches:
        kw_list  = list(dict.fromkeys([anchor] + batch))[:5]
        kw_query = ",".join(kw_list)
        print(f"      [serpapi] {kw_list}")

        try:
            params = {
                "engine":   "google_trends",
                "q":        kw_query,
                "geo":      geo,
                "date":     date_param,
                "api_key":  SERPAPI_KEY,
            }
            resp = requests.get("https://serpapi.com/search", params=params, timeout=30)
            data = resp.json()

            if "error" in data:
                print(f"      ✗ SerpAPI error: {data['error']}")
                continue

            timeline = data.get("interest_over_time", {}).get("timeline_data", [])
            if not timeline:
                print(f"      ⚠ No timeline data")
                continue

            # Parse into series
            dates = []
            kw_data = {kw: [] for kw in kw_list}

            for point in timeline:
                # SerpAPI returns week ranges like "Mar 16 – 22, 2025"
                # Extract just the start date
                raw_date = point["date"]
                try:
                    # Try direct parse first
                    parsed_date = pd.to_datetime(raw_date)
                except Exception:
                    try:
                        # Handle "Mar 16 – 22, 2025" → take "Mar 16, 2025"
                        import re
                        # Remove the " – 22" part (en-dash range)
                        cleaned = re.sub(r'\s[–\-]\s*\d+', '', raw_date)
                        parsed_date = pd.to_datetime(cleaned)
                    except Exception:
                        try:
                            # Last resort: extract first date pattern
                            import re
                            m = re.search(r'([A-Za-z]+ \d+,?\s*\d{4})', raw_date)
                            parsed_date = pd.to_datetime(m.group(1)) if m else None
                        except Exception:
                            parsed_date = None

                if parsed_date is not None:
                    dates.append(parsed_date)
                    for val in point.get("values", []):
                        q = val.get("query")
                        v = val.get("extracted_value", 0)
                        if q in kw_data:
                            kw_data[q].append(v)

            for kw in kw_list:
                if kw in kw_data and len(kw_data[kw]) == len(dates):
                    s = pd.Series(kw_data[kw], index=pd.DatetimeIndex(dates), name=kw)
                    if kw not in all_series:
                        all_series[kw] = s

            time.sleep(1.0)

        except Exception as e:
            print(f"      ✗ SerpAPI request error: {e}")

    if not all_series:
        return None

    result = pd.DataFrame(all_series)
    ordered = [k for k in keywords if k in result.columns]
    return result[ordered]


# ═══════════════════════════════════════════════════════════
# FETCH WITH FALLBACK
# ═══════════════════════════════════════════════════════════

def fetch_with_fallback(keywords, geo, region, timeframe):
    """Try pytrends first (unless SKIP_PYTRENDS=True), fall back to SerpAPI."""

    if not SKIP_PYTRENDS:
        print(f"    Trying pytrends...")
        df = fetch_pytrends(keywords, geo, region, timeframe)
        if df is not None and not df.empty:
            print(f"    ✓ pytrends succeeded")
            return df, "pytrends"
        print(f"    pytrends failed (429?) — trying SerpAPI...")
    else:
        print(f"    Skipping pytrends — using SerpAPI directly...")

    if SERPAPI_KEY:
        df = fetch_serpapi(keywords, geo, timeframe)
        if df is not None and not df.empty:
            print(f"    ✓ SerpAPI succeeded")
            return df, "serpapi"
        print(f"    ✗ SerpAPI also failed")
    else:
        print(f"    ✗ No SerpAPI key configured — set SERPAPI_KEY in the script")

    return None, "failed"


# ═══════════════════════════════════════════════════════════
# WRITE TO SHEET
# Long format: one row per keyword per date point
# Columns: Date, Keyword, Interest, Type, Country,
#          Region, MonthYear, Year, Source, FetchedAt
# ═══════════════════════════════════════════════════════════

def write_to_sheet(keywords, df, kw_type, geo, region, source):
    """Write data rows. kw_type = 'brand' or 'generic'"""
    fetched_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    headers = [
        "Date", "Keyword", "Interest", "Type",
        "Country", "Region", "MonthYear", "Year",
        "Source", "FetchedAt"
    ]

    rows = [headers]

    for date_idx in df.index:
        date_str    = str(date_idx)[:10]
        month_label = date_idx.strftime("%b %Y")
        year_str    = str(date_idx.year)

        for kw in keywords:
            if kw not in df.columns:
                continue
            value = int(df.loc[date_idx, kw])
            rows.append([
                date_str,
                kw,
                value,
                kw_type,         # "brand" or "generic"
                geo,
                region or geo,
                month_label,
                year_str,
                source,
                fetched_at,
            ])

    return rows


# ═══════════════════════════════════════════════════════════
# PROCESS ONE TAB
# ═══════════════════════════════════════════════════════════

def process_tab(spreadsheet, tab_name, geo, region, brands, generics):
    print(f"\n{chr(9472)*55}")
    print(f"  Tab: {tab_name}  ({geo}/{region or 'All'})")
    print(f"{chr(9472)*55}")

    # If no keywords configured — clear the Sheet tab and return
    if not brands and not generics:
        print(f"  No keywords configured — clearing Sheet tab if it exists")
        try:
            ws = spreadsheet.worksheet(tab_name)
            ws.clear()
            print(f"  Cleared '{tab_name}' in Google Sheets")
        except Exception:
            print(f"  Tab '{tab_name}' does not exist yet — nothing to clear")
        return "CLEARED"

    all_rows    = []
    header_done = False

    # Fetch brands
    print(f"\n  Brands ({len(brands)}):")
    if not brands:
        print("  No brands configured — skipping")
        brand_df, brand_source = None, "skipped"
    else:
        brand_df, brand_source = fetch_with_fallback(brands, geo, region, TIMEFRAME)

    if brand_df is not None:
        rows = write_to_sheet(brands, brand_df, "brand", geo, region, brand_source)
        all_rows.extend(rows)
        header_done = True
        print(f"  {len(rows)-1} brand data rows")
    else:
        if brands:
            print(f"  No brand data fetched")

    print(f"  Pausing {TAB_DELAY}s before generic trends...")
    time.sleep(TAB_DELAY)

    # Fetch generic trends
    print(f"\n  Generic trends ({len(generics)}):")
    if not generics:
        print("  No generic trends configured — skipping")
        gen_df, gen_source = None, "skipped"
    else:
        gen_df, gen_source = fetch_with_fallback(generics, geo, region, TIMEFRAME)

    if gen_df is not None:
        rows = write_to_sheet(generics, gen_df, "generic", geo, region, gen_source)
        if not header_done:
            all_rows.extend(rows)
        else:
            all_rows.extend(rows[1:])
        print(f"  {len(rows)-1} generic data rows")
    else:
        if generics:
            print(f"  No generic data fetched")

    # Write to sheet
    if all_rows:
        ws = get_or_create_tab(spreadsheet, tab_name)
        ws.update(all_rows, "A1")
        print(f"\n  Written {len(all_rows)-1} total rows to '{tab_name}'")
        return "OK"
    else:
        print(f"\n  Nothing to write for {tab_name}")
        return "NO DATA"


def write_metadata(spreadsheet, summary):
    try:
        ws = get_or_create_tab(spreadsheet, "_metadata")
    except Exception:
        ws = spreadsheet.add_worksheet(title="_metadata", rows=50, cols=8)

    now  = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    rows = [["Tab", "Country", "Region", "Brands", "Generics", "Status", "LastUpdated"]]

    for tab_name, geo, region, brands, generics, status in summary:
        rows.append([
            tab_name, geo, region or "All",
            len(brands), len(generics),
            status, now
        ])

    ws.update(rows, "A1")
    print(f"\n  ✓ Metadata updated")


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

def tab_already_has_data(spreadsheet, tab_name):
    """Returns True if the tab exists and has more than 5 rows of data."""
    try:
        ws = spreadsheet.worksheet(tab_name)
        all_vals = ws.get_all_values()
        rows_with_data = [r for r in all_vals if any(c.strip() for c in r)]
        return len(rows_with_data) > 5
    except Exception:
        return False


def main():
    import sys
    force_all = "--force" in sys.argv

    print("\n" + "═"*55)
    print("  Brand Intelligence — Trends Fetcher")
    print(f"  Tabs      : {len(TABS)}")
    print(f"  Timeframe : {TIMEFRAME}")
    print(f"  SerpAPI   : {'enabled' if SERPAPI_KEY else 'disabled (pytrends only)'}")
    print(f"  Mode      : {'FORCE ALL (re-fetch everything)' if force_all else 'Smart (skip tabs with existing data)'}")
    print("═"*55)

    print("\nConnecting to Google Sheets...")
    try:
        spreadsheet = connect_sheets()
        print(f"  ✓ Connected to: '{spreadsheet.title}'")
    except Exception as e:
        print(f"  ✗ Connection failed: {e}")
        print("\n  → Check that:")
        print("    1. credentials.json is in this folder")
        print("    2. SHEET_ID is correct in this script")
        print("    3. The Sheet is shared with your service account email")
        return

    summary = []

    for tab_name, geo, region, brands, generics in TABS:

        # Skip empty tabs — just clear them
        if not brands and not generics:
            try:
                ws = spreadsheet.worksheet(tab_name)
                ws.clear()
                print(f"  ✅ {tab_name} — no keywords, cleared Sheet tab")
            except Exception:
                print(f"  ✅ {tab_name} — no keywords, tab doesn't exist yet")
            summary.append((tab_name, geo, region, brands, generics, "CLEARED"))
            continue

        # Skip tabs that already have data (unless --force)
        if not force_all and tab_already_has_data(spreadsheet, tab_name):
            print(f"  ⏭  {tab_name} — already has data, skipping (use --force to re-fetch)")
            summary.append((tab_name, geo, region, brands, generics, "SKIPPED"))
            continue

        try:
            status = process_tab(spreadsheet, tab_name, geo, region, brands, generics)
        except Exception as e:
            print(f"  ✗ Unexpected error on {tab_name}: {e}")
            import traceback
            traceback.print_exc()
            status = f"ERROR: {e}"

        summary.append((tab_name, geo, region, brands, generics, status))

        print(f"\n  Waiting {TAB_DELAY}s before next tab...")
        time.sleep(TAB_DELAY)

    write_metadata(spreadsheet, summary)

    print("\n" + "═"*55)
    print("  FINAL SUMMARY")
    print("═"*55)
    for tab_name, geo, region, brands, generics, status in summary:
        icon = "✅" if status in ("OK", "SKIPPED", "CLEARED") else "⚠ "
        print(f"  {icon}  {tab_name:15} {status}")

    print(f"\n  Sheet: https://docs.google.com/spreadsheets/d/{SHEET_ID}\n")


if __name__ == "__main__":
    main()
