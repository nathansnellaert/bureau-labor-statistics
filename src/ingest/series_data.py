"""Fetch time series data from BLS API for selected series.

Loads series IDs from the catalog (crawled from Data Finder) and fetches
actual data + metadata via the BLS API with catalog=true.
"""

import os
from collections import defaultdict
from datetime import datetime

from subsets_utils import load_raw_json, save_raw_json, load_state, save_state
from utils import rate_limited_post

API_BASE_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"


def get_api_key():
    return os.environ['BLS_API_KEY']

# How many series to fetch per survey prefix (top N by popularity rank)
SERIES_PER_SURVEY = 500

# BLS API limits
BATCH_SIZE = 50  # Max series per request
MAX_YEARS_PER_REQUEST = 20

# Surveys that need more series (annual point-in-time surveys with 1 data point per series)
HIGH_VOLUME_SURVEYS = {"OE", "WM"}
SERIES_PER_SURVEY_HIGH_VOLUME = 2000

# Hardcoded series for important surveys missing from both catalog and popular_series
FALLBACK_SERIES = {
    # JOLTS - Job Openings and Labor Turnover Survey
    "JT": [
        # Total nonfarm - levels (seasonally adjusted)
        "JTS000000000000000JOL",  # Job openings level
        "JTS000000000000000HIL",  # Hires level
        "JTS000000000000000QUL",  # Quits level
        "JTS000000000000000LDL",  # Layoffs and discharges level
        "JTS000000000000000TSL",  # Total separations level
        # Total nonfarm - rates (seasonally adjusted)
        "JTS000000000000000JOR",  # Job openings rate
        "JTS000000000000000HIR",  # Hires rate
        "JTS000000000000000QUR",  # Quits rate
        "JTS000000000000000LDR",  # Layoffs and discharges rate
        "JTS000000000000000TSR",  # Total separations rate
        # Total private
        "JTS100000000000000JOL",  # Private job openings level
        "JTS100000000000000HIL",  # Private hires level
        "JTS100000000000000QUL",  # Private quits level
        # Government
        "JTS900000000000000JOL",  # Government job openings level
        # By industry - job openings
        "JTS510000000000000JOL",  # Professional and business services
        "JTS540000000000000JOL",  # Health care and social assistance
        "JTS440000000000000JOL",  # Retail trade
        "JTS720000000000000JOL",  # Accommodation and food services
        "JTS320000000000000JOL",  # Manufacturing
        "JTS230000000000000JOL",  # Construction
    ],
    # NOTE: QCEW (EN) is NOT available through the BLS timeseries API
    # It requires a separate bulk data download from https://www.bls.gov/cew/downloadable-data-files.htm
    # NOTE: OR (Occupational Requirements) series are covered by the catalog (20 series)
}


def select_series_from_catalog(catalog: list[dict], popular_data: dict | None, per_survey: int) -> list[str]:
    """Select top N series per survey prefix from the catalog.

    Falls back to popular_series for surveys missing from catalog.

    Args:
        catalog: List of series dicts with rank, series_id, survey_prefix
        popular_data: Popular series data (fallback for missing surveys)
        per_survey: How many series to take per survey

    Returns:
        List of series IDs to fetch
    """
    by_survey = defaultdict(list)

    for entry in catalog:
        prefix = entry.get('survey_prefix', entry.get('series_id', '')[:2])
        by_survey[prefix].append(entry)

    selected = []
    for prefix, series_list in by_survey.items():
        # Use higher limit for point-in-time surveys (OE, WM only have 1 data point per series)
        limit = SERIES_PER_SURVEY_HIGH_VOLUME if prefix in HIGH_VOLUME_SURVEYS else per_survey
        # Already sorted by rank from catalog
        top_n = series_list[:limit]
        selected.extend(s['series_id'] for s in top_n)

    # Fill in surveys missing from catalog using popular_series
    if popular_data:
        catalog_prefixes = set(by_survey.keys())
        for survey_prefix, series_list in popular_data.get("by_survey", {}).items():
            if survey_prefix not in catalog_prefixes:
                for series in series_list:
                    if series and series.get("seriesID"):
                        selected.append(series["seriesID"])
                        catalog_prefixes.add(survey_prefix)  # Mark as covered

    # Fill in important surveys missing from both sources using hardcoded fallback
    covered_prefixes = set(by_survey.keys())
    if popular_data:
        covered_prefixes.update(
            p for p, s in popular_data.get("by_survey", {}).items()
            if any(x and x.get("seriesID") for x in s)
        )
    for survey_prefix, series_ids in FALLBACK_SERIES.items():
        if survey_prefix not in covered_prefixes:
            selected.extend(series_ids)
            print(f"  Added {len(series_ids)} hardcoded {survey_prefix} series")

    return selected


def fetch_series_batch(series_ids: list[str], start_year: int, end_year: int) -> list[dict]:
    """Fetch data for a batch of series from BLS API."""
    headers = {"Content-type": "application/json"}
    json_data = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": get_api_key(),
        "catalog": True
    }

    response = rate_limited_post(API_BASE_URL, headers=headers, json=json_data)
    data = response.json()

    status = data.get("status")
    if status in ["REQUEST_FAILED", "REQUEST_NOT_PROCESSED"]:
        raise ValueError(f"BLS API error: {data.get('message', 'Unknown error')}")

    return data.get("Results", {}).get("series", [])


def run():
    """Fetch time series data for selected series and save raw JSON."""
    # Load popular_series for fallback (surveys missing from catalog)
    try:
        popular_data = load_raw_json("popular_series")
    except FileNotFoundError:
        popular_data = None

    # Try to load from series_catalog first, fall back to popular_series
    try:
        catalog = load_raw_json("series_catalog")
        series_ids = select_series_from_catalog(catalog, popular_data, SERIES_PER_SURVEY)
        print(f"  Selected {len(series_ids)} series from catalog ({SERIES_PER_SURVEY} per survey)")
        if popular_data:
            print(f"  (with fallback to popular_series for missing surveys)")
    except FileNotFoundError:
        print("  No series_catalog found, falling back to popular_series")
        if not popular_data:
            raise FileNotFoundError("No series_catalog or popular_series found")
        series_ids = set()
        for series in popular_data.get("overall", []):
            if series and series.get("seriesID"):
                series_ids.add(series["seriesID"])
        for survey_series in popular_data.get("by_survey", {}).values():
            for series in survey_series:
                if series and series.get("seriesID"):
                    series_ids.add(series["seriesID"])
        series_ids = list(series_ids)
        print(f"  Found {len(series_ids)} series from popular_series")

    # Check state for resumption
    state = load_state("series_data")
    completed_ids = set(state.get("completed_series", []))

    remaining_ids = [sid for sid in series_ids if sid not in completed_ids]
    print(f"  {len(remaining_ids)} series remaining to fetch")

    if not remaining_ids:
        print("  All series already fetched")
        return

    # Determine date range (20 years of data)
    current_date = datetime.now()
    if current_date.month <= 2:
        end_year = current_date.year - 1
    else:
        end_year = current_date.year
    start_year = end_year - 19

    all_series_data = state.get("series_data", [])
    total_batches = (len(remaining_ids) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(remaining_ids), BATCH_SIZE):
        batch = remaining_ids[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        print(f"  Batch {batch_num}/{total_batches}: fetching {len(batch)} series")

        try:
            series_data_list = fetch_series_batch(batch, start_year, end_year)
            if series_data_list:
                all_series_data.extend(series_data_list)
                print(f"    Retrieved {len(series_data_list)} series with data")

            # Update state after each batch
            completed_ids.update(batch)
            save_state("series_data", {
                "completed_series": list(completed_ids),
                "series_data": all_series_data
            })

        except Exception as e:
            print(f"    Error in batch: {e}")
            # Save progress and continue
            save_state("series_data", {
                "completed_series": list(completed_ids),
                "series_data": all_series_data
            })

    print(f"  Total: {len(all_series_data)} series with data")

    save_raw_json({
        "series": all_series_data,
        "start_year": start_year,
        "end_year": end_year
    }, "series_data")

    # Clear incremental state
    save_state("series_data", {"completed": True})


if __name__ == "__main__":
    run()
