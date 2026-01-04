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


def select_series_from_catalog(catalog: list[dict], per_survey: int) -> list[str]:
    """Select top N series per survey prefix from the catalog.

    Args:
        catalog: List of series dicts with rank, series_id, survey_prefix
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
        # Already sorted by rank from catalog
        top_n = series_list[:per_survey]
        selected.extend(s['series_id'] for s in top_n)

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
    # Try to load from series_catalog first, fall back to popular_series
    try:
        catalog = load_raw_json("series_catalog")
        series_ids = select_series_from_catalog(catalog, SERIES_PER_SURVEY)
        print(f"  Selected {len(series_ids)} series from catalog ({SERIES_PER_SURVEY} per survey)")
    except FileNotFoundError:
        print("  No series_catalog found, falling back to popular_series")
        popular_data = load_raw_json("popular_series")
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
