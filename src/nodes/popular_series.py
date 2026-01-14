"""Fetch popular series for each survey from BLS API.

This node fetches the list of popular series (top 25 overall + top per survey)
from the BLS API. Used as fallback for surveys missing from the series catalog.
"""

import os

from subsets_utils import load_raw_json, save_raw_json
from utils import rate_limited_get

POPULAR_URL = "https://api.bls.gov/publicAPI/v2/timeseries/popular"


def _get_api_key():
    return os.environ['BLS__get_api_key()']


def run():
    """Fetch popular series for each survey and save raw JSON."""
    # Skip if popular_series already exists (rarely changes)
    try:
        existing = load_raw_json("popular_series")
        if existing and (existing.get("overall") or existing.get("by_survey")):
            total = len(existing.get("overall", [])) + sum(len(s) for s in existing.get("by_survey", {}).values())
            print(f"  popular_series.json already exists ({total} series), skipping")
            return
    except FileNotFoundError:
        pass

    # Load surveys to know which surveys to query
    try:
        surveys = load_raw_json("surveys")
    except FileNotFoundError:
        print("  surveys.json not found, skipping popular_series fetch")
        return

    survey_ids = [s.get("survey_abbreviation") for s in surveys if s.get("survey_abbreviation")]

    all_data = {"overall": [], "by_survey": {}}

    # First get overall popular series (top 25 across all surveys)
    print("  Fetching overall popular series...")
    params = {"registrationkey": _get_api_key()}
    response = rate_limited_get(POPULAR_URL, params=params)
    data = response.json()

    # Check for quota limit
    message = data.get("message", [])
    message_str = message[0] if isinstance(message, list) and message else str(message)
    if "daily threshold" in message_str.lower():
        print(f"  Daily API quota exceeded, skipping popular_series fetch")
        return

    if data.get("status") == "REQUEST_SUCCEEDED":
        all_data["overall"] = data.get("Results", {}).get("series", [])
        print(f"    Found {len(all_data['overall'])} overall popular series")

    # Then get popular series for each survey
    print(f"  Fetching popular series for {len(survey_ids)} surveys...")
    for survey_id in survey_ids:
        params = {
            "survey": survey_id,
            "registrationkey": _get_api_key()
        }

        response = rate_limited_get(POPULAR_URL, params=params)
        data = response.json()

        # Check for quota limit
        message = data.get("message", [])
        message_str = message[0] if isinstance(message, list) and message else str(message)
        if "daily threshold" in message_str.lower():
            print(f"  Daily API quota exceeded at survey {survey_id}")
            break

        if data.get("status") == "REQUEST_SUCCEEDED":
            series_list = data.get("Results", {}).get("series", [])
            if series_list:
                all_data["by_survey"][survey_id] = series_list

    total_by_survey = sum(len(s) for s in all_data["by_survey"].values())
    print(f"  Total: {len(all_data['overall'])} overall + {total_by_survey} by survey")

    save_raw_json(all_data, "popular_series")


from nodes.surveys import run as surveys_run

NODES = {
    run: [surveys_run],
}


if __name__ == "__main__":
    run()
