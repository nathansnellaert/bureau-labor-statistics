import os

from subsets_utils import load_raw_json, save_raw_json
from bls_client import rate_limited_get

API_KEY = os.environ['BLS_API_KEY']
POPULAR_URL = "https://api.bls.gov/publicAPI/v2/timeseries/popular"


def run():
    """Fetch popular series for each survey and save raw JSON"""
    # Load surveys to know which surveys to query
    surveys = load_raw_json("surveys")
    survey_ids = [s.get("survey_abbreviation") for s in surveys if s.get("survey_abbreviation")]

    all_data = {"overall": [], "by_survey": {}}

    # First get overall popular series (top 25 across all surveys)
    print("  Fetching overall popular series...")
    params = {"registrationkey": API_KEY}
    response = rate_limited_get(POPULAR_URL, params=params)
    data = response.json()

    if data.get("status") == "REQUEST_SUCCEEDED":
        all_data["overall"] = data.get("Results", {}).get("series", [])
        print(f"    Found {len(all_data['overall'])} overall popular series")

    # Then get popular series for each survey
    print(f"  Fetching popular series for {len(survey_ids)} surveys...")
    for survey_id in survey_ids:
        params = {
            "survey": survey_id,
            "registrationkey": API_KEY
        }

        response = rate_limited_get(POPULAR_URL, params=params)
        data = response.json()

        if data.get("status") == "REQUEST_SUCCEEDED":
            series_list = data.get("Results", {}).get("series", [])
            if series_list:
                all_data["by_survey"][survey_id] = series_list

    total_by_survey = sum(len(s) for s in all_data["by_survey"].values())
    print(f"  Total: {len(all_data['overall'])} overall + {total_by_survey} by survey")

    save_raw_json(all_data, "popular_series")
