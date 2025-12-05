import os

from subsets_utils import get, save_raw_json

API_KEY = os.environ['BLS_API_KEY']
SURVEYS_URL = "https://api.bls.gov/publicAPI/v2/surveys"


def run():
    """Fetch all available BLS surveys and save raw JSON"""
    print("  Fetching all BLS surveys...")

    params = {"registrationkey": API_KEY}
    response = get(SURVEYS_URL, params=params)
    data = response.json()

    if data.get("status") != "REQUEST_SUCCEEDED":
        raise ValueError(f"API error: {data.get('message', 'Unknown error')}")

    results = data.get("Results", {})
    surveys = results.get("survey", [])

    if not surveys:
        raise ValueError("No surveys data retrieved from BLS API")

    print(f"  Found {len(surveys)} surveys")

    save_raw_json(surveys, "surveys")
