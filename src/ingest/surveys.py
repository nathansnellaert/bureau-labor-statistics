import os

from subsets_utils import get, save_raw_json, load_raw_json

API_KEY = os.environ['BLS_API_KEY']
SURVEYS_URL = "https://api.bls.gov/publicAPI/v2/surveys"


def run():
    """Fetch all available BLS surveys and save raw JSON"""
    # Skip if surveys already exist (rarely changes)
    try:
        existing = load_raw_json("surveys")
        if existing:
            print(f"  surveys.json already exists ({len(existing)} surveys), skipping")
            return
    except FileNotFoundError:
        pass

    print("  Fetching all BLS surveys...")

    params = {"registrationkey": API_KEY}
    response = get(SURVEYS_URL, params=params)
    data = response.json()

    status = data.get("status")
    message = data.get("message", ["Unknown error"])
    message_str = message[0] if isinstance(message, list) else str(message)

    # Handle daily quota limit gracefully
    if "daily threshold" in message_str.lower():
        print(f"  Daily API quota exceeded, skipping surveys fetch")
        return

    if status != "REQUEST_SUCCEEDED":
        raise ValueError(f"API error: {message_str}")

    results = data.get("Results", {})
    surveys = results.get("survey", [])

    if not surveys:
        raise ValueError("No surveys data retrieved from BLS API")

    print(f"  Found {len(surveys)} surveys")

    save_raw_json(surveys, "surveys")
