"""Fetch all available BLS surveys.

The survey list rarely changes; we cache it on disk and skip subsequent runs.
"""

from subsets_utils import save_raw_json, load_raw_json
from connector_utils import fetch_surveys, DailyQuotaExceeded


def run():
    try:
        existing = load_raw_json("surveys")
        if existing:
            print(f"  surveys.json already exists ({len(existing)} surveys), skipping")
            return
    except FileNotFoundError:
        pass

    print("  Fetching all BLS surveys...")
    try:
        surveys = fetch_surveys()
    except DailyQuotaExceeded as e:
        print(f"  Daily API quota exceeded, skipping surveys fetch: {e}")
        return

    if not surveys:
        raise ValueError("No surveys data retrieved from BLS API")

    print(f"  Found {len(surveys)} surveys")
    save_raw_json(surveys, "surveys")


NODES = {
    run: [],
}


if __name__ == "__main__":
    run()
