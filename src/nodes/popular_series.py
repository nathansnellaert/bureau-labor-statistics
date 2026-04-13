"""Fetch popular series for each survey from BLS API.

Used as fallback for surveys missing from the curated series catalog.
"""

from subsets_utils import load_raw_json, save_raw_json
from connector_utils import fetch_popular_series, DailyQuotaExceeded


def run():
    try:
        existing = load_raw_json("popular_series")
        if existing and (existing.get("overall") or existing.get("by_survey")):
            total = len(existing.get("overall", [])) + sum(len(s) for s in existing.get("by_survey", {}).values())
            print(f"  popular_series.json already exists ({total} series), skipping")
            return
    except FileNotFoundError:
        pass

    try:
        surveys = load_raw_json("surveys")
    except FileNotFoundError:
        print("  surveys.json not found, skipping popular_series fetch")
        return

    survey_ids = [s.get("survey_abbreviation") for s in surveys if s.get("survey_abbreviation")]

    all_data = {"overall": [], "by_survey": {}}

    print("  Fetching overall popular series...")
    try:
        all_data["overall"] = fetch_popular_series()
    except DailyQuotaExceeded as e:
        print(f"  Daily API quota exceeded, skipping popular_series fetch: {e}")
        return
    print(f"    Found {len(all_data['overall'])} overall popular series")

    print(f"  Fetching popular series for {len(survey_ids)} surveys...")
    for survey_id in survey_ids:
        try:
            series_list = fetch_popular_series(survey=survey_id)
        except DailyQuotaExceeded as e:
            print(f"  Daily API quota exceeded at survey {survey_id}: {e}")
            break
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
