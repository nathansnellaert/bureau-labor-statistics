import os
import pyarrow as pa
from utils import get, load_state, save_state

API_KEY = os.environ['BLS_API_KEY']
SURVEYS_URL = "https://api.bls.gov/publicAPI/v2/surveys"

def process_surveys():
    """Fetch all available BLS surveys."""
    state = load_state("surveys")
    
    # Get all surveys
    params = {"registrationkey": API_KEY}
    response = get(SURVEYS_URL, params=params)
    data = response.json()
    
    if data.get("status") != "REQUEST_SUCCEEDED":
        raise ValueError(f"API error: {data.get('message', 'Unknown error')}")
    
    results = data.get("Results", {})
    surveys = results.get("survey", [])
    
    if not surveys:
        raise ValueError("No surveys data retrieved from BLS API")
    
    # Convert to records format
    records = []
    for survey in surveys:
        print(survey)
        record = {
            "survey_abbreviation": survey.get("survey_abbreviation", ""),
            "survey_name": survey.get("survey_name", ""),
        }
        records.append(record)
    
    # Convert to PyArrow table
    table = pa.Table.from_pylist(records)
    
    # Update state
    save_state("surveys", {
        "survey_count": len(records),
        "survey_ids": [r["survey_abbreviation"] for r in records]
    })
    
    return table