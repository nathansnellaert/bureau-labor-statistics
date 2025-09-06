import os
import pyarrow as pa
import pyarrow.compute as pc
from utils import get, load_state, save_state

API_KEY = os.environ['BLS_API_KEY']
POPULAR_URL = "https://api.bls.gov/publicAPI/v2/timeseries/popular"

def process_popular_series(surveys_table):
    """Fetch popular series for each survey."""
    state = load_state("popular_series")
    
    all_records = []
    
    # First get overall popular series (top 25 across all surveys)
    params = {"registrationkey": API_KEY}
    try:
        response = get(POPULAR_URL, params=params)
        data = response.json()
        
        if data.get("status") == "REQUEST_SUCCEEDED":
            series_list = data.get("Results", {}).get("series", [])
            
            for rank, series in enumerate(series_list, 1):
                record = {
                    "series_id": series.get("seriesID", ""),
                    "survey": "ALL",  # Overall popular series
                    "popularity_rank": rank,
                }
                all_records.append(record)
    except Exception as e:
        print(f"Warning: Could not fetch overall popular series: {e}")
    
    # Then get popular series for each survey
    survey_ids = pc.unique(surveys_table["survey_abbreviation"]).to_pylist()
    
    for survey_id in survey_ids:
        if not survey_id:  # Skip empty survey IDs
            continue
            
        params = {
            "survey": survey_id,
            "registrationkey": API_KEY
        }
        
        try:
            response = get(POPULAR_URL, params=params)
            
            # Check if response contains JSON
            if response.headers.get('content-type', '').startswith('application/json'):
                data = response.json()
            else:
                print(f"Warning: Survey {survey_id} returned non-JSON response (content-type: {response.headers.get('content-type')})")
                continue
            
            if data.get("status") == "REQUEST_SUCCEEDED":
                results = data.get("Results")
                if results and isinstance(results, dict):
                    series_list = results.get("series", [])
                    
                    # Only process if we have series data
                    if series_list:
                        for rank, series in enumerate(series_list, 1):
                            # Skip null entries
                            if series is None:
                                continue
                            record = {
                                "series_id": series.get("seriesID", ""),
                                "survey": survey_id,
                                "popularity_rank": rank,
                            }
                            all_records.append(record)
                    # No error message for surveys without popular series - this is normal
        except Exception as e:
            # Only print actual errors, not missing data
            if "NoneType" not in str(e):
                print(f"Error fetching popular series for survey {survey_id}: {e}")
            continue
    
    if not all_records:
        # Return empty table with proper schema
        schema = pa.schema([
            ("series_id", pa.string()),
            ("survey", pa.string()),
            ("popularity_rank", pa.int64())
        ])
        return pa.Table.from_pylist([], schema=schema)
    
    # Convert to PyArrow table
    table = pa.Table.from_pylist(all_records)
    
    # Remove duplicates (keeping the one with highest rank/lowest number)
    # Sort by series_id and popularity_rank
    sort_indices = pc.sort_indices(table, [("series_id", "ascending"), ("popularity_rank", "ascending")])
    table = pc.take(table, sort_indices)
    
    # Get unique series IDs
    unique_series = []
    seen_ids = set()
    for record in table.to_pylist():
        if record["series_id"] not in seen_ids:
            unique_series.append(record)
            seen_ids.add(record["series_id"])
    
    table = pa.Table.from_pylist(unique_series)
    
    # Update state
    save_state("popular_series", {
        "series_count": len(unique_series),
        "survey_count": len(survey_ids)
    })
    
    return table