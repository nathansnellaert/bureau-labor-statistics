import os
from datetime import datetime
import pyarrow as pa
import pyarrow.compute as pc
from ratelimit import limits, sleep_and_retry
from utils import post, load_state, save_state

API_KEY = os.environ['BLS_API_KEY']
API_BASE_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

def parse_value(value):
    """Parse value from BLS API, handling special cases like '-' for missing data."""
    if value == '-' or value == '' or value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

@sleep_and_retry
@limits(calls=50, period=1)  # BLS allows 50 requests per query with registered key
def fetch_series_batch(series_ids, start_year, end_year):
    """Fetch data for a batch of series from BLS API."""
    headers = {"Content-type": "application/json"}
    json_data = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": API_KEY,
        "catalog": True  # Request catalog information
    }
    
    print(f"    Fetching {len(series_ids)} series for years {start_year}-{end_year}")
    response = post(API_BASE_URL, headers=headers, json=json_data)
    
    # Check HTTP status code
    response.raise_for_status()
    
    data = response.json()
    
    # Debug: Show what the API returned
    if not data.get("Results", {}).get("series"):
        print(f"    API Status: {data.get('status')}")
        print(f"    API Message: {data.get('message', 'No message')}")
        if "Results" in data:
            print(f"    Results keys: {list(data['Results'].keys())}")
    
    # Check for various API error statuses
    status = data.get("status")
    if status in ["REQUEST_FAILED", "REQUEST_NOT_PROCESSED"]:
        error_msg = f"BLS API error: {data.get('message', 'Unknown error')}"
        print(f"    Full API Response: {data}")
        raise ValueError(error_msg)
    
    return data.get("Results", {}).get("series", [])

def parse_series_data(series_data):
    """Convert raw series data to records."""
    records = []
    
    series_id = series_data.get("seriesID", "")
    catalog = series_data.get("catalog", {})
    
    # Extract metadata from catalog if available
    series_title = catalog.get("series_title", "")
    survey_name = catalog.get("survey_name", "")
    survey_abbreviation = catalog.get("survey_abbreviation", "")
    seasonality = catalog.get("seasonality", "")
    
    print(f"    Processing series {series_id}: {series_title[:50] if series_title else 'No title'}...")
    
    for entry in series_data.get("data", []):
        year = entry.get("year", "")
        period = entry.get("period", "")
        
        # Convert period to date
        if period.startswith("M"):
            month = int(period[1:])
            date = f"{year}-{month:02d}-01"
        elif period.startswith("Q"):
            quarter = int(period[1:])
            month = (quarter - 1) * 3 + 1
            date = f"{year}-{month:02d}-01"
        elif period == "A01":
            date = f"{year}-01-01"
        elif period.startswith("S"):  # Semi-annual
            half = int(period[1:])
            month = 1 if half == 1 else 7
            date = f"{year}-{month:02d}-01"
        else:
            continue
        
        # Handle footnotes safely - ensure we get a list and text values are strings
        footnotes_data = entry.get("footnotes")
        if footnotes_data and isinstance(footnotes_data, list):
            footnote_texts = []
            for fn in footnotes_data:
                if isinstance(fn, dict):
                    text = fn.get("text", "")
                    # Ensure text is a string
                    if text is not None and not isinstance(text, str):
                        text = str(text)
                    elif text is None:
                        text = ""
                    footnote_texts.append(text)
            footnotes_str = " ".join(footnote_texts)
        else:
            footnotes_str = ""
        
        # Handle 'latest' field - ensure it's always a boolean
        latest_value = entry.get("latest", False)
        if isinstance(latest_value, str):
            # Convert string values to boolean
            latest_bool = latest_value.lower() in ['true', 'yes', '1']
        elif isinstance(latest_value, bool):
            latest_bool = latest_value
        else:
            latest_bool = False
            
        record = {
            "date": date,
            "series_id": series_id,
            "series_title": series_title,
            "survey_name": survey_name,
            "survey_abbreviation": survey_abbreviation,
            "seasonality": seasonality,
            "value": parse_value(entry.get("value", 0)),
            "year": int(year),
            "month": int(period[1:]) if period.startswith("M") else None,
            "quarter": int(period[1:]) if period.startswith("Q") else None,
            "period": period,
            "period_name": entry.get("periodName", ""),
            "latest": latest_bool,
            "footnotes": footnotes_str,
        }
        
        records.append(record)
    
    return records

def process_series_data(popular_series_table):
    """Process time series data for all popular series."""
    state = load_state("series_data")
    
    # Get unique series IDs from popular series
    series_ids = pc.unique(popular_series_table["series_id"]).to_pylist()
    
    if not series_ids:
        # Return empty table with proper schema
        schema = pa.schema([
            ("date", pa.string()),
            ("series_id", pa.string()),
            ("series_title", pa.string()),
            ("survey_name", pa.string()),
            ("survey_abbreviation", pa.string()),
            ("seasonality", pa.string()),
            ("value", pa.float64()),
            ("year", pa.int64()),
            ("month", pa.int64()),
            ("quarter", pa.int64()),
            ("period", pa.string()),
            ("period_name", pa.string()),
            ("latest", pa.bool_()),
            ("footnotes", pa.string())
        ])
        return pa.Table.from_pylist([], schema=schema)
    
    # Determine date range
    # BLS data is usually available up to the previous month
    # If we're early in the year, data for the current year might not be available yet
    current_date = datetime.now()
    if current_date.month <= 2:  # If January or February, use previous year as end
        end_year = current_date.year - 1
    else:
        end_year = current_date.year
    start_year = end_year - 19  # Get 20 years of data
    
    all_records = []
    batch_size = 50  # BLS API limit for registered users
    
    print(f"Processing {len(series_ids)} series")
    
    # Process in batches
    for i in range(0, len(series_ids), batch_size):
        batch = series_ids[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(series_ids) + batch_size - 1) // batch_size
        print(f"  Batch {batch_num}/{total_batches}")
        
        # BLS API has a 20-year limit per request
        year_ranges = []
        current_start = start_year
        while current_start <= end_year:
            current_end = min(current_start + 19, end_year)
            year_ranges.append((current_start, current_end))
            current_start = current_end + 1
        
        for year_start, year_end in year_ranges:
            try:
                series_data_list = fetch_series_batch(batch, year_start, year_end)
                
                if series_data_list:  # Only process if we got data
                    for series_data in series_data_list:
                        try:
                            records = parse_series_data(series_data)
                            all_records.extend(records)
                        except Exception as e:
                            series_id = series_data.get("seriesID", "unknown")
                            print(f"    ERROR parsing series {series_id}: {e}")
                            # Re-raise with more context
                            raise ValueError(f"Failed to parse series {series_id}: {e}")
                    print(f"    Retrieved {len(series_data_list)} series")
                else:
                    print(f"    No data returned for years {year_start}-{year_end}")
            except Exception as e:
                print(f"    Error fetching batch: {e}")
                # Don't continue, re-raise to see the actual error
                raise
    
    if not all_records:
        raise ValueError("No time series data retrieved from BLS API")
    
    # Convert to PyArrow table
    table = pa.Table.from_pylist(all_records)
    
    # Sort by date and series
    sort_indices = pc.sort_indices(table, [("date", "ascending"), ("series_id", "ascending")])
    table = pc.take(table, sort_indices)
    
    # Update state
    if len(all_records) > 0:
        latest_date = max(r["date"] for r in all_records)
        unique_series = len(set(r["series_id"] for r in all_records))
        save_state("series_data", {
            "last_updated": latest_date,
            "series_count": unique_series,
            "record_count": len(all_records)
        })
    
    return table