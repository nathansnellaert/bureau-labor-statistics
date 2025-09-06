import os
import pyarrow as pa
from utils import load_state, save_state

# Hardcoded list of popular BLS series based on their documentation
# These are the most commonly requested series from BLS
POPULAR_SERIES = [
    # Consumer Price Index (CPI)
    {"series_id": "CUUR0000SA0", "survey": "CU", "description": "CPI-U: All items, U.S. city average, seasonally adjusted"},
    {"series_id": "CUSR0000SA0", "survey": "CU", "description": "CPI-U: All items, U.S. city average, not seasonally adjusted"},
    {"series_id": "CUUR0000SA0L1E", "survey": "CU", "description": "CPI-U: All items less food and energy, seasonally adjusted"},
    {"series_id": "CUUR0000SAF1", "survey": "CU", "description": "CPI-U: Food, seasonally adjusted"},
    {"series_id": "CUUR0000SAH", "survey": "CU", "description": "CPI-U: Housing, seasonally adjusted"},
    {"series_id": "CUUR0000SETA01", "survey": "CU", "description": "CPI-U: Gasoline (all types), seasonally adjusted"},
    
    # Employment
    {"series_id": "LNS14000000", "survey": "LN", "description": "Unemployment rate, seasonally adjusted"},
    {"series_id": "CES0000000001", "survey": "CE", "description": "All employees, thousands, total nonfarm, seasonally adjusted"},
    {"series_id": "CES0500000003", "survey": "CE", "description": "Average hourly earnings, total private, seasonally adjusted"},
    {"series_id": "LNS11300000", "survey": "LN", "description": "Labor force participation rate, seasonally adjusted"},
    
    # Producer Price Index (PPI)
    {"series_id": "WPUFD4", "survey": "WP", "description": "PPI: Final demand"},
    {"series_id": "WPSFD4", "survey": "WP", "description": "PPI: Final demand, seasonally adjusted"},
    
    # Employment Cost Index
    {"series_id": "CIS1010000000000I", "survey": "CI", "description": "Employment Cost Index: Total compensation for private industry"},
    {"series_id": "CIS2010000000000I", "survey": "CI", "description": "Employment Cost Index: Wages and salaries for private industry"},
    
    # Productivity
    {"series_id": "PRS85006092", "survey": "PR", "description": "Nonfarm business labor productivity (output per hour)"},
    {"series_id": "PRS85006112", "survey": "PR", "description": "Nonfarm business unit labor costs"},
]

def process_popular_series(surveys_table):
    """Return hardcoded popular series since the API doesn't have a popular endpoint."""
    state = load_state("popular_series")
    
    # Create records with popularity rank
    all_records = []
    for rank, series_info in enumerate(POPULAR_SERIES, 1):
        record = {
            "series_id": series_info["series_id"],
            "survey": series_info["survey"],
            "popularity_rank": rank,
        }
        all_records.append(record)
    
    # Convert to PyArrow table
    table = pa.Table.from_pylist(all_records)
    
    # Update state
    save_state("popular_series", {
        "series_count": len(all_records),
        "source": "hardcoded_popular_series"
    })
    
    return table