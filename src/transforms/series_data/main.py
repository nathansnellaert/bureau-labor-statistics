"""Transform Bureau of Labor Statistics series data into topic-based datasets.

Splits data by survey_abbreviation. Long format with indicator as a column.
Only includes dimensions that vary within each dataset.
"""

from collections import defaultdict

import pyarrow as pa
import pyarrow.compute as pc
from subsets_utils import load_raw_json, upload_data, publish
from .test import test

# Survey prefix to human-readable topic mapping
SURVEY_TOPICS = {
    "CU": ("consumer_prices", "Consumer Price Index (CPI-U)"),
    "CW": ("consumer_prices_workers", "Consumer Price Index (CPI-W)"),
    "SU": ("consumer_prices_chained", "Chained Consumer Price Index"),
    "AP": ("average_prices", "Average Prices"),
    "WP": ("producer_prices_commodities", "Producer Price Index - Commodities"),
    "PC": ("producer_prices_industry", "Producer Price Index - Industry"),
    "CE": ("employment_national", "Current Employment Statistics (National)"),
    "SM": ("employment_state", "State and Area Employment"),
    "LA": ("unemployment_local", "Local Area Unemployment Statistics"),
    "LN": ("labor_force", "Labor Force Statistics (CPS)"),
    "LE": ("employment_situation", "Employment Situation"),
    "LU": ("union_membership", "Union Membership"),
    "BD": ("business_dynamics", "Business Employment Dynamics"),
    "JT": ("job_openings", "Job Openings and Labor Turnover (JOLTS)"),
    "CI": ("employment_cost_index", "Employment Cost Index"),
    "CM": ("employer_costs", "Employer Costs for Employee Compensation"),
    "OE": ("occupational_employment", "Occupational Employment and Wages"),
    "WM": ("modeled_wages", "Modeled Wage Estimates"),
    "TU": ("time_use", "American Time Use Survey"),
    "PR": ("productivity", "Productivity and Costs"),
    "IP": ("international_prices", "Import/Export Price Indexes"),
    "EI": ("employment_projections", "Employment Projections"),
    "CX": ("consumer_expenditure", "Consumer Expenditure"),
    "FM": ("mass_layoffs", "Mass Layoff Statistics"),
    "OR": ("occupational_requirements", "Occupational Requirements Survey"),
}

# All potential dimension columns
ALL_DIMENSIONS = [
    "date",
    "seasonality",
    "area",
    "area_type",
    "industry",
    "occupation",
    "demographic_age",
    "demographic_gender",
    "demographic_race",
    "demographic_education",
]


def parse_value(value):
    """Parse numeric value, handling BLS null representations."""
    if value in ('-', '', None):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_series_data(series_data: dict) -> list[dict]:
    """Parse a single series with full catalog metadata."""
    records = []
    series_id = series_data.get("seriesID", "")
    catalog = series_data.get("catalog") or {}

    series_info = {
        "survey_abbreviation": catalog.get("survey_abbreviation", series_id[:2] if series_id else ""),
        "seasonality": catalog.get("seasonality", ""),
        "area": catalog.get("area", ""),
        "area_type": catalog.get("area_type", ""),
        "indicator": catalog.get("measure_data_type", ""),
        "industry": catalog.get("commerce_industry", catalog.get("industry", "")),
        "occupation": catalog.get("occupation", ""),
        "demographic_age": catalog.get("demographic_age", ""),
        "demographic_gender": catalog.get("demographic_gender", ""),
        "demographic_race": catalog.get("demographic_race", ""),
        "demographic_education": catalog.get("demographic_education", ""),
    }

    for entry in series_data.get("data", []):
        year_str = entry.get("year", "")
        period = entry.get("period", "")

        # Parse date based on period type
        if period.startswith("M") and period[1:].isdigit():
            month = int(period[1:])
            if month == 13:  # M13 = annual average
                date = f"{year_str}"
            else:
                date = f"{year_str}-{month:02d}"
        elif period.startswith("Q") and period[1:].isdigit():
            quarter = int(period[1:])
            date = f"{year_str}-Q{quarter}"
        elif period == "A01":
            date = f"{year_str}"
        elif period.startswith("S") and period[1:].isdigit():
            half = int(period[1:])
            date = f"{year_str}-H{half}"
        else:
            continue

        value = parse_value(entry.get("value"))
        if value is None:
            continue

        record = {
            **series_info,
            "date": date,
            "value": value,
        }
        records.append(record)

    return records


def find_varying_columns(records: list[dict]) -> list[str]:
    """Find dimension columns that have more than one unique non-empty value."""
    varying = []
    for col in ALL_DIMENSIONS:
        values = set()
        for r in records:
            v = r.get(col, "")
            if v and v != "":
                values.add(v)
        if len(values) > 1:
            varying.append(col)
    return varying


def get_constant_values(records: list[dict]) -> dict:
    """Get constant dimension values for metadata."""
    constants = {}
    for col in ALL_DIMENSIONS:
        if col == "date":
            continue
        values = set()
        for r in records:
            v = r.get(col, "")
            if v and v != "":
                values.add(v)
        if len(values) == 1:
            constants[col] = list(values)[0]
    return constants


def build_schema(varying_cols: list[str]) -> pa.Schema:
    """Build PyArrow schema with only varying columns + indicator + value."""
    fields = []
    for col in varying_cols:
        fields.append((col, pa.string()))
    fields.append(("indicator", pa.string()))
    fields.append(("value", pa.float64()))
    return pa.schema(fields)


def filter_records(records: list[dict], varying_cols: list[str]) -> list[dict]:
    """Keep only relevant columns in records."""
    keep_cols = set(varying_cols) | {"indicator", "value"}
    return [{k: v for k, v in r.items() if k in keep_cols} for r in records]


def make_metadata(dataset_id: str, survey_desc: str, varying_cols: list[str],
                  indicators: list[str], constants: dict) -> dict:
    """Generate metadata for a dataset."""
    desc_parts = [f"Time series data from the Bureau of Labor Statistics {survey_desc} program."]

    if constants:
        const_str = ", ".join(f"{k}={v}" for k, v in constants.items())
        desc_parts.append(f"Filtered to: {const_str}.")

    col_descs = {
        "date": "Date of observation (YYYY, YYYY-MM, YYYY-QN, or YYYY-HN)",
        "indicator": "The measure or metric being reported",
        "value": "Numeric value",
    }

    dim_descriptions = {
        "seasonality": "Seasonal adjustment status",
        "area": "Geographic area name",
        "area_type": "Type of geographic area (nation, state, metro, etc.)",
        "industry": "Industry classification",
        "occupation": "Occupation classification",
        "demographic_age": "Age group",
        "demographic_gender": "Gender",
        "demographic_race": "Race/ethnicity",
        "demographic_education": "Education level",
    }

    for col in varying_cols:
        if col in dim_descriptions:
            col_descs[col] = dim_descriptions[col]

    return {
        "id": dataset_id,
        "title": f"BLS {survey_desc}",
        "description": " ".join(desc_parts),
        "column_descriptions": col_descs,
    }


def run():
    """Transform series data and upload as topic-based datasets."""
    raw_data = load_raw_json("series_data")
    series_list = raw_data.get("series", [])

    # Parse all series and group by survey
    by_survey = defaultdict(list)

    for series_data in series_list:
        records = parse_series_data(series_data)
        if records:
            survey_abbr = records[0].get("survey_abbreviation", "XX")
            by_survey[survey_abbr].extend(records)

    if not by_survey:
        raise ValueError("No series data found")

    print(f"  Parsed {sum(len(r) for r in by_survey.values()):,} records across {len(by_survey)} surveys")

    # Process each survey
    uploaded = 0
    for survey_abbr, records in sorted(by_survey.items()):
        topic_info = SURVEY_TOPICS.get(survey_abbr)
        if not topic_info:
            print(f"  Skipping unknown survey: {survey_abbr}")
            continue

        topic_name, survey_desc = topic_info
        dataset_id = f"bls_{topic_name}"

        # Filter out records with empty indicator
        records = [r for r in records if r.get("indicator")]

        if not records:
            print(f"  Skipping {dataset_id}: no records with indicators")
            continue

        # Find varying dimensions
        varying_cols = find_varying_columns(records)
        if "date" not in varying_cols:
            varying_cols = ["date"] + varying_cols

        # Get constants for metadata
        constants = get_constant_values(records)

        # Get unique indicators
        indicators = sorted(set(r.get("indicator", "") for r in records if r.get("indicator")))

        print(f"  Processing {dataset_id}: {len(records):,} records")
        print(f"    Dimensions: {varying_cols}")
        print(f"    Indicators: {len(indicators)}")

        # Build schema and filter records
        schema = build_schema(varying_cols)
        filtered = filter_records(records, varying_cols)

        # Create table
        table = pa.Table.from_pylist(filtered, schema=schema)

        # Sort by date descending, then indicator
        sort_indices = pc.sort_indices(table, [("date", "descending"), ("indicator", "ascending")])
        table = pc.take(table, sort_indices)

        print(f"    Result: {len(table):,} rows x {len(table.schema)} cols")

        test(table, dataset_id)

        upload_data(table, dataset_id)
        publish(dataset_id, make_metadata(dataset_id, survey_desc, varying_cols, indicators, constants))
        uploaded += 1

    print(f"  Uploaded {uploaded} datasets")


if __name__ == "__main__":
    run()
