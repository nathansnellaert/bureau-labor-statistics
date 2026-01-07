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
# Format: (dataset_suffix, description)
SURVEY_TOPICS = {
    "CU": ("consumer_prices", "Inflation for urban consumers - measures price changes for goods and services purchased by households"),
    "CW": ("consumer_prices_workers", "Inflation for urban wage earners and clerical workers - subset of CPI for working households"),
    "SU": ("consumer_prices_chained", "Chained inflation index - accounts for consumer substitution when prices change"),
    "AP": ("average_prices", "Retail prices for common consumer goods like food, fuel, and household items"),
    "WP": ("producer_prices_commodities", "Wholesale prices for raw materials and commodities before retail markup"),
    "PC": ("producer_prices_industry", "Prices received by domestic producers for their output by industry"),
    "CE": ("employment_national", "Jobs, hours, and earnings across US industries from employer payroll data"),
    "SM": ("employment_state", "Jobs and wages by state and metro area from employer payroll data"),
    "LA": ("unemployment_local", "Unemployment rates for states, counties, and metro areas"),
    "LN": ("labor_force", "Civilian labor force participation, employment, and unemployment from household survey"),
    "LE": ("employment_situation", "Monthly employment and unemployment by demographics (age, gender, race)"),
    "LU": ("union_membership", "Workers represented by unions and covered by collective bargaining"),
    "BD": ("business_dynamics", "Job creation and destruction from business openings, closings, and expansions"),
    "JT": ("job_openings", "Job openings, hires, quits, and layoffs from employer surveys (JOLTS)"),
    "CI": ("employment_cost_index", "Changes in employer labor costs including wages and benefits"),
    "CM": ("employer_costs", "Employer spending on wages, salaries, and benefits per hour worked"),
    "OE": ("occupational_employment", "Employment counts and wages by detailed occupation"),
    "WM": ("modeled_wages", "Wage estimates for occupations in areas with limited survey data"),
    "TU": ("time_use", "How Americans spend their time - work, leisure, childcare, household activities"),
    "PR": ("productivity", "Output per hour worked and unit labor costs by sector"),
    "IP": ("international_prices", "Price changes for goods entering and leaving the US (imports/exports)"),
    "EI": ("employment_projections", "10-year forecasts of employment by occupation and industry"),
    "CX": ("consumer_expenditure", "How households allocate spending across categories"),
    "FM": ("mass_layoffs", "Large-scale layoff events and workers affected (discontinued 2013)"),
    "OR": ("occupational_requirements", "Physical demands and educational requirements by occupation"),
    "EN": ("qcew", "Quarterly employment and wages by county, industry, and ownership from employer tax records"),
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
    "unit",
]


def parse_value(value):
    """Parse numeric value, handling BLS null representations."""
    if value in ('-', '', None):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def extract_unit(series_title: str) -> str:
    """Extract unit from series title."""
    title_lower = series_title.lower()
    if "percent change" in title_lower or "percent of" in title_lower:
        return "percent"
    if "index" in title_lower:
        return "index"
    if "in thousands" in title_lower or "thousands" in title_lower:
        return "thousands"
    if "in millions" in title_lower:
        return "millions"
    if "in dollars" in title_lower or "dollars per" in title_lower:
        return "dollars"
    if "hours" in title_lower and ("average" in title_lower or "weekly" in title_lower):
        return "hours"
    if "rate" in title_lower:
        return "rate"
    return ""


def parse_series_data(series_data: dict) -> list[dict]:
    """Parse a single series with full catalog metadata."""
    records = []
    series_id = series_data.get("seriesID", "")
    catalog = series_data.get("catalog") or {}

    # Use series_title as indicator - it's self-documenting
    series_title = catalog.get("series_title", "")
    unit = extract_unit(series_title)

    series_info = {
        "survey_abbreviation": catalog.get("survey_abbreviation", series_id[:2] if series_id else ""),
        "seasonality": catalog.get("seasonality", ""),
        "area": catalog.get("area", ""),
        "area_type": catalog.get("area_type", ""),
        "indicator": series_title,  # Full descriptive title
        "unit": unit,
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
        "indicator": "Full series title describing what the value represents",
        "value": "Numeric value (see indicator and unit for interpretation)",
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
        "unit": "Unit of measurement (percent, index, thousands, dollars, hours, rate)",
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
