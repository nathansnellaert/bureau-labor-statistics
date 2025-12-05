"""Validation for BLS series data datasets (long format)."""

import pyarrow as pa
from subsets_utils import validate


def test(table: pa.Table, dataset_id: str) -> None:
    """Validate BLS series data output.

    Args:
        table: PyArrow table to validate
        dataset_id: Dataset identifier for context in error messages
    """
    # Must have required columns
    required = ["date", "indicator", "value"]
    for col in required:
        assert col in table.schema.names, f"{dataset_id}: Missing {col} column"

    # Basic validation
    validate(table, {
        "not_null": ["date", "indicator", "value"],
        "min_rows": 1,
    })

    # Date format validation
    dates = table.column("date").to_pylist()
    for d in dates[:100]:
        assert d and len(d) >= 4, f"Invalid date format: {d}"
        year_part = d[:4]
        assert year_part.isdigit(), f"Date should start with year: {d}"
        year = int(year_part)
        assert 1900 <= year <= 2030, f"Year out of range: {year}"

    # Value column should be numeric
    value_col = table.column("value")
    assert pa.types.is_floating(value_col.type), f"{dataset_id}: value should be float"

    # Should have multiple indicators
    indicators = set(table.column("indicator").to_pylist())
    assert len(indicators) >= 1, f"{dataset_id}: Should have at least 1 indicator"

    print(f"    Validated {dataset_id}: {len(table):,} rows, {len(indicators)} indicators")
