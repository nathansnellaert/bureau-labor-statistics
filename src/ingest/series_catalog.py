"""Convert existing BLS series CSV to JSON catalog format.

The bls_series.csv file was previously crawled from BLS Data Finder and contains
~1M series ranked by popularity. This module converts it to standard JSON format.
"""

import csv
import os

from subsets_utils import save_raw_json, get_data_dir


def run():
    """Convert bls_series.csv to series_catalog.json."""
    # CSV is in the connector root directory (sibling to src/)
    connector_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    csv_path = os.path.join(connector_root, "bls_series.csv")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Series CSV not found at {csv_path}")

    print(f"  Loading series from {csv_path}")

    all_series = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            series_id = row.get('Series ID', '')
            all_series.append({
                'rank': int(row.get('Rank', 0)),
                'series_id': series_id,
                'series_title': row.get('Series Title', ''),
                'survey_prefix': series_id[:2] if series_id else ''
            })

    print(f"  Loaded {len(all_series):,} series")
    save_raw_json(all_series, "series_catalog")


if __name__ == "__main__":
    run()
