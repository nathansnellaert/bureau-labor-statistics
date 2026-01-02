"""Convert existing BLS series CSV to JSON catalog format.

The bls_series.csv file was previously crawled from BLS Data Finder and contains
~1M series ranked by popularity. This module converts it to standard JSON format.

If series_catalog.json already exists in raw data, this step is skipped.
"""

import csv
import os

from subsets_utils import save_raw_json, load_raw_json, get_data_dir


def run():
    """Convert bls_series.csv to series_catalog.json."""
    # Skip if catalog already exists
    try:
        existing = load_raw_json("series_catalog")
        if existing:
            print(f"  series_catalog.json already exists ({len(existing):,} series), skipping")
            return
    except FileNotFoundError:
        pass

    # CSV is in the connector root directory (sibling to src/)
    connector_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    csv_path = os.path.join(connector_root, "bls_series.csv")

    if not os.path.exists(csv_path):
        print(f"  No bls_series.csv found, skipping catalog generation")
        print(f"  (series_data will fall back to popular_series API)")
        return

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
