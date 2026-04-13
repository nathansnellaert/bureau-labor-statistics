"""Load BLS series catalog from bundled text file.

The series_catalog.txt file contains ~10K pre-selected series IDs (top 500-2000 per survey).
This avoids needing to crawl BLS Data Finder or rely on the limited popular_series API.
"""

import os

from subsets_utils import save_raw_json, load_raw_json


def run():
    """Load series_catalog.txt and save as series_catalog.json."""
    # Skip if catalog already exists
    try:
        existing = load_raw_json("series_catalog")
        if existing:
            print(f"  series_catalog.json already exists ({len(existing):,} series), skipping")
            return
    except FileNotFoundError:
        pass

    # Text file is in the connector root directory (sibling to src/)
    connector_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    txt_path = os.path.join(connector_root, "series_catalog.txt")

    if not os.path.exists(txt_path):
        print(f"  No series_catalog.txt found, skipping catalog generation")
        print(f"  (series_data will fall back to popular_series API)")
        return

    print(f"  Loading series from {txt_path}")

    all_series = []
    with open(txt_path, 'r', encoding='utf-8') as f:
        for rank, line in enumerate(f, 1):
            series_id = line.strip()
            if series_id:
                all_series.append({
                    'rank': rank,
                    'series_id': series_id,
                    'survey_prefix': series_id[:2]
                })

    print(f"  Loaded {len(all_series):,} series")
    save_raw_json(all_series, "series_catalog")


NODES = {
    run: [],
}


if __name__ == "__main__":
    run()
