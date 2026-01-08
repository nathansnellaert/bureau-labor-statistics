"""BLS Data Connector - fetches and transforms Bureau of Labor Statistics data.

Data flow:
1. series_catalog: Use existing catalog if available, otherwise fetch popular_series
2. series_data: Fetch top N series per survey via BLS API with catalog metadata
3. transform: Split into topic-based datasets (bls_consumer_prices, bls_unemployment_local, etc.)

The series_catalog step is optional - if not available, series_data falls back to the
popular_series API endpoint (fewer series but no crawling required).

Note: BLS API has a daily request limit (~500 requests/day for registered users).
If the limit is hit, the connector saves progress and exits with code 2 to signal
that another run is needed tomorrow to continue fetching data.
"""

import argparse
import os
import sys

os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from subsets_utils import validate_environment, load_raw_json
from ingest import series_catalog as ingest_series_catalog
from ingest import surveys as ingest_surveys
from ingest import popular_series as ingest_popular_series
from ingest import series_data as ingest_series_data
from transforms.series_data import main as transform_series_data


def catalog_exists() -> bool:
    """Check if series_catalog.json exists in raw data."""
    try:
        load_raw_json("series_catalog")
        return True
    except FileNotFoundError:
        return False


def main():
    parser = argparse.ArgumentParser(description="BLS Data Connector")
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from API")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    parser.add_argument("--skip-catalog", action="store_true", help="Skip series catalog crawl (use popular_series fallback)")
    args = parser.parse_args()

    validate_environment(['BLS_API_KEY'])

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only
    needs_continuation = False

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")

        # Try to use series_catalog if available
        if not args.skip_catalog:
            print("\n--- Series Catalog ---")
            ingest_series_catalog.run()

        # Always fetch surveys + popular_series (used as fallback for surveys missing from catalog)
        print("\n--- Surveys (fetching survey list) ---")
        ingest_surveys.run()

        print("\n--- Popular Series (fetching from BLS API) ---")
        ingest_popular_series.run()

        print("\n--- Series Data (fetching from BLS API) ---")
        needs_continuation = ingest_series_data.run() or False

    if should_transform:
        print("\n=== Phase 2: Transform ===")

        print("\n--- Series Data (splitting into topic datasets) ---")
        transform_series_data.run()

    # Signal continuation if API quota was hit and more data remains
    if needs_continuation:
        print("\n=== Continuation needed (daily API quota reached) ===")
        print("Run again tomorrow to fetch remaining series")
        sys.exit(2)


if __name__ == "__main__":
    main()
