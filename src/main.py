"""BLS Data Connector - fetches and transforms Bureau of Labor Statistics data.

Data flow:
1. series_catalog: Crawl BLS Data Finder to discover series ranked by popularity
2. series_data: Fetch top N series per survey via BLS API with catalog metadata
3. transform: Split into topic-based datasets (bls_consumer_prices, bls_unemployment_local, etc.)

The series_catalog step is optional - if not run, series_data falls back to the
popular_series API endpoint (fewer series but no crawling required).
"""

import argparse
import os

os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from subsets_utils import validate_environment
from ingest import series_catalog as ingest_series_catalog
from ingest import series_data as ingest_series_data
from transforms.series_data import main as transform_series_data


def main():
    parser = argparse.ArgumentParser(description="BLS Data Connector")
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from API")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    parser.add_argument("--skip-catalog", action="store_true", help="Skip series catalog crawl (use popular_series fallback)")
    args = parser.parse_args()

    validate_environment(['BLS_API_KEY'])

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")

        if not args.skip_catalog:
            print("\n--- Series Catalog (crawling BLS Data Finder) ---")
            ingest_series_catalog.run()

        print("\n--- Series Data (fetching from BLS API) ---")
        ingest_series_data.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")

        print("\n--- Series Data (splitting into topic datasets) ---")
        transform_series_data.run()


if __name__ == "__main__":
    main()
