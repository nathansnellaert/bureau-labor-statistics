import os

# Set environment variables for this run
os.environ['CONNECTOR_NAME'] = 'bureau-labor-statistics'
os.environ['RUN_ID'] = 'debug-run'
os.environ['ENABLE_HTTP_CACHE'] = 'true'
os.environ['CATALOG_TYPE'] = 'local'
os.environ['DATA_DIR'] = 'data'

# Check if API key is set
if 'BLS_API_KEY' not in os.environ:
    print("ERROR: BLS_API_KEY environment variable not set")
    print("Please set it before running this test")
    exit(1)

# Simple test of the full connector with our fixes
from main import main

try:
    print("Running BLS connector with improved error handling...")
    main()
    print("\n✅ Connector completed successfully!")
except Exception as e:
    print(f"\n❌ Connector failed: {e}")
    import traceback
    traceback.print_exc()