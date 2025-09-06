import os
os.environ['CONNECTOR_NAME'] = 'bureau-labor-statistics'
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from utils import validate_environment, upload_data
from assets.surveys.surveys import process_surveys
from assets.popular_series.popular_series import process_popular_series
from assets.series_data.series_data import process_series_data

def main():
    validate_environment(['BLS_API_KEY'])
    
    # DAG: surveys -> popular_series -> series_data
    
    # First, get all available surveys
    surveys_data = process_surveys()
    upload_data(surveys_data, "surveys")
    
    # Get popular series for each survey
    popular_series_data = process_popular_series(surveys_data)
    upload_data(popular_series_data, "popular_series")
    
    # Fetch actual time series data for popular series
    series_data = process_series_data(popular_series_data)
    upload_data(series_data, "series_data")

if __name__ == "__main__":
    main()