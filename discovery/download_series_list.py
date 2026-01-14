import requests
from bs4 import BeautifulSoup
import csv
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# The base URL for the BLS data query
BASE_URL = "https://data.bls.gov/dataQuery/find"
# Set the number of parallel requests
MAX_WORKERS = 5

def fetch_page_data(start_index):
    """
    Fetches and parses a single page of BLS series data.

    Args:
        start_index (int): The starting record index for the page.

    Returns:
        list: A list of dictionaries, where each dictionary represents a series.
              Returns an empty list if the page has no results or if an error occurs.
    """
    params = {
        'st': start_index,
        'r': 100,  # Records per page
        'fq': 'mg:[Measure+Attributes]',
        'more': 0,
        'popularity': 'D'  # Sort by popularity
    }
    
    page_data = []
    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)

        soup = BeautifulSoup(response.content, 'html.parser')
        results = soup.find_all('div', class_='dq-result-item')

        if not results:
            return []  # No more results

        # Process each result on the page
        for i, item in enumerate(results):
            series_id_input = item.find('input', type='checkbox')
            series_title_p = item.find('p')

            if series_id_input and series_title_p:
                series_id = series_id_input.get('value', 'N/A')
                series_title = series_title_p.get_text(strip=True)
                rank = start_index + i + 1  # Calculate the rank
                
                page_data.append({
                    'Rank': rank,
                    'Series ID': series_id,
                    'Series Title': series_title
                })
        return page_data

    except requests.exceptions.RequestException as e:
        print(f"\nAn error occurred on page starting at index {start_index}: {e}")
        return []

def scrape_bls_series():
    """
    Scrapes up to 1,000,000 series from the BLS website using parallel requests
    and saves the data, including a rank, to a CSV file.
    """
    all_series_data = []
    
    # Create a list of starting indices for each page to be scraped
    start_indices = list(range(0, 1_000_000, 100))

    print(f"Starting scrape with {MAX_WORKERS} parallel workers...")

    # Use ThreadPoolExecutor to manage parallel requests
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # The `map` function applies `fetch_page_data` to each start_index.
        # It returns results in the order the tasks were submitted.
        results_iterator = executor.map(fetch_page_data, start_indices)
        
        # Wrap the iterator with tqdm for a progress bar
        progress_bar = tqdm(results_iterator, total=len(start_indices), desc="Scraping Pages")

        for page_result in progress_bar:
            if not page_result:
                # If a page comes back empty, it's likely the end of the data.
                # We can stop submitting new tasks (though `map` submits them all at once).
                # This check ensures we don't process empty lists unnecessarily.
                pass
            all_series_data.extend(page_result)

    # Sort the data by rank to ensure it's in the correct order,
    # just in case any network issues caused pages to be missed.
    all_series_data.sort(key=lambda x: x['Rank'])

    # Save the scraped data to a CSV file
    if all_series_data:
        output_filename = 'bls_series.csv'
        print(f"\nScraping complete. Writing {len(all_series_data)} series to {output_filename}...")
        
        with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Rank', 'Series ID', 'Series Title']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(all_series_data)
            
        print("Successfully saved data to CSV.")
    else:
        print("\nNo data was scraped. The output file was not created.")

if __name__ == '__main__':
    scrape_bls_series()