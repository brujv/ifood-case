import os
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BRONZE_PATH = "/Volumes/workspace/default/nyc_taxi_bronze"
TLC_PAGE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
MONTHS = ["2023-01", "2023-02", "2023-03", "2023-04", "2023-05"]

def get_available_files():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    response = requests.get(TLC_PAGE_URL, headers=headers)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    parquet_links = []
    for link in soup.find_all('a', href=True):
        href = link['href'].strip()
        if 'yellow_tripdata' in href and href.endswith('.parquet'):
            full_url = urljoin(TLC_PAGE_URL, href)
            parquet_links.append(full_url)
    
    return parquet_links

def filter_files_by_months(files, target_months):
    filtered = []
    for file_url in files:
        for month in target_months:
            if f"yellow_tripdata_{month}" in file_url:
                filtered.append((month, file_url))
                break
    return filtered

def download_file(month, url):
    filename = f"yellow_tripdata_{month}.parquet"
    local_path = os.path.join(BRONZE_PATH, filename)
    
    print(f"Downloading {month} from {url}")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    os.makedirs(BRONZE_PATH, exist_ok=True)
    
    with open(local_path, \"wb\") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    print(f"Saved to {local_path}")
    return local_path

if __name__ == "__main__":
    print("Fetching available files from TLC website")
    available_files = get_available_files()
    print(f"Found {len(available_files)} yellow taxi parquet files")

    print(f"\nFiltering for target months: {MONTHS}")
    target_files = filter_files_by_months(available_files, MONTHS)
    print(f"Found {len(target_files)} matching files\n")

    for month, url in target_files:
        download_file(month, url)

    print(f"\n✓ Download complete. Files saved to {BRONZE_PATH}")