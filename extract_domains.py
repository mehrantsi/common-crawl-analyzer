#!/usr/bin/env python3
"""
Extract all unique domains from Common Crawl index files.
"""

import os
import sys
import gzip
import urllib.parse
import csv
import pyarrow.parquet as pq
import pandas as pd
import requests
from tqdm import tqdm
import concurrent.futures
from urllib.robotparser import RobotFileParser
import argparse
import threading

# Create a lock for file writing to prevent race conditions
file_lock = threading.Lock()

def download_file(url, local_path):
    """Download a file from a URL to a local path if it doesn't exist."""
    if os.path.exists(local_path):
        print(f"File already exists: {local_path}")
        return local_path
    
    print(f"Downloading {url} to {local_path}")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total_size = int(r.headers.get('content-length', 0))
        with open(local_path, 'wb') as f:
            with tqdm(total=total_size, unit='B', unit_scale=True, desc=local_path) as pbar:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
    return local_path

def extract_domain(url):
    """Extract domain from URL."""
    try:
        parsed = urllib.parse.urlparse(url)
        domain = parsed.netloc.lower()
        # Remove port if present
        domain = domain.split(':')[0]
        return domain if domain else None
    except Exception:
        return None

def process_parquet_file(parquet_file, output_file):
    """Process a parquet file and extract domains, writing directly to output file."""
    print(f"Processing {parquet_file}")
    
    try:
        # Read the parquet file
        table = pq.read_table(parquet_file)
        df = table.to_pandas()
        
        # If the dataframe has a 'url' column, use it
        if 'url' in df.columns:
            url_column = 'url'
        # Otherwise, try to find a column that might contain URLs
        else:
            possible_url_columns = ['url', 'URL', 'uri', 'URI', 'Url', 'Uri']
            for col in possible_url_columns:
                if col in df.columns:
                    url_column = col
                    break
            else:
                # If no URL column found, print columns and exit
                print(f"No URL column found in {parquet_file}. Columns: {df.columns}")
                return 0
        
        # Extract domains and write them to file while processing
        domains = set()
        with file_lock:
            with open(output_file, 'a') as f:
                for url in df[url_column].dropna():
                    domain = extract_domain(url)
                    if domain and domain not in domains:
                        domains.add(domain)
                        f.write(f"{domain}\n")
        
        # Clear dataframe from memory
        del df
        del table
        
        return len(domains)
    except Exception as e:
        print(f"Error processing {parquet_file}: {e}")
        return 0

def process_file(path, base_url, output_file, limit_rows=None):
    """Process a single file from the index."""
    # Construct the URL and local path
    url = base_url + path
    local_path = os.path.join("data", path)
    
    try:
        # Download the file
        local_file = download_file(url, local_path)
        
        # Process the file if it's a parquet
        domains_count = 0
        if local_file.endswith('.parquet'):
            domains_count = process_parquet_file(local_file, output_file)
        else:
            print(f"Skipping non-parquet file: {local_file}")
        
        # Delete the file after processing
        if os.path.exists(local_file):
            os.remove(local_file)
            print(f"Deleted {local_file}")
        
        return domains_count
    except Exception as e:
        print(f"Error processing {url}: {e}")
        # Ensure the file is deleted even if processing fails
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
                print(f"Deleted {local_path}")
            except Exception as del_error:
                print(f"Error deleting {local_path}: {del_error}")
        return 0

def process_index_paths(index_paths_url, output_file, limit_files=None, limit_rows=None, base_url="https://data.commoncrawl.org/", max_workers=4):
    """Process the index paths file to get URLs of parquet files."""
    # Download the index paths file
    local_index_path = os.path.join("data", os.path.basename(index_paths_url))
    download_file(index_paths_url, local_index_path)
    
    # Read the paths and store in a list
    with gzip.open(local_index_path, 'rt') as f:
        paths = [line.strip() for line in f]
    
    print(f"Found {len(paths)} paths in {local_index_path}")
    
    # Delete the index paths file after reading
    os.remove(local_index_path)
    print(f"Deleted {local_index_path}")
    
    # Limit the number of files if specified
    if limit_files and limit_files > 0:
        paths = paths[:limit_files]
    
    # Initialize output file
    with open(output_file, 'w') as f:
        pass  # Create empty file
    
    # Process files in parallel
    total_domains = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {executor.submit(process_file, path, base_url, output_file, limit_rows): path for path in paths}
        
        for future in tqdm(concurrent.futures.as_completed(future_to_file), total=len(paths), desc="Processing files"):
            file_path = future_to_file[future]
            try:
                domains_count = future.result()
                total_domains += domains_count
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
    
    print(f"Total domains extracted: {total_domains}")
    # Note: No longer deduplicating to save memory with billions of entries
    return total_domains

def main():
    parser = argparse.ArgumentParser(description='Extract domains from Common Crawl index files.')
    parser.add_argument('--index-url', type=str, 
                        default='https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-13/cc-index-table.paths.gz',
                        help='URL to the index paths file')
    parser.add_argument('--output', type=str, default='domains.txt',
                        help='Output file for domains')
    parser.add_argument('--limit-files', type=int, default=None,
                        help='Limit the number of files to process (for testing)')
    parser.add_argument('--limit-rows', type=int, default=None,
                        help='Limit the number of rows per file (for testing)')
    parser.add_argument('--base-url', type=str, default='https://data.commoncrawl.org/',
                        help='Base URL for Common Crawl data')
    parser.add_argument('--workers', type=int, default=4,
                        help='Number of worker threads for parallel processing')
    
    args = parser.parse_args()
    
    process_index_paths(
        args.index_url,
        args.output,
        args.limit_files,
        args.limit_rows,
        args.base_url,
        args.workers
    )

if __name__ == "__main__":
    main() 