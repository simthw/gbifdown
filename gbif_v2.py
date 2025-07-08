#!/usr/bin/env python
# Optimized GBIF occurrence downloader with parallel processing

import os
import pandas as pd
import concurrent.futures
import time
import json
from pygbif import occurrences
import requests

# Create the occurrences directory if it doesn't exist
os.makedirs('occurrences', exist_ok=True)

def download_species(species_name, index=None, total=None):
    """Download occurrences for a single species with rate limiting"""
    species_filename = species_name.replace(' ', '_') + '.json'
    species_path = os.path.join('occurrences', species_filename)
    
    # Skip if already downloaded
    if os.path.exists(species_path):
        if index is not None and total is not None:
            print(f"Skipping species {index}/{total}: {species_name} (already downloaded)")
        return 0
    
    if index is not None and total is not None:
        print(f"Processing species {index}/{total}: {species_name}")
        print(f"Downloading occurrences for {species_name}...")
    
    try:
        # Set a reasonable limit and add offset parameter for pagination
        limit = 300  # Reasonable number per request
        offset = 0
        all_results = []
        
        # Use pagination to get all results
        while True:
            try:
                results = occurrences.search(
                    scientificName=species_name,
                    limit=limit,
                    offset=offset
                )
                
                if 'results' not in results or not results['results']:
                    break
                
                all_results.extend(results['results'])
                
                # Check if we've reached the end
                if len(results['results']) < limit:
                    break
                
                offset += limit
                time.sleep(0.5)  # Add delay between requests to avoid rate limiting
                
            except requests.exceptions.RequestException as e:
                print(f"Request error for {species_name}: {e}. Retrying in 5 seconds...")
                time.sleep(5)  # Wait longer if there's an error
                continue
        
        # Save the results to a file
        with open(species_path, 'w', encoding='utf-8') as f:
            json.dump(all_results, f)
        
        return len(all_results)
    except Exception as e:
        print(f"Error downloading {species_name}: {e}")
        return 0

def main():
    # Load species list
    try:
        df = pd.read_csv('species_list_483.csv')
        species_list = df['scientific_name'].tolist()
        print(f"Loaded {len(species_list)} species names from species_list_483.csv")
    except Exception as e:
        print(f"Error loading species list: {e}")
        return
    
    # Use ThreadPoolExecutor for parallel downloads
    max_workers = 5  # Adjust based on your internet connection
    total_downloaded = 0
    
    print(f"Starting parallel download with {max_workers} workers...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create tasks for each species with progress information
        futures = [
            executor.submit(download_species, species, i+1, len(species_list))
            for i, species in enumerate(species_list)
        ]
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(futures):
            count = future.result()
            total_downloaded += count
    
    print(f"Download complete! Downloaded data for {total_downloaded} occurrences.")

if __name__ == "__main__":
    main()