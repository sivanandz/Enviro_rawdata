import cdsapi
import os
import time
import random
import json
import xarray as xr
import argparse
import logging
from datetime import datetime

# ==========================================
# SETUP LOGGING
# ==========================================
def setup_logging(worker_id):
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"worker_{worker_id}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format=f'[Worker {worker_id}] %(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(f"worker_{worker_id}")

# ==========================================
# CONFIGURATION
# ==========================================
DATA_DIR = "era5_data_2025"
YEARS = [2025]
MONTHS = list(range(1, 13))

def load_cities(worker_id):
    config_path = os.path.join("config", f"worker_{worker_id}_cities.json")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_city_progress(city, country, data_dir):
    """Check how many files exist for a city and return missing files"""
    existing_files = 0
    missing_files = []

    for year in YEARS:
        for month in MONTHS:
            # Check for JSON first (final output), then NC (intermediate)
            # The logic in previous script checked for .nc but we want to know if job is DONE.
            # Final output is .json.
            filename = f"{city}_{country}_{year}_{month:02d}.json"
            filepath = os.path.join(data_dir, filename)

            if os.path.exists(filepath):
                existing_files += 1
            else:
                missing_files.append((year, month))

    return existing_files, missing_files

def is_city_complete(city, country, data_dir):
    """Check if all files exist for a city"""
    existing, missing = get_city_progress(city, country, data_dir)
    total_expected = len(YEARS) * len(MONTHS)
    return existing == total_expected

def download_era5_data(worker_id, logger, city, country, lat, lon, year, month):
    """Download ERA5 data with Exponential Backoff"""
    c = cdsapi.Client(quiet=True) # Quiet the default CDSAPI logger
    
    nc_filename = f"{city}_{country}_{year}_{month:02d}.nc"
    json_filename = f"{city}_{country}_{year}_{month:02d}.json"
    nc_filepath = os.path.join(DATA_DIR, nc_filename)
    json_filepath = os.path.join(DATA_DIR, json_filename)

    if os.path.exists(json_filepath):
        logger.info(f"JSON already exists: {json_filename}")
        return True

    # --- EXPONENTIAL BACKOFF LOGIC ---
    max_retries = 10 
    base_delay = 5  # Start with 5 seconds
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempt {attempt+1}: Downloading {nc_filename}...")
            c.retrieve(
                'reanalysis-era5-single-levels',
                {
                    'product_type': 'reanalysis',
                    'variable': [
                        '2m_temperature', 'total_precipitation', 'total_cloud_cover',
                        '10m_u_component_of_wind', '10m_v_component_of_wind'
                    ],
                    'year': str(year),
                    'month': f'{month:02d}',
                    'day': [f'{d:02d}' for d in range(1, 32)],
                    'time': [f'{h:02d}:00' for h in range(24)],
                    'area': [lat, lon, lat, lon],
                    'format': 'netcdf',
                },
                nc_filepath
            )
            
            # --- CONVERT TO JSON ---
            logger.info(f"Converting {nc_filename} to JSON...")
            try:
                # Debugging: Check file size
                file_size = os.path.getsize(nc_filepath)
                logger.info(f"File size: {file_size} bytes")
                
                if file_size < 1000:
                    # It's suspiciously small, might be a text error
                    with open(nc_filepath, 'rb') as f:
                        header = f.read(100)
                    logger.warning(f"File header (first 100 bytes): {header}")

                ds = xr.open_dataset(nc_filepath, engine="netcdf4") # Force netcdf4 engine
                df = ds.to_dataframe()
                df.to_json(json_filepath, orient="table") 
                ds.close() 
                os.remove(nc_filepath) 
                return True
            except Exception as convers_e:
                logger.error(f"Conversion failed for {nc_filename}: {convers_e}")
                
                # Double check imports
                try:
                    import netCDF4
                    logger.info("netCDF4 module is importable.")
                except ImportError:
                    logger.warning("netCDF4 module is NOT importable. Please run: pip install netCDF4")

                try:
                    import scipy
                    logger.info("scipy module is importable.")
                except ImportError:
                    logger.warning("scipy module is NOT importable. Please run: pip install scipy")
                    
                # Inspect file on failure if we haven't already
                if os.path.exists(nc_filepath):
                    file_size = os.path.getsize(nc_filepath)
                    logger.info(f"Failed file size: {file_size} bytes")
                    with open(nc_filepath, 'rb') as f:
                        header = f.read(200)
                    logger.info(f"Failed file header: {header}")
                    
                    # We DO NOT remove the file here so we can inspect it if needed, 
                    # OR we remove it to allow retry.
                    # Current logic: remove to allow retry.
                    os.remove(nc_filepath) 
                
                # If it was an OSError or similar, maybe we should not retry immediately?
                # But for now, we continue to outer loop to retry or fail.
                pass 

        except Exception as e:
            error_str = str(e).lower()
            if "rejected" in error_str or "queued requests" in error_str or "limited" in error_str or "400" in error_str:
                # Specific handling for API rate limits / queue full
                delay = 60 + random.uniform(0, 30)
                logger.warning(f"API Limit/Rejection detected: {e}. Sleeping long ({delay:.1f}s)...")
                time.sleep(delay)
                continue # Retry loop

            delay = (base_delay * 2 ** attempt) + random.uniform(0, 5)
            # Cap delay at some reasonable max (e.g. 5 mins)
            delay = min(delay, 300) 
            logger.warning(f"Error: {e}. Retrying in {delay:.1f}s...")
            time.sleep(delay)

    logger.error(f"Failed to download {nc_filename} after {max_retries} attempts.")
    return False

def process_city(worker_id, logger, city_data):
    """Process all months for a city"""
    city = city_data["city"]
    country = city_data["country"]
    lat = city_data["lat"]
    lon = city_data["lon"]
    
    if is_city_complete(city, country, DATA_DIR):
        logger.info(f"City {city}, {country} already complete. Skipping.")
        return True
    
    existing, missing = get_city_progress(city, country, DATA_DIR)
    logger.info(f"Starting city: {city}, {country} | Found {existing} files, Need {len(missing)}")

    success_count = existing
    
    for year, month in missing:
        if download_era5_data(worker_id, logger, city, country, lat, lon, year, month):
            success_count += 1
        # Slight delay to be nice to the API locally, though parallel workers will hit it hard
        time.sleep(1) 

    total_expected = len(YEARS) * len(MONTHS)
    if success_count == total_expected:
        logger.info(f"Completed city: {city}")
        return True
    else:
        logger.warning(f"Partially completed {city}: {success_count}/{total_expected}")
        return False

def main():
    parser = argparse.ArgumentParser(description="ERA5 Data Downloader Worker")
    parser.add_argument("--worker-id", type=int, required=True, help="Worker ID (1-10)")
    args = parser.parse_args()
    
    worker_id = args.worker_id
    logger = setup_logging(worker_id)
    
    logger.info(f"=== Worker {worker_id} Started ===")
    
    try:
        cities = load_cities(worker_id)
        logger.info(f"Loaded {len(cities)} cities from config.")
    except Exception as e:
        logger.critical(f"Failed to load config: {e}")
        return

    # Ensure data directory
    os.makedirs(DATA_DIR, exist_ok=True)

    completed_cities = 0
    for city_data in cities:
        try:
            if process_city(worker_id, logger, city_data):
                completed_cities += 1
        except Exception as e:
            logger.error(f"Critical error processing city {city_data.get('city')}: {e}")

    logger.info(f"=== Worker {worker_id} Finished ===")
    logger.info(f"Successfully completed: {completed_cities}/{len(cities)} cities")

if __name__ == "__main__":
    main()
