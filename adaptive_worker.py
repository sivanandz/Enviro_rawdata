import sqlite3
import cdsapi
import os
import time
import random
import xarray as xr
import numpy as np
import sys
import logging
import argparse
import unicodedata
import re
import zipfile
import shutil
from timer_utils import GlobalTimer
from db_config import DB_FILE

# ==========================================
# SETUP
# ==========================================
timer = GlobalTimer(work_duration=4*3600, rest_duration=1*3600)


def setup_logging(worker_id):
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"adaptive_worker_{worker_id}.log")

    logging.basicConfig(
        level=logging.INFO,
        format=f"[Worker {worker_id}] %(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )
    return logging.getLogger(f"worker_{worker_id}")


def get_next_task(worker_id):
    """Fetch the next pending task from the DB atomically"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # Simple transaction to lock and claim a row
    try:
        c.execute("BEGIN IMMEDIATE")

        # Priority: Retry pending tasks that haven't been touched in > 2 hours?
        # For simplicity, just get one marked 'pending'
        c.execute("""
            SELECT t.id, c.city, c.country, c.lat, c.lon, t.year, t.month
            FROM tasks t
            JOIN cities c ON t.city_id = c.id
            WHERE t.status = 'pending'
            LIMIT 1
        """)
        row = c.fetchone()

        if row:
            task_id = row[0]
            c.execute(
                """
                UPDATE tasks 
                SET status = 'processing', worker_id = ?, last_attempt = CURRENT_TIMESTAMP
                WHERE id = ?
            """,
                (worker_id, task_id),
            )
            conn.commit()
            conn.close()
            return row
        else:
            conn.commit()  # Nothing found
            conn.close()
            return None

    except sqlite3.OperationalError:
        # DB locked, wait and buffer
        conn.close()
        time.sleep(random.uniform(0.1, 1.0))
        return None


def update_task_status(task_id, status, error_msg=None):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        """
        UPDATE tasks 
        SET status = ?, error_message = ?
        WHERE id = ?
    """,
        (status, error_msg, task_id),
    )
    conn.commit()
    conn.close()


def sanitize_filename(text):
    """
    Normalize text to ASCII, removing accents and special characters.
    e.g. "Briançon" -> "Briancon"
    """
    # Normalize unicode characters to their base form (e.g. ç -> c + cedilla)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    # Remove any remaining non-alphanumeric chars (except space/underscore/hyphen)
    text = re.sub(r"[^\w\s-]", "", text)
    return text


def download_era5_data(logger, city, country, lat, lon, year, month):
    """Download ERA5 data - Adapted for single task execution"""
    c = cdsapi.Client(quiet=True)

    clean_city = sanitize_filename(city)
    clean_country = sanitize_filename(country)

    nc_filename = f"{clean_city}_{clean_country}_{year}_{month:02d}.nc"
    json_filename = f"{clean_city}_{clean_country}_{year}_{month:02d}.json"

    data_dir = f"era5_data_{year}"
    os.makedirs(data_dir, exist_ok=True)

    nc_filepath = os.path.join(data_dir, nc_filename)
    json_filepath = os.path.join(data_dir, json_filename)

    if os.path.exists(json_filepath):
        # SMART CHECK: Check if the existing file has the new data
        try:
            with open(json_filepath, "r") as f:
                # Read enough to cover the schema/columns part of the table JSON
                start_content = f.read(5000)

            if "peak_wave_period" in start_content:
                logger.info(
                    f"JSON already exists and contains new vars: {json_filename}"
                )
                return "exists"
            else:
                logger.info(
                    f"JSON exists but missing new vars. Redownloading: {json_filename}"
                )
        except Exception as e:
            logger.warning(
                f"Error reading existing file {json_filename}: {e}. Overwriting."
            )

    # Minimal retry inside the worker logic, but we rely on the Manager for major backoff
    try:
        logger.info(f"Downloading {nc_filename}...")
        c.retrieve(
            "reanalysis-era5-single-levels",
            {
                "product_type": "reanalysis",
                "variable": [
                    "2m_temperature",
                    "skin_temperature",
                    "2m_dewpoint_temperature",
                    "total_precipitation",
                    "snowfall",
                    "snow_depth",
                    "snow_density",
                    "downward_uv_radiation_at_the_surface",
                    "total_cloud_cover",
                    "surface_solar_radiation_downwards",
                    "10m_u_component_of_wind",
                    "10m_v_component_of_wind",
                    "significant_height_of_combined_wind_waves_and_swell",
                    "mean_wave_period",
                    "peak_wave_period",
                ],
                "year": str(year),
                "month": f"{month:02d}",
                "day": [f"{d:02d}" for d in range(1, 32)],
                "time": [f"{h:02d}:00" for h in range(24)],
                "area": [
                    lat + 0.25,
                    lon - 0.25,
                    lat - 0.25,
                    lon + 0.25,
                ],  # North, West, South, East (Bounding box to ensure grid point inclusion)
                "data_format": "netcdf",
            },
            nc_filepath,
        )

        logger.info(f"Download Complete: {nc_filename}")

        # CHECK FOR ZIP FILE (The API sometimes sends a ZIP instead of NC)
        if zipfile.is_zipfile(nc_filepath):
            logger.info(f"Detected ZIP file. Extracting...")
            try:
                with zipfile.ZipFile(nc_filepath, "r") as zip_ref:
                    # Get list of files
                    files = zip_ref.namelist()
                    # Look for a .nc file
                    nc_files = [f for f in files if f.endswith(".nc")]

                    if not nc_files:
                        raise Exception("ZIP downloaded but contained no .nc files!")

                    # Extract the first one
                    target_file = nc_files[0]
                    zip_ref.extract(target_file, data_dir)

                # Remove the zip file
                os.remove(nc_filepath)

                # Rename the extracted file to our expected filename
                extracted_path = os.path.join(data_dir, target_file)
                # We need to handle if the extracted content is already there or conflict?
                # Move/Rename
                if os.path.exists(nc_filepath):
                    os.remove(nc_filepath)  # Should be gone already but safety first
                os.rename(extracted_path, nc_filepath)
                logger.info(f"Extracted {target_file} to {nc_filename}")

            except Exception as zip_e:
                logger.error(f"Failed to unzip: {zip_e}")
                # Don't return, let it fail below if file is bad

        logger.info(f"Converting {nc_filename} to JSON...")
        ds = xr.open_dataset(nc_filepath, engine="netcdf4")  # Force netcdf4

        # Calculate Relative Humidity
        # Constants for August-Roche-Magnus approximation (Alduchov and Eskridge 1996)
        a = 17.625
        b = 243.04

        # Temperatures in ERA5 are Kelvin, convert to Celsius
        t_c = ds["t2m"] - 273.15
        td_c = ds["d2m"] - 273.15

        # RH = 100 * exp((a*Td)/(b+Td)) / exp((a*T)/(b+T))
        # equivalent to 100 * exp( (a*Td)/(b+Td) - (a*T)/(b+T) )
        rh = 100 * np.exp((a * td_c) / (b + td_c) - (a * t_c) / (b + t_c))

        # Clip to valid range [0, 100]
        ds["relative_humidity"] = rh.clip(0, 100)

        df = ds.to_dataframe()
        df.to_json(json_filepath, orient="table")
        ds.close()
        os.remove(nc_filepath)
        return "success"

    except Exception as e:
        error_str = str(e).lower()
        if (
            "rejected" in error_str
            or "queued requests" in error_str
            or "limited" in error_str
            or "429" in error_str
        ):
            logger.warning(f"JOB REJECTED (Rate Limit): {e}")
            return "rejected"  # SIGNAL TO EXIT

        if "assertion failed" in error_str and "area" in error_str:
            logger.error(f"CRITICAL CONFIG ERROR (Area): {e}")
            return f"error: area_assertion_failed"

        if "400" in error_str:
            logger.error(f"BAD REQUEST (400): {e}. Selection might be invalid.")
            return f"error: bad_request_400"

        # Check for bad file format
        if "unknown file format" in error_str or "errno -51" in error_str:
            if os.path.exists(nc_filepath):
                size = os.path.getsize(nc_filepath)
                logger.error(f"Bad NetCDF File! Size: {size} bytes")
                with open(nc_filepath, "rb") as f:
                    header = f.read(100)
                logger.error(f"File Header: {header}")
                os.remove(nc_filepath)  # Delete it so we can retry!
                return f"error: corrupted file (deleted)"

        logger.error(f"Download Error: {e}")
        return f"error: {str(e)}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-id", type=int, required=True)
    args = parser.parse_args()

    worker_id = args.worker_id
    logger = setup_logging(worker_id)

    # Directory will be created dynamically when data is downloaded
    logger.info("Adaptive Worker Started. Polling DB...")

    # Worker loop
    while True:
        # Check if the system is in resting mode
        timer.check_wait(worker_id, logger)
        
        # Log work remaining occasionally
        status = timer.get_short_status()
        if "Work Rem" in status:
             logger.info(f"Ready for next task. {status}")
        
        task = get_next_task(worker_id)

        if not task:
            # No tasks pending? Wait a bit, or exit if completely done?
            # We'll verify if ANY pending tasks exist. For now, sleep.
            time.sleep(5)
            continue

        task_id, city, country, lat, lon, year, month = task
        logger.info(f"Picked task {task_id}: {city} ({year}-{month:02d})")

        result = download_era5_data(logger, city, country, lat, lon, year, month)

        if result == "success" or result == "exists":
            update_task_status(task_id, "completed")
            time.sleep(1)  # Be nice
        elif result == "rejected":
            # CRITICAL: Mark as pending so it can be retried later, then SUICIDE
            update_task_status(task_id, "pending", "Rate Limit Rejection")
            logger.warning("Exiting due to Rate Limit. Manager should handle backoff.")
            sys.exit(42)  # Special exit code for Manager
        else:
            # Regular error
            update_task_status(task_id, "error", result)


if __name__ == "__main__":
    main()
