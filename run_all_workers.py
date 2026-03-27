import subprocess
import sys
import time
import os
from datetime import datetime
from timer_utils import GlobalTimer

# Initialize Global Timer
timer = GlobalTimer(work_duration=4*3600, rest_duration=1*3600)


def main():
    num_workers = 100
    batch_size = 5
    processes = []

    try:
        year_input = input("Enter the target year (e.g., 2024): ").strip()
        target_year = int(year_input)
    except ValueError:
        print("Invalid year format. Defaulting to 2025.")
        target_year = 2025

    print(f"Starting {num_workers} parallel workers in batches of {batch_size}...")

    # Ensure logs directory exists so workers don't race to create it
    os.makedirs("logs", exist_ok=True)

    try:
        for batch_start in range(1, num_workers + 1, batch_size):
            batch_end = min(batch_start + batch_size - 1, num_workers)
            print(
                f"\n[{datetime.now().strftime('%H:%M:%S')}] ==> STARTING BATCH {batch_start} to {batch_end} <==\n"
            )

            # Start parallel nodes for this batch
            batch_processes = []
            for i in range(batch_start, batch_end + 1):
                python_exe = sys.executable or "C:\\Python313\\python.exe"
                cmd = [
                    python_exe,
                    "worker_main.py",
                    "--worker-id",
                    str(i),
                    "--year",
                    str(target_year),
                ]

                p = subprocess.Popen(cmd)
                batch_processes.append((i, p))
                processes.append((i, p))  # Keep a global list for graceful shutdown
                print(f"Launched Worker {i} (PID: {p.pid})")
                time.sleep(2)  # Stagger to ease initial API handshake load

            print(
                "\nBatch launched. Waiting for these workers to finish before starting the next batch...\n"
            )

            # Wait for entirely batch to close before looping
            while True:
                active_in_batch = 0
                for i, p in batch_processes:
                    if p.poll() is None:
                        active_in_batch += 1

                if active_in_batch == 0:
                    print(f"Batch ({batch_start}-{batch_end}) completed successfully.")
                    break

                # Update the global timer
                timer.tick(5)
                timer_status = timer.get_short_status()

                print(
                    f"[{timer_status}] Active in batch: {active_in_batch}/{len(batch_processes)}",
                    end="\r",
                    flush=True
                )
                time.sleep(5)

        print(
            "\nAll 100 workers across all 10 batches have successfully finished executing."
        )

        print("\nGenerating final failed cities report...")
        generate_failed_report(target_year)

    except KeyboardInterrupt:
        print("\nStopping all workers...")
        for i, p in processes:
            try:
                p.terminate()
            except Exception:
                pass
        print("Terminated.")


def generate_failed_report(target_year):
    """Scan all expected cities to check which are missing JSON months"""
    import json
    import glob
    import unicodedata
    import re

    def sanitize_filename(text):
        text = (
            unicodedata.normalize("NFKD", text)
            .encode("ascii", "ignore")
            .decode("ascii")
        )
        text = re.sub(r"[^\w\s-]", "", text)
        return text

    data_dir = f"era5_data_{target_year}"
    failed_cities = []

    # Check what should exist by reading what was distributed
    config_files = glob.glob(os.path.join("config", "worker_*_cities.json"))

    for conf_file in config_files:
        try:
            with open(conf_file, "r", encoding="utf-8") as f:
                cities = json.load(f)

            for city_data in cities:
                city = sanitize_filename(city_data["city"])
                country = sanitize_filename(city_data["country"])

                missing_months = []
                for month in range(1, 13):
                    expected_json = os.path.join(
                        data_dir, f"{city}_{country}_{target_year}_{month:02d}.json"
                    )
                    if not os.path.exists(expected_json):
                        missing_months.append(month)

                if missing_months:
                    failed_cities.append(
                        f"{city}, {country} - Missing months: {missing_months}"
                    )
        except Exception as e:
            print(f"Error checking {conf_file}: {e}")

    report_path = "failed_cities.txt"
    if failed_cities:
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(
                f"The following cities failed to complete all {target_year} downloads:\n"
            )
            f.write("-" * 60 + "\n")
            for line in failed_cities:
                f.write(line + "\n")
        print(f"Warning: Data incomplete. See '{report_path}' for missing cities.")
    else:
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(
                f"All target cities successfully downloaded all 12 months for {target_year}!\n"
            )
        print(f"Success! All cities downloaded. Created '{report_path}'.")


if __name__ == "__main__":
    main()
