import sqlite3
import json
import os
import glob

from db_config import DB_FILE, CONFIG_DIR


def sync_db(target_year):
    if not os.path.exists(DB_FILE):
        print(f"Error: {DB_FILE} not found. Run init_db.py first.")
        return

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # 1. Clear existing data
    print("Clearing existing tasks and cities...")
    c.execute("DELETE FROM tasks")
    c.execute("DELETE FROM cities")
    # Reset autoincrement
    c.execute("DELETE FROM sqlite_sequence WHERE name='tasks'")
    c.execute("DELETE FROM sqlite_sequence WHERE name='cities'")

    # 2. Load all worker_*.json files
    config_files = glob.glob(os.path.join(CONFIG_DIR, "worker_*_cities.json"))

    total_cities = 0
    total_tasks = 0

    for config_file in config_files:
        print(f"Processing {config_file}...")
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                cities = json.load(f)

            for city_data in cities:
                city = city_data["city"]
                country = city_data["country"]
                lat = city_data["lat"]
                lon = city_data["lon"]

                # Insert city
                c.execute(
                    "INSERT INTO cities (city, country, lat, lon) VALUES (?, ?, ?, ?)",
                    (city, country, lat, lon),
                )

                # Get city ID
                city_id = c.lastrowid

                # Create tasks for the target year (12 months)
                for month in range(1, 13):
                    c.execute(
                        """
                        INSERT INTO tasks (city_id, year, month, status)
                        VALUES (?, ?, ?, 'pending')
                    """,
                        (city_id, target_year, month),
                    )
                    total_tasks += 1

                total_cities += 1
        except Exception as e:
            print(f"Failed to process {config_file}: {e}")
            conn.rollback()
            conn.close()
            return

    conn.commit()
    conn.close()
    print(f"\nSynchronization complete!")
    print(f"Total cities added: {total_cities}")
    print(f"Total tasks created: {total_tasks}")


if __name__ == "__main__":
    try:
        year_input = input("Enter the target year (e.g., 2024): ").strip()
        target_year = int(year_input)
    except ValueError:
        print("Invalid year format. Defaulting to 2025.")
        target_year = 2025

    sync_db(target_year)
