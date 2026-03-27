import sqlite3
import json
import os
import glob
import time

DB_FILE = "tasks.db"
CONFIG_DIR = "config"


def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # Create tables
    c.execute("""
        CREATE TABLE IF NOT EXISTS cities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT NOT NULL,
            country TEXT NOT NULL,
            lat REAL,
            lon REAL,
            UNIQUE(city, country)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id INTEGER,
            year INTEGER,
            month INTEGER,
            status TEXT DEFAULT 'pending', -- pending, processing, completed, error
            worker_id INTEGER,
            last_attempt TIMESTAMP,
            error_message TEXT,
            FOREIGN KEY(city_id) REFERENCES cities(id),
            UNIQUE(city_id, year, month)
        )
    """)

    # Create system_state table for global coordination
    c.execute("""
        CREATE TABLE IF NOT EXISTS system_state (
            id INTEGER PRIMARY KEY DEFAULT 1,
            phase TEXT DEFAULT 'WORKING', -- WORKING, RESTING
            last_transition_time REAL,
            work_accumulated REAL DEFAULT 0,
            CHECK (id = 1)
        )
    """)

    # Initialize the single state row if it doesn't exist
    c.execute("INSERT OR IGNORE INTO system_state (id, phase, last_transition_time, work_accumulated) VALUES (1, 'WORKING', ?, 0)", (time.time(),))

    conn.commit()
    conn.close()
    print(f"Initialized database: {DB_FILE}")


def load_cities_from_configs(target_year):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # Load all worker_*.json files
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
                try:
                    c.execute(
                        "INSERT OR IGNORE INTO cities (city, country, lat, lon) VALUES (?, ?, ?, ?)",
                        (city, country, lat, lon),
                    )

                    # Get city ID
                    c.execute(
                        "SELECT id FROM cities WHERE city = ? AND country = ?",
                        (city, country),
                    )
                    city_id = c.fetchone()[0]

                    # Create tasks for target year (12 months)
                    for month in range(1, 13):
                        c.execute(
                            """
                            INSERT OR IGNORE INTO tasks (city_id, year, month, status)
                            VALUES (?, ?, ?, 'pending')
                        """,
                            (city_id, target_year, month),
                        )
                        total_tasks += 1

                    total_cities += 1
                except Exception as e:
                    print(f"Error inserting {city}: {e}")

        except Exception as e:
            print(f"Failed to read {config_file}: {e}")

    conn.commit()
    conn.close()
    print(f"Migration complete! Added {total_cities} cities and {total_tasks} tasks.")


if __name__ == "__main__":
    try:
        year_input = input("Enter the target year (e.g., 2024): ").strip()
        target_year = int(year_input)
    except ValueError:
        print("Invalid year format. Defaulting to 2025.")
        target_year = 2025

    init_db()
    load_cities_from_configs(target_year)
