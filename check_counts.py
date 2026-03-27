import json
import glob
import os
import sqlite3

from db_config import DB_FILE, CONFIG_DIR

def count_cities():
    config_files = glob.glob(os.path.join(CONFIG_DIR, 'worker_*.json'))
    json_city_names = set()
    total_json_cities = 0
    
    cities_per_file = {}
    
    for f in config_files:
        with open(f, 'r', encoding='utf-8') as file:
            data = json.load(file)
            cities_per_file[f] = len(data)
            total_json_cities += len(data)
            for city in data:
                json_city_names.add((city['city'], city['country']))
    
    db_city_names = set()
    total_db_cities = 0
    if os.path.exists(DB_FILE):
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute('SELECT city, country FROM cities')
        rows = c.fetchall()
        total_db_cities = len(rows)
        for row in rows:
            db_city_names.add((row[0], row[1]))
        conn.close()

    print(f"Cities in JSON files: {total_json_cities}")
    for f, count in cities_per_file.items():
        print(f"  - {f}: {count}")
    
    print(f"Total unique cities in JSON files: {len(json_city_names)}")
    print(f"Cities in database: {total_db_cities}")
    
    missing_in_db = json_city_names - db_city_names
    if missing_in_db:
        print(f"\nMissing in database ({len(missing_in_db)}):")
        for city, country in sorted(list(missing_in_db))[:10]:
            print(f"  - {city} ({country})")
        if len(missing_in_db) > 10:
            print(f"  ... and {len(missing_in_db) - 10} more")
    else:
        print("\nAll cities from JSON files are in the database.")

if __name__ == '__main__':
    count_cities()
