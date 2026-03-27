import json
import os
import math
import glob

# Paths to the existing worker notebooks
NOTEBOOK_PATHS = [
    "Worker1_exponen",  # This one was a python script/text file we looked at earlier, but let's check if there is a .ipynb version or just use this one if it's the source of truth for worker 1.
    "Worker_ID_1.ipynb",
    "Worker_ID_2.ipynb",
    "Worker_ID_3.ipynb",
    "Worker_ID_4.ipynb",
]

CONFIG_DIR = "config"


def extract_cities_from_notebook(file_path):
    """
    Extracts the CITIES list from a Jupyter Notebook or Python file.
    Returns a list of city dictionaries.
    """
    cities = []

    if not os.path.exists(file_path):
        print(f"Warning: File {file_path} not found. Skipping.")
        return []

    try:
        # Try processing as a JSON notebook first
        with open(file_path, "r", encoding="utf-8") as f:
            content = json.load(f)

        print(f"Processing {file_path} as Notebook...")

        # Iterate through cells to find the one defining CITIES = [...]
        for cell in content.get("cells", []):
            if cell.get("cell_type") == "code":
                source = "".join(cell.get("source", []))
                if "CITIES =" in source or "CITIES=" in source:
                    # We found the cell. Now we need to safeguard the execution.
                    # We will execute this block in a restricted scope to extract the variable.
                    local_scope = {}
                    try:
                        exec(source, {}, local_scope)
                        if "CITIES" in local_scope:
                            cities.extend(local_scope["CITIES"])
                            print(f"  -> Found {len(local_scope['CITIES'])} cities.")
                    except Exception as e:
                        print(f"  -> Error executing code block in {file_path}: {e}")

    except (json.JSONDecodeError, UnicodeDecodeError):
        # Fallback for Python files or text files (like Worker1_exponen)
        print(f"Processing {file_path} as Text/Python script...")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                source = f.read()

            local_scope = {}
            try:
                # We interpret the whole file, but might need to mock imports if they fail
                # Assuming the file structure is simple enough or we just grab the list text
                # A safer approach for a huge script is to find the CITIES [...] block.
                # But let's try exec first if it's a simple script.
                exec(source, {}, local_scope)
                if "CITIES" in local_scope:
                    cities.extend(local_scope["CITIES"])
                    print(f"  -> Found {len(local_scope['CITIES'])} cities.")
            except Exception as e:
                # If exec fails (e.g. missing imports), we might need to be more clever.
                # Let's try to just extract the text block starting with CITIES = [ ... ]
                # This is a bit hacky but works if the format is consistent.
                import ast

                lines = source.split("\n")
                cities_block = []
                in_cities = False
                for line in lines:
                    if "CITIES = [" in line:
                        in_cities = True
                        cities_block.append("[")
                        continue
                    if in_cities:
                        cities_block.append(line)
                        if line.strip().startswith("]"):
                            in_cities = False
                            break

                if cities_block:
                    cities_str = "".join(cities_block)
                    try:
                        cities_list = ast.literal_eval(cities_str)
                        cities.extend(cities_list)
                        print(f"  -> Parsed {len(cities_list)} cities using AST.")
                    except Exception as ast_e:
                        print(f"  -> AST parsing failed: {ast_e}")

        except Exception as e:
            print(f"  -> Error reading {file_path}: {e}")

    return cities


def main():
    all_cities = []

    # 1. Gather all cities
    # We prefer the .ipynb files if they exist, but check the directory listing from earlier
    # directory listing showed: Worker1_exponen, Worker_ID_1.ipynb, Worker_ID_2.ipynb, Worker_ID_3.ipynb, Worker_ID_4.ipynb

    files_to_process = [
        "Worker _ID_1.ipynb",
        "Worker_ID_2.ipynb",
        "Worker_ID_3.ipynb",
        "Worker_ID_4.ipynb",
    ]

    # If Worker_ID_1.ipynb is small/empty compared to Worker1_exponen, we might want to use Worker1_exponen
    # logic: Try to load from all unique files you see

    for fname in files_to_process:
        cities = extract_cities_from_notebook(fname)
        all_cities.extend(cities)

    # Remove duplicates based on city name + country
    unique_cities = {}
    for city in all_cities:
        key = (city["city"], city["country"])
        if key not in unique_cities:
            unique_cities[key] = city

    final_city_list = list(unique_cities.values())
    print(f"\nTotal unique cities found: {len(final_city_list)}")

    # 2. Split into 100 chunks for the new orchestrated batch runner.
    num_workers = 100

    os.makedirs(CONFIG_DIR, exist_ok=True)

    # Calculate base size and remainder for perfectly even distribution
    base_size = len(final_city_list) // num_workers
    remainder = len(final_city_list) % num_workers

    start_idx = 0
    # Generate a json file for each worker
    for i in range(num_workers):
        # The first 'remainder' workers get base_size + 1 cities to absorb the remainder evenly
        current_chunk_size = base_size + (1 if i < remainder else 0)
        end_idx = start_idx + current_chunk_size
        worker_cities = final_city_list[start_idx:end_idx]
        start_idx = end_idx

        config_path = os.path.join(CONFIG_DIR, f"worker_{i + 1}_cities.json")
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(worker_cities, f, indent=4)

        print(f"Created {config_path} with {len(worker_cities)} cities.")


if __name__ == "__main__":
    main()
