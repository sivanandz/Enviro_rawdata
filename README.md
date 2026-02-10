# EnviroReliability Data Downloader

This project contains Python scripts and Jupyter notebooks for downloading environmental data (ERA5) for various cities.

## Scripts

- `adaptive_manager.py`: Manages the download tasks.
- `adaptive_worker.py`: Worker script that performs the actual downloads.
- `generate_and_distribute_configs.py`: Generates configuration files for workers.
- `init_db.py`: Initializes the SQLite database for task management.
- `reset_tasks.py`: Resets task statuses in the database.
- `run_all_workers.py`: Utility to run multiple workers.

## Usage

1.  Install dependencies:
    ```bash
    pip install -r req.txt
    ```

2.  Initialize the database:
    ```bash
    python init_db.py
    ```

3.  Generate configurations (if needed):
    ```bash
    python generate_and_distribute_configs.py
    ```

4.  Run the manager or workers as needed.
