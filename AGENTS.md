# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Project Overview
ERA5 climate data download system with PySide6 GUI dashboard. Downloads ERA5 reanalysis data via CDS API, converts NetCDF→JSON, stores in SQLite.

## Build/Run Commands
- **GUI**: `python gui_app.py` (requires PySide6)
- **Init DB**: `python init_db.py` (interactive, asks for year)
- **Single worker**: `python worker_main.py --worker-id N`
- **Adaptive manager**: `python adaptive_manager.py`
- **Run all workers**: `python run_all_workers.py`
- **Tests**: No test framework; use `python test_global_timer.py` or `python test_gui.py`

## Architecture
- `tasks.db` (SQLite) is the coordination hub — workers, manager, and GUI all read/write it
- `system_state` table (single row, id=1) holds global WORKING/RESTING phase and `work_accumulated`
- `GlobalTimer` in `timer_utils.py` manages work/rest cycles (4h work / 1h rest default)
- Workers call `timer.check_wait()` which blocks during RESTING phase
- Only `adaptive_manager.py` calls `timer.tick()` to accumulate work time — GUI does NOT tick the timer
- GUI reads timer state via `gui_db.get_system_state()` every 1s but never writes work_accumulated

## Critical Patterns
- `DB_FILE` and `CONFIG_DIR` are centralized in `db_config.py` — import from there, never hardcode
- Workers claim tasks atomically via `BEGIN IMMEDIATE` transactions in `adaptive_worker.py`
- Rate-limited workers exit with code 42; adaptive manager detects this and scales down
- `sanitize_filename()` is duplicated in `adaptive_worker.py`, `worker_main.py`, and `gui_db.py`
- `set_system_phase()` in `gui_db.py` resets `work_accumulated` to 0 on every call
- Config files in `config/` are `worker_N_cities.json` with city/country/lat/lon arrays

## Key Gotcha
The GUI timer countdown appears stuck because `work_accumulated` is only updated by the adaptive manager process. When running in batch mode or standalone GUI, nothing increments it.
