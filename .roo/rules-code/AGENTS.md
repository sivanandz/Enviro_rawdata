# Project Coding Rules (Non-Obvious Only)

- `DB_FILE` and `CONFIG_DIR` are centralized in `db_config.py` — always import from there, never hardcode `"tasks.db"`
- `sanitize_filename()` is copy-pasted in `adaptive_worker.py`, `worker_main.py`, and `gui_db.py` — if you fix one, fix all three
- `set_system_phase()` in `gui_db.py` always resets `work_accumulated` to 0 — this is intentional but causes timer reset on pause/unpause
- Workers claim tasks via `BEGIN IMMEDIATE` transactions in `adaptive_worker.py` — do not add SELECT+UPDATE without a transaction or workers will race
- Rate-limited workers exit with code 42 — `adaptive_manager.py` catches this to scale down; do not change this exit code
- `GlobalTimer.tick()` in `timer_utils.py` is the ONLY method that increments `work_accumulated` — only `adaptive_manager.py` calls it
- GUI reads timer state via `gui_db.get_system_state()` but never writes `work_accumulated` — this is why the countdown appears stuck
- Config files in `config/` follow the naming pattern `worker_N_cities.json` with arrays of `{city, country, lat, lon}` objects
- `system_state` table is a single-row table with `CHECK (id = 1)` — never insert a second row
- `init_db.py` uses `INSERT OR IGNORE` for both cities and tasks — re-running it is safe and idempotent
