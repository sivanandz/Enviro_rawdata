# Project Debug Rules (Non-Obvious Only)

- GUI timer countdown stuck at 4:00:00 was fixed — GUI now ticks `GlobalTimer` when adaptive manager isn't running
- When adaptive manager IS running, it ticks the timer; GUI skips ticking to avoid double-counting
- RESTING countdown works because it uses `time.time() - last_transition_time` (real-time calc), while WORKING countdown uses `work_duration - work_accumulated` (DB value)
- `set_system_phase()` in `gui_db.py` resets `work_accumulated` to 0 every time — calling Force WORK/REST restarts the timer
- Workers block in `timer.check_wait()` during RESTING phase, polling every 5 seconds — this is normal, not a hang
- SQLite `BEGIN IMMEDIATE` can throw `OperationalError: database is locked` under high concurrency — workers retry with random backoff
- No test framework exists — tests are standalone scripts (`test_global_timer.py`, `test_gui.py`) that must be run manually
- `tasks.db` must exist before GUI or workers can function — run `python init_db.py` first
