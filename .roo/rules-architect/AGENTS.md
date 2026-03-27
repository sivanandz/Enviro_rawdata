# Project Architecture Rules (Non-Obvious Only)

- `tasks.db` SQLite is the single coordination hub — all processes (GUI, manager, workers) read/write the same DB file
- `system_state` single-row table acts as a shared memory bus for WORKING/RESTING phase coordination
- `work_accumulated` is written by `adaptive_manager.py` via `GlobalTimer.tick()` when running; GUI ticks it when manager isn't active
- Workers use `BEGIN IMMEDIATE` transactions for atomic task claiming — this creates a bottleneck under high concurrency but prevents double-processing
- `sanitize_filename()` is duplicated in 3 files rather than imported from a shared utility — any fix must be applied to all copies
- `set_system_phase()` resets `work_accumulated` to 0 — this means pause/resume from GUI restarts the work cycle timer
- The GUI's `_update_timer_display()` runs every 1s but only reads state — it does not participate in the timer state machine
- Rate limit exit code 42 is a contract between `adaptive_worker.py` and `adaptive_manager.py` — changing one requires changing the other
- No shared configuration module — `DB_FILE`, work/rest durations, and other constants are duplicated across files
