"""
ERA5 Mission Control — Database Helper Functions
Read-only queries + reset helpers for the GUI layer.
"""

import sqlite3
import os
import json
import glob
import unicodedata
import re
import time

from db_config import DB_FILE, CONFIG_DIR


def _get_conn():
    """Return a new SQLite connection (GUI uses short-lived reads)."""
    conn = sqlite3.connect(DB_FILE, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


# ──────────────────────────────────────────────
# TASK COUNTS
# ──────────────────────────────────────────────
def get_task_counts(year=None):
    """Return dict with pending, processing, completed, error counts."""
    conn = _get_conn()
    c = conn.cursor()
    query = "SELECT status, COUNT(*) as cnt FROM tasks"
    params = []
    if year is not None:
        query += " WHERE year = ?"
        params.append(year)
    query += " GROUP BY status"
    c.execute(query, params)
    counts = {"pending": 0, "processing": 0, "completed": 0, "error": 0, "total": 0}
    for row in c.fetchall():
        status = row["status"]
        cnt = row["cnt"]
        if status in counts:
            counts[status] = cnt
        counts["total"] += cnt
    conn.close()
    return counts


# ──────────────────────────────────────────────
# CITY QUERIES
# ──────────────────────────────────────────────
def get_all_cities():
    """Return list of CityRecord-like dicts for all 243 cities."""
    from gui_theme import CityRecord
    conn = _get_conn()
    c = conn.cursor()
    c.execute("SELECT id, city, country, lat, lon FROM cities ORDER BY city")
    cities = []
    for row in c.fetchall():
        cities.append(CityRecord(
            id=row["id"],
            city=row["city"],
            country=row["country"],
            lat=row["lat"],
            lon=row["lon"],
        ))
    conn.close()
    return cities


def get_city_by_name(city, country):
    """Look up a single city record by name+country."""
    from gui_theme import CityRecord
    conn = _get_conn()
    c = conn.cursor()
    c.execute("SELECT id, city, country, lat, lon FROM cities WHERE city=? AND country=?",
              (city, country))
    row = c.fetchone()
    conn.close()
    if row:
        return CityRecord(id=row["id"], city=row["city"], country=row["country"],
                          lat=row["lat"], lon=row["lon"])
    return None


# ──────────────────────────────────────────────
# FAILED CITY DETECTION
# ──────────────────────────────────────────────
def sanitize_filename(text):
    """Normalize text to ASCII, removing accents and special characters."""
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^\w\s-]", "", text)
    return text


def get_failed_cities(year):
    """Scan era5_data_{year}/ for cities with missing months. Returns list of FailedCity."""
    from gui_theme import FailedCity
    data_dir = f"era5_data_{year}"
    failed = []

    # Read from DB to get expected cities
    conn = _get_conn()
    c = conn.cursor()
    c.execute("SELECT DISTINCT c.city, c.country FROM cities c JOIN tasks t ON c.id = t.city_id WHERE t.year = ?",
              (year,))
    city_rows = c.fetchall()
    conn.close()

    for row in city_rows:
        city = row["city"]
        country = row["country"]
        clean_city = sanitize_filename(city)
        clean_country = sanitize_filename(country)
        missing = []
        for month in range(1, 13):
            expected = os.path.join(data_dir, f"{clean_city}_{clean_country}_{year}_{month:02d}.json")
            if not os.path.exists(expected):
                missing.append(month)
        if missing:
            failed.append(FailedCity(city=city, country=country, missing_months=missing))

    return failed


# ──────────────────────────────────────────────
# TASK RESET HELPERS
# ──────────────────────────────────────────────
def reset_all_tasks():
    """Reset all tasks to pending."""
    conn = _get_conn()
    c = conn.cursor()
    c.execute("UPDATE tasks SET status = 'pending', worker_id = NULL, error_message = NULL")
    count = c.rowcount
    conn.commit()
    conn.close()
    return count


def reset_error_tasks():
    """Reset only error tasks to pending."""
    conn = _get_conn()
    c = conn.cursor()
    c.execute("UPDATE tasks SET status = 'pending', worker_id = NULL, error_message = NULL WHERE status = 'error'")
    count = c.rowcount
    conn.commit()
    conn.close()
    return count


def reset_tasks_for_city(city_id, year):
    """Reset tasks for a specific city+year to pending."""
    conn = _get_conn()
    c = conn.cursor()
    c.execute("UPDATE tasks SET status = 'pending', worker_id = NULL, error_message = NULL WHERE city_id = ? AND year = ?",
              (city_id, year))
    count = c.rowcount
    conn.commit()
    conn.close()
    return count


# ──────────────────────────────────────────────
# SYSTEM STATE (TIMER)
# ──────────────────────────────────────────────
def get_system_state():
    """Return dict with phase, last_transition_time, work_accumulated."""
    conn = _get_conn()
    c = conn.cursor()
    try:
        c.execute("SELECT phase, last_transition_time, work_accumulated FROM system_state WHERE id = 1")
        row = c.fetchone()
        if row:
            return {"phase": row["phase"], "last_transition_time": row["last_transition_time"],
                    "work_accumulated": row["work_accumulated"]}
    except sqlite3.OperationalError:
        pass
    finally:
        conn.close()
    return {"phase": "WORKING", "last_transition_time": time.time(), "work_accumulated": 0}


def set_system_phase(phase):
    """Force set system phase (WORKING or RESTING)."""
    conn = _get_conn()
    c = conn.cursor()
    c.execute("UPDATE system_state SET phase = ?, last_transition_time = ?, work_accumulated = 0 WHERE id = 1",
              (phase, time.time()))
    conn.commit()
    conn.close()


# ──────────────────────────────────────────────
# WORKER DETAILS FROM DB
# ──────────────────────────────────────────────
def get_active_worker_details():
    """Get list of active workers from DB (tasks currently processing)."""
    conn = _get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT t.worker_id, t.city_id, c.city, c.country, t.year, t.month, t.last_attempt
        FROM tasks t
        JOIN cities c ON t.city_id = c.id
        WHERE t.status = 'processing'
        ORDER BY t.worker_id
    """)
    workers = []
    for row in c.fetchall():
        workers.append({
            "worker_id": row["worker_id"],
            "city": row["city"],
            "country": row["country"],
            "year": row["year"],
            "month": row["month"],
            "last_attempt": row["last_attempt"],
        })
    conn.close()
    return workers


# ──────────────────────────────────────────────
# DB INIT / SYNC (called from GUI menus)
# ──────────────────────────────────────────────
def init_db_gui(year):
    """Initialize DB and load cities from configs for a given year."""
    import init_db
    init_db.init_db()
    init_db.load_cities_from_configs(year)
    return True


def sync_db_gui(year):
    """Sync DB: clear and reload all data."""
    import sync_db
    sync_db.sync_db(year)
    return True


def ensure_db_exists():
    """Check if tasks.db exists and has tables."""
    if not os.path.exists(DB_FILE):
        return False
    try:
        conn = _get_conn()
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tasks'")
        result = c.fetchone()
        conn.close()
        return result is not None
    except Exception:
        return False


def get_years_in_db():
    """Get list of distinct years in tasks table."""
    conn = _get_conn()
    c = conn.cursor()
    try:
        c.execute("SELECT DISTINCT year FROM tasks ORDER BY year")
        years = [row["year"] for row in c.fetchall()]
    except sqlite3.OperationalError:
        years = []
    conn.close()
    return years


def get_total_cities():
    """Get count of cities in DB."""
    conn = _get_conn()
    c = conn.cursor()
    try:
        c.execute("SELECT COUNT(*) as cnt FROM cities")
        count = c.fetchone()["cnt"]
    except sqlite3.OperationalError:
        count = 0
    conn.close()
    return count