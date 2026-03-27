"""
ERA5 Mission Control — Unit Tests for GUI Database Layer
"""

import unittest
import sqlite3
import os
import time
import sys

# Ensure we're in the right directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))


class TestGUIDB(unittest.TestCase):
    """Tests for gui_db module."""

    @classmethod
    def setUpClass(cls):
        """Create a test database if it doesn't exist."""
        from db_config import DB_FILE
        if not os.path.exists(DB_FILE):
            import init_db
            init_db.init_db()
            init_db.load_cities_from_configs(2025)

    def test_ensure_db_exists(self):
        """Test DB existence check."""
        import gui_db
        self.assertTrue(gui_db.ensure_db_exists())

    def test_get_task_counts(self):
        """Test task counts query."""
        import gui_db
        counts = gui_db.get_task_counts()
        self.assertIn("pending", counts)
        self.assertIn("processing", counts)
        self.assertIn("completed", counts)
        self.assertIn("error", counts)
        self.assertIn("total", counts)
        self.assertGreaterEqual(counts["total"], 0)

    def test_get_task_counts_by_year(self):
        """Test task counts query with year filter."""
        import gui_db
        counts = gui_db.get_task_counts(2025)
        self.assertIn("total", counts)

    def test_get_all_cities(self):
        """Test getting all cities."""
        import gui_db
        cities = gui_db.get_all_cities()
        self.assertIsInstance(cities, list)
        if len(cities) > 0:
            city = cities[0]
            self.assertTrue(hasattr(city, 'city'))
            self.assertTrue(hasattr(city, 'country'))
            self.assertTrue(hasattr(city, 'lat'))
            self.assertTrue(hasattr(city, 'lon'))

    def test_get_city_by_name(self):
        """Test city lookup by name."""
        import gui_db
        cities = gui_db.get_all_cities()
        if cities:
            city = cities[0]
            found = gui_db.get_city_by_name(city.city, city.country)
            self.assertIsNotNone(found)
            self.assertEqual(found.city, city.city)

    def test_get_system_state(self):
        """Test system state query."""
        import gui_db
        state = gui_db.get_system_state()
        self.assertIn("phase", state)
        self.assertIn("last_transition_time", state)
        self.assertIn("work_accumulated", state)
        self.assertIn(state["phase"], ["WORKING", "RESTING"])

    def test_get_years_in_db(self):
        """Test getting years from DB."""
        import gui_db
        years = gui_db.get_years_in_db()
        self.assertIsInstance(years, list)

    def test_get_total_cities(self):
        """Test getting total city count."""
        import gui_db
        count = gui_db.get_total_cities()
        self.assertGreaterEqual(count, 0)

    def test_sanitize_filename(self):
        """Test filename sanitization."""
        import gui_db
        self.assertEqual(gui_db.sanitize_filename("São Paulo"), "Sao Paulo")
        self.assertEqual(gui_db.sanitize_filename("New York"), "New York")
        self.assertEqual(gui_db.sanitize_filename("Tel Aviv-Yafo"), "Tel Aviv-Yafo")

    def test_get_active_worker_details(self):
        """Test active worker details query."""
        import gui_db
        workers = gui_db.get_active_worker_details()
        self.assertIsInstance(workers, list)

    def test_set_system_phase(self):
        """Test setting system phase."""
        import gui_db
        original = gui_db.get_system_state()
        gui_db.set_system_phase("WORKING")
        state = gui_db.get_system_state()
        self.assertEqual(state["phase"], "WORKING")
        # Restore
        gui_db.set_system_phase(original["phase"])


class TestGUITheme(unittest.TestCase):
    """Tests for gui_theme module."""

    def test_colors_exist(self):
        """Test color palette has all required keys."""
        from gui_theme import COLORS
        required = ["bg_dark", "bg_primary", "bg_card", "accent_green",
                     "text_primary", "error", "warning", "success"]
        for key in required:
            self.assertIn(key, COLORS)

    def test_stylesheet_not_empty(self):
        """Test stylesheet is generated."""
        from gui_theme import STYLESHEET
        self.assertGreater(len(STYLESHEET), 100)

    def test_enums(self):
        """Test enum definitions."""
        from gui_theme import WorkerStatus, TaskStatus
        self.assertEqual(WorkerStatus.IDLE.value, "idle")
        self.assertEqual(TaskStatus.COMPLETED.value, "completed")

    def test_dataclasses(self):
        """Test dataclass creation."""
        from gui_theme import WorkerInfo, LogEntry, CityRecord, FailedCity
        wi = WorkerInfo(worker_id=1, pid=123, status="idle", current_city="Test",
                        country="Land", start_time=0.0, tasks_completed=5)
        self.assertEqual(wi.worker_id, 1)


class TestGUIImports(unittest.TestCase):
    """Test that all GUI modules can be imported."""

    def test_import_gui_theme(self):
        import gui_theme
        self.assertTrue(hasattr(gui_theme, 'COLORS'))
        self.assertTrue(hasattr(gui_theme, 'STYLESHEET'))
        self.assertTrue(hasattr(gui_theme, 'apply_theme'))

    def test_import_gui_db(self):
        import gui_db
        self.assertTrue(hasattr(gui_db, 'get_task_counts'))
        self.assertTrue(hasattr(gui_db, 'get_all_cities'))
        self.assertTrue(hasattr(gui_db, 'get_failed_cities'))

    def test_import_gui_widgets(self):
        from gui_widgets import StatCard, ProgressCard, WorkerTableWidget
        from gui_widgets import LogTerminalWidget, CityBrowserDialog, FailedCitiesDialog

    def test_import_gui_workers(self):
        from gui_workers import ManagerThread, BatchManagerThread
        from gui_workers import SingleCityThread, LogWatcherThread

    def test_import_gui_app(self):
        from gui_app import MainWindow, main


if __name__ == "__main__":
    unittest.main(verbosity=2)