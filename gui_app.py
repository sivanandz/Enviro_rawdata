"""
ERA5 Mission Control — Main GUI Application
PySide6-based dashboard for monitoring and controlling ERA5 data downloads.
"""

import sys
import os
import time
from datetime import timedelta

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QSpinBox, QGroupBox, QFrame, QSplitter,
    QMenuBar, QMenu, QMessageBox, QStatusBar, QComboBox, QGridLayout,
    QSizePolicy, QInputDialog, QProgressBar,
)
from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QAction, QFont

from gui_theme import COLORS, apply_theme
from gui_widgets import (
    StatCard, ProgressCard, WorkerTableWidget,
    LogTerminalWidget, CityBrowserDialog, FailedCitiesDialog,
)
from gui_workers import (
    ManagerThread, BatchManagerThread, SingleCityThread, LogWatcherThread,
)
import gui_db
from timer_utils import GlobalTimer


# ═══════════════════════════════════════════════
# MAIN WINDOW
# ═══════════════════════════════════════════════
class MainWindow(QMainWindow):
    """ERA5 Mission Control Dashboard."""

    def __init__(self):
        super().__init__()
        self.setWindowTitle("🌍 ERA5 Mission Control")
        self.setMinimumSize(1200, 800)
        self.resize(1400, 900)

        # ── State ──
        self._manager_thread = None
        self._batch_thread = None
        self._single_city_thread = None
        self._log_watcher = None
        self._active_mode = None  # "adaptive" or "batch" or None
        self._paused = False

        # ── Global Timer (GUI ticks when adaptive manager isn't running) ──
        self._global_timer = GlobalTimer(work_duration=4*3600, rest_duration=1*3600)
        self._last_tick_time = time.time()

        # ── Build UI ──
        self._build_menu_bar()
        self._build_central_widget()
        self._build_status_bar()

        # ── Periodic update timers ──
        self._timer_refresh = QTimer(self)
        self._timer_refresh.timeout.connect(self._update_timer_display)
        self._timer_refresh.start(1000)

        self._progress_refresh = QTimer(self)
        self._progress_refresh.timeout.connect(self._update_progress)
        self._progress_refresh.start(2000)

        self._worker_refresh = QTimer(self)
        self._worker_refresh.timeout.connect(self._update_worker_grid)
        self._worker_refresh.start(3000)

        # ── Failed cities file watcher ──
        self._failed_cities_shown = False
        self._failed_cities_mtime = 0.0
        self._failed_cities_watcher = QTimer(self)
        self._failed_cities_watcher.timeout.connect(self._check_failed_cities_file)
        self._failed_cities_watcher.start(5000)

        # ── Start log watcher ──
        self._start_log_watcher()

        # ── Initial refresh ──
        self._update_timer_display()
        self._update_progress()

    # ──────────────────────────────────────────
    # MENU BAR
    # ──────────────────────────────────────────
    def _build_menu_bar(self):
        menubar = self.menuBar()

        # ── Database Menu ──
        db_menu = menubar.addMenu("💾 Database")

        act_init = QAction("Initialize DB...", self)
        act_init.triggered.connect(self._init_db_gui)
        db_menu.addAction(act_init)

        act_sync = QAction("Sync DB (Full Reload)...", self)
        act_sync.triggered.connect(self._sync_db_gui)
        db_menu.addAction(act_sync)

        db_menu.addSeparator()

        act_reset_all = QAction("Reset All Tasks to Pending", self)
        act_reset_all.triggered.connect(self._reset_all_tasks_gui)
        db_menu.addAction(act_reset_all)

        act_reset_errors = QAction("Reset Error Tasks Only", self)
        act_reset_errors.triggered.connect(self._reset_error_tasks_gui)
        db_menu.addAction(act_reset_errors)

        # ── Tools Menu ──
        tools_menu = menubar.addMenu("🔧 Tools")

        act_city_browser = QAction("🌍 City Browser...", self)
        act_city_browser.triggered.connect(self._open_city_browser)
        tools_menu.addAction(act_city_browser)

        act_failed = QAction("⚠ Check Failed Cities...", self)
        act_failed.triggered.connect(self._open_failed_cities)
        tools_menu.addAction(act_failed)

        # ── Help Menu ──
        help_menu = menubar.addMenu("❓ Help")
        act_about = QAction("About", self)
        act_about.triggered.connect(self._show_about)
        help_menu.addAction(act_about)

    # ──────────────────────────────────────────
    # CENTRAL WIDGET
    # ──────────────────────────────────────────
    def _build_central_widget(self):
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setSpacing(8)
        main_layout.setContentsMargins(10, 10, 10, 10)

        # ── Top Section: Timer + Controls ──
        top_splitter = QSplitter(Qt.Horizontal)

        # Timer Section
        timer_frame = self._build_timer_section()
        top_splitter.addWidget(timer_frame)

        # Controls Section
        controls_frame = self._build_controls()
        top_splitter.addWidget(controls_frame)

        top_splitter.setStretchFactor(0, 2)
        top_splitter.setStretchFactor(1, 3)
        main_layout.addWidget(top_splitter, stretch=0)

        # ── Middle Section: Stats + Progress ──
        stats_progress_frame = self._build_progress_section()
        main_layout.addWidget(stats_progress_frame, stretch=0)

        # ── Worker Grid ──
        worker_frame = self._build_worker_grid()
        main_layout.addWidget(worker_frame, stretch=1)

        # ── Log Terminal ──
        log_frame = self._build_log_terminal()
        main_layout.addWidget(log_frame, stretch=2)

    # ──────────────────────────────────────────
    # TIMER SECTION
    # ──────────────────────────────────────────
    def _build_timer_section(self):
        frame = QFrame()
        frame.setObjectName("card")
        layout = QVBoxLayout(frame)
        layout.setContentsMargins(12, 8, 12, 8)

        title = QLabel("⏱ Global Timer")
        title.setObjectName("titleLabel")
        layout.addWidget(title)

        # Phase indicator
        self._phase_label = QLabel("WORKING")
        self._phase_label.setStyleSheet(f"color: {COLORS['accent_green']}; font-size: 28px; font-weight: bold;")
        self._phase_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self._phase_label)

        # Countdown
        self._countdown_label = QLabel("00:00:00")
        self._countdown_label.setStyleSheet(f"color: {COLORS['text_primary']}; font-size: 22px; font-family: Consolas;")
        self._countdown_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self._countdown_label)

        # Progress bar for work/rest cycle
        self._timer_progress = QProgressBar()
        self._timer_progress.setRange(0, 100)
        self._timer_progress.setValue(0)
        self._timer_progress.setTextVisible(True)
        layout.addWidget(self._timer_progress)

        # Work accumulated label
        self._work_acc_label = QLabel("Work: 0.00h / 4.00h")
        self._work_acc_label.setObjectName("dimLabel")
        self._work_acc_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self._work_acc_label)

        # Force phase buttons
        phase_row = QHBoxLayout()
        btn_force_work = QPushButton("⚡ Force WORK")
        btn_force_work.setObjectName("btnGreen")
        btn_force_work.clicked.connect(lambda: self._force_phase("WORKING"))
        phase_row.addWidget(btn_force_work)

        btn_force_rest = QPushButton("😴 Force REST")
        btn_force_rest.setObjectName("btnAmber")
        btn_force_rest.clicked.connect(lambda: self._force_phase("RESTING"))
        phase_row.addWidget(btn_force_rest)
        layout.addLayout(phase_row)

        return frame

    # ──────────────────────────────────────────
    # CONTROLS SECTION
    # ──────────────────────────────────────────
    def _build_controls(self):
        frame = QFrame()
        frame.setObjectName("card")
        layout = QVBoxLayout(frame)
        layout.setContentsMargins(12, 8, 12, 8)

        title = QLabel("🎮 Controls")
        title.setObjectName("titleLabel")
        layout.addWidget(title)

        # Year selector
        year_row = QHBoxLayout()
        year_label = QLabel("Target Year:")
        year_row.addWidget(year_label)
        self._year_spin = QSpinBox()
        self._year_spin.setRange(2020, 2030)
        self._year_spin.setValue(2025)
        year_row.addWidget(self._year_spin)
        year_row.addStretch()
        layout.addLayout(year_row)

        # Mode selection
        mode_row = QHBoxLayout()
        mode_label = QLabel("Mode:")
        mode_row.addWidget(mode_label)

        self._btn_adaptive = QPushButton("🚀 Start Adaptive")
        self._btn_adaptive.setObjectName("btnGreen")
        self._btn_adaptive.clicked.connect(self._start_adaptive)
        mode_row.addWidget(self._btn_adaptive)

        layout.addLayout(mode_row)

        # Batch settings
        batch_row = QHBoxLayout()
        batch_label = QLabel("Batch Size:")
        batch_row.addWidget(batch_label)
        self._batch_size_spin = QSpinBox()
        self._batch_size_spin.setRange(1, 20)
        self._batch_size_spin.setValue(5)
        batch_row.addWidget(self._batch_size_spin)

        num_label = QLabel("Workers:")
        batch_row.addWidget(num_label)
        self._num_workers_spin = QSpinBox()
        self._num_workers_spin.setRange(1, 100)
        self._num_workers_spin.setValue(100)
        batch_row.addWidget(self._num_workers_spin)

        self._btn_batch = QPushButton("📦 Start Batch")
        self._btn_batch.setObjectName("btnGreen")
        self._btn_batch.clicked.connect(self._start_batch)
        batch_row.addWidget(self._btn_batch)
        layout.addLayout(batch_row)

        # Stop / Pause / Kill
        ctrl_row = QHBoxLayout()

        self._btn_stop = QPushButton("⏹ Stop All")
        self._btn_stop.setObjectName("btnRed")
        self._btn_stop.clicked.connect(self._stop_all)
        self._btn_stop.setEnabled(False)
        ctrl_row.addWidget(self._btn_stop)

        self._btn_pause = QPushButton("⏸ Pause")
        self._btn_pause.setObjectName("btnAmber")
        self._btn_pause.clicked.connect(self._pause_resume)
        self._btn_pause.setEnabled(False)
        ctrl_row.addWidget(self._btn_pause)

        self._btn_kill = QPushButton("💀 Emergency Kill")
        self._btn_kill.setObjectName("btnRed")
        self._btn_kill.clicked.connect(self._emergency_kill)
        self._btn_kill.setEnabled(False)
        ctrl_row.addWidget(self._btn_kill)

        layout.addLayout(ctrl_row)

        # Quick tools
        tools_row = QHBoxLayout()
        btn_browser = QPushButton("🌍 City Browser")
        btn_browser.clicked.connect(self._open_city_browser)
        tools_row.addWidget(btn_browser)

        btn_failed = QPushButton("⚠ Failed Cities")
        btn_failed.clicked.connect(self._open_failed_cities)
        tools_row.addWidget(btn_failed)

        layout.addLayout(tools_row)

        return frame

    # ──────────────────────────────────────────
    # PROGRESS SECTION
    # ──────────────────────────────────────────
    def _build_progress_section(self):
        frame = QFrame()
        frame.setObjectName("card")
        layout = QHBoxLayout(frame)
        layout.setContentsMargins(8, 4, 8, 4)
        layout.setSpacing(12)

        # Overall progress card
        self._progress_card = ProgressCard("Overall Progress")
        layout.addWidget(self._progress_card)

        # Stat cards
        self._stat_pending = StatCard("Pending", "0", COLORS["warning"])
        layout.addWidget(self._stat_pending)

        self._stat_processing = StatCard("Processing", "0", COLORS["accent_teal"])
        layout.addWidget(self._stat_processing)

        self._stat_completed = StatCard("Completed", "0", COLORS["success"])
        layout.addWidget(self._stat_completed)

        self._stat_error = StatCard("Errors", "0", COLORS["error"])
        layout.addWidget(self._stat_error)

        self._stat_cities = StatCard("Cities", "0", COLORS["accent_green"])
        layout.addWidget(self._stat_cities)

        return frame

    # ──────────────────────────────────────────
    # WORKER GRID
    # ──────────────────────────────────────────
    def _build_worker_grid(self):
        frame = QFrame()
        frame.setObjectName("card")
        layout = QVBoxLayout(frame)
        layout.setContentsMargins(8, 4, 8, 4)

        header_row = QHBoxLayout()
        title = QLabel("👥 Active Workers")
        title.setObjectName("titleLabel")
        header_row.addWidget(title)
        header_row.addStretch()
        self._worker_count_label = QLabel("0 active")
        self._worker_count_label.setObjectName("dimLabel")
        header_row.addWidget(self._worker_count_label)
        layout.addLayout(header_row)

        self._worker_table = WorkerTableWidget()
        layout.addWidget(self._worker_table)

        return frame

    # ──────────────────────────────────────────
    # LOG TERMINAL
    # ──────────────────────────────────────────
    def _build_log_terminal(self):
        frame = QFrame()
        frame.setObjectName("card")
        layout = QVBoxLayout(frame)
        layout.setContentsMargins(8, 4, 8, 4)

        title = QLabel("📋 Log Terminal")
        title.setObjectName("titleLabel")
        layout.addWidget(title)

        self._log_terminal = LogTerminalWidget()
        layout.addWidget(self._log_terminal)

        return frame

    # ──────────────────────────────────────────
    # STATUS BAR
    # ──────────────────────────────────────────
    def _build_status_bar(self):
        self._statusbar = QStatusBar()
        self.setStatusBar(self._statusbar)
        self._status_label = QLabel("Ready")
        self._statusbar.addWidget(self._status_label)
        self._mode_label = QLabel("Mode: Idle")
        self._statusbar.addPermanentWidget(self._mode_label)

    # ══════════════════════════════════════════
    # ACTIONS — START / STOP / CONTROL
    # ══════════════════════════════════════════

    def _set_running_ui(self, running=True):
        """Enable/disable buttons based on running state."""
        self._btn_stop.setEnabled(running)
        self._btn_kill.setEnabled(running)
        self._btn_pause.setEnabled(running)
        self._btn_adaptive.setEnabled(not running)
        self._btn_batch.setEnabled(not running)

    def _start_adaptive(self):
        """Start adaptive manager in background thread."""
        if self._active_mode:
            QMessageBox.warning(self, "Already Running", "Stop current process first.")
            return

        self._active_mode = "adaptive"
        self._set_running_ui(True)
        self._mode_label.setText("Mode: Adaptive")
        self._status_label.setText("Starting Adaptive Manager...")

        self._manager_thread = ManagerThread(self)
        self._manager_thread.log_entry.connect(self._on_log_entry)
        self._manager_thread.status_update.connect(self._on_manager_status)
        self._manager_thread.finished_signal.connect(self._on_manager_finished)
        self._manager_thread.start()

        self._log_terminal.append_log({
            "timestamp": time.strftime("%H:%M:%S"),
            "worker_id": 0,
            "level": "INFO",
            "message": "Adaptive Manager started from GUI"
        })

    def _start_batch(self):
        """Start batch workers in background thread."""
        if self._active_mode:
            QMessageBox.warning(self, "Already Running", "Stop current process first.")
            return

        year = self._year_spin.value()
        batch_size = self._batch_size_spin.value()
        num_workers = self._num_workers_spin.value()

        self._active_mode = "batch"
        self._set_running_ui(True)
        self._mode_label.setText(f"Mode: Batch ({num_workers} workers)")
        self._status_label.setText("Starting Batch Mode...")

        self._batch_thread = BatchManagerThread(
            year=year, batch_size=batch_size, num_workers=num_workers, parent=self
        )
        self._batch_thread.log_entry.connect(self._on_log_entry)
        self._batch_thread.batch_started.connect(self._on_batch_started)
        self._batch_thread.batch_completed.connect(self._on_batch_completed)
        self._batch_thread.all_done.connect(self._on_batch_all_done)
        self._batch_thread.start()

    def _stop_all(self):
        """Stop all running processes gracefully."""
        self._status_label.setText("Stopping all processes...")
        self._log_terminal.append_log({
            "timestamp": time.strftime("%H:%M:%S"),
            "worker_id": 0,
            "level": "WARNING",
            "message": "Stopping all processes..."
        })

        if self._manager_thread and self._manager_thread.isRunning():
            self._manager_thread.stop()
            self._manager_thread.wait(5000)

        if self._batch_thread and self._batch_thread.isRunning():
            self._batch_thread.stop()
            self._batch_thread.wait(5000)

        if self._single_city_thread and self._single_city_thread.isRunning():
            self._single_city_thread.stop()
            self._single_city_thread.wait(5000)

        self._active_mode = None
        self._set_running_ui(False)
        self._mode_label.setText("Mode: Idle")
        self._status_label.setText("All processes stopped.")

    def _pause_resume(self):
        """Toggle pause state by forcing rest/work phase."""
        if not self._active_mode:
            return
        if not self._paused:
            gui_db.set_system_phase("RESTING")
            self._paused = True
            self._btn_pause.setText("▶ Resume")
            self._status_label.setText("PAUSED — Workers will wait at rest phase")
        else:
            gui_db.set_system_phase("WORKING")
            self._paused = False
            self._btn_pause.setText("⏸ Pause")
            self._status_label.setText("Resumed — Workers will continue")

    def _force_phase(self, phase):
        """Force the global timer to a specific phase."""
        gui_db.set_system_phase(phase)
        self._log_terminal.append_log({
            "timestamp": time.strftime("%H:%M:%S"),
            "worker_id": 0,
            "level": "INFO",
            "message": f"Forced phase to: {phase}"
        })
        self._update_timer_display()

    def _emergency_kill(self):
        """Kill all processes immediately."""
        reply = QMessageBox.critical(
            self, "Emergency Kill",
            "⚠ This will forcefully kill ALL worker processes.\n\nAre you sure?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        self._log_terminal.append_log({
            "timestamp": time.strftime("%H:%M:%S"),
            "worker_id": 0,
            "level": "ERROR",
            "message": "EMERGENCY KILL — Terminating all processes!"
        })

        if self._manager_thread:
            self._manager_thread.stop()
        if self._batch_thread:
            self._batch_thread.stop()
        if self._single_city_thread:
            self._single_city_thread.stop()

        # Also kill any orphaned python processes running our scripts
        try:
            if sys.platform == "win32":
                os.system('taskkill /F /FI "WINDOWTITLE eq adaptive_worker*" 2>NUL')
                os.system('taskkill /F /FI "WINDOWTITLE eq worker_main*" 2>NUL')
            else:
                os.system("pkill -f adaptive_worker.py 2>/dev/null")
                os.system("pkill -f worker_main.py 2>/dev/null")
        except Exception:
            pass

        self._active_mode = None
        self._set_running_ui(False)
        self._mode_label.setText("Mode: Idle (Killed)")
        self._status_label.setText("Emergency kill completed.")

    # ══════════════════════════════════════════
    # CITY BROWSER
    # ══════════════════════════════════════════

    def _open_city_browser(self):
        """Open city browser dialog."""
        if not gui_db.ensure_db_exists():
            QMessageBox.warning(self, "No DB", "Database not found. Please init DB first.")
            return

        cities = gui_db.get_all_cities()
        if not cities:
            QMessageBox.warning(self, "No Cities", "No cities found in DB. Please sync DB first.")
            return

        dialog = CityBrowserDialog(cities, self)
        dialog.cities_selected.connect(self._run_selected_cities)
        dialog.exec()

    def _run_selected_cities(self, cities):
        """Run downloads for selected cities."""
        year = self._year_spin.value()
        self._status_label.setText(f"Running {len(cities)} cities for {year}...")

        self._single_city_thread = SingleCityThread(cities, year, self)
        self._single_city_thread.log_entry.connect(self._on_log_entry)
        self._single_city_thread.download_complete.connect(
            lambda c: self._status_label.setText(f"Completed: {c}")
        )
        self._single_city_thread.start()

    # ══════════════════════════════════════════
    # FAILED CITIES
    # ══════════════════════════════════════════

    def _open_failed_cities(self):
        """Open failed cities dialog."""
        year = self._year_spin.value()
        failed = gui_db.get_failed_cities(year)

        if not failed:
            QMessageBox.information(self, "No Failures", f"All cities have complete data for {year}! 🎉")
            return

        dialog = FailedCitiesDialog(failed, self)
        dialog.retry_cities.connect(self._retry_failed_cities)
        dialog.exec()

    def _retry_failed_cities(self, failed_cities):
        """Retry selected failed cities."""
        year = self._year_spin.value()

        # Reset tasks for failed cities
        for fc in failed_cities:
            city_rec = gui_db.get_city_by_name(fc.city, fc.country)
            if city_rec:
                gui_db.reset_tasks_for_city(city_rec.id, year)

        # Convert FailedCity to CityRecord for SingleCityThread
        city_records = []
        for fc in failed_cities:
            city_rec = gui_db.get_city_by_name(fc.city, fc.country)
            if city_rec:
                city_records.append(city_rec)

        if city_records:
            self._status_label.setText(f"Retrying {len(city_records)} failed cities...")
            self._single_city_thread = SingleCityThread(city_records, year, self)
            self._single_city_thread.log_entry.connect(self._on_log_entry)
            self._single_city_thread.start()

    # ══════════════════════════════════════════
    # DB OPERATIONS
    # ══════════════════════════════════════════

    def _init_db_gui(self):
        year, ok = QInputDialog.getInt(self, "Initialize DB", "Target Year:", 2025, 2020, 2030)
        if not ok:
            return
        try:
            gui_db.init_db_gui(year)
            QMessageBox.information(self, "Success", f"DB initialized for {year}.")
            self._update_progress()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to init DB: {e}")

    def _sync_db_gui(self):
        year, ok = QInputDialog.getInt(self, "Sync DB", "Target Year:", 2025, 2020, 2030)
        if not ok:
            return
        reply = QMessageBox.warning(
            self, "Confirm Sync",
            "This will clear ALL tasks and cities, then reload from configs.\n\nContinue?",
            QMessageBox.Yes | QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return
        try:
            gui_db.sync_db_gui(year)
            QMessageBox.information(self, "Success", f"DB synced for {year}.")
            self._update_progress()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to sync DB: {e}")

    def _reset_all_tasks_gui(self):
        reply = QMessageBox.warning(
            self, "Reset All Tasks",
            "Reset ALL tasks to pending? This cannot be undone.",
            QMessageBox.Yes | QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return
        try:
            count = gui_db.reset_all_tasks()
            QMessageBox.information(self, "Done", f"Reset {count} tasks to pending.")
            self._update_progress()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed: {e}")

    def _reset_error_tasks_gui(self):
        try:
            count = gui_db.reset_error_tasks()
            QMessageBox.information(self, "Done", f"Reset {count} error tasks to pending.")
            self._update_progress()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed: {e}")

    # ══════════════════════════════════════════
    # PERIODIC REFRESH SLOTS
    # ══════════════════════════════════════════

    def _update_timer_display(self):
        """Refresh timer section from DB."""
        # Tick the global timer to keep work_accumulated current.
        # Only tick from GUI when adaptive manager isn't running (avoids double-counting).
        now = time.time()
        delta = now - self._last_tick_time
        self._last_tick_time = now
        if delta > 0 and delta < 10 and self._active_mode != "adaptive":
            self._global_timer.tick(delta)

        state = gui_db.get_system_state()
        phase = state["phase"]

        work_duration = self._global_timer.work_duration
        rest_duration = self._global_timer.rest_duration

        if phase == "WORKING":
            remaining = max(0, work_duration - state["work_accumulated"])
            pct = int(100 * state["work_accumulated"] / work_duration) if work_duration > 0 else 0
            self._phase_label.setText("⚡ WORKING")
            self._phase_label.setStyleSheet(f"color: {COLORS['accent_green']}; font-size: 28px; font-weight: bold;")
            self._countdown_label.setText(f"Rest in: {self._format_seconds(remaining)}")
            self._timer_progress.setValue(pct)
            self._timer_progress.setFormat(f"Work Progress: {pct}%")
            self._timer_progress.setStyleSheet(
                f"QProgressBar::chunk {{ background-color: {COLORS['accent_green']}; border-radius: 5px; }}"
            )
            self._work_acc_label.setText(
                f"Work: {state['work_accumulated']/3600:.2f}h / {work_duration/3600:.2f}h"
            )
        else:
            elapsed = time.time() - state["last_transition_time"]
            remaining = max(0, rest_duration - elapsed)
            pct = int(100 * elapsed / rest_duration) if rest_duration > 0 else 0
            self._phase_label.setText("😴 RESTING")
            self._phase_label.setStyleSheet(f"color: {COLORS['resting']}; font-size: 28px; font-weight: bold;")
            self._countdown_label.setText(f"Resume in: {self._format_seconds(remaining)}")
            self._timer_progress.setValue(min(pct, 100))
            self._timer_progress.setFormat(f"Rest Progress: {min(pct, 100)}%")
            self._timer_progress.setStyleSheet(
                f"QProgressBar::chunk {{ background-color: {COLORS['resting']}; border-radius: 5px; }}"
            )
            self._work_acc_label.setText(
                f"Rest elapsed: {elapsed/60:.1f} min"
            )

    def _update_progress(self):
        """Refresh progress cards and stat cards."""
        year = self._year_spin.value()
        counts = gui_db.get_task_counts(year)

        completed = counts["completed"]
        total = counts["total"]
        self._progress_card.set_progress(completed, total)

        self._stat_pending.set_value(str(counts["pending"]), COLORS["warning"])
        self._stat_processing.set_value(str(counts["processing"]), COLORS["accent_teal"])
        self._stat_completed.set_value(str(completed), COLORS["success"])
        self._stat_error.set_value(str(counts["error"]),
                                   COLORS["error"] if counts["error"] > 0 else COLORS["success"])

        # Cities count
        num_cities = gui_db.get_total_cities()
        self._stat_cities.set_value(str(num_cities))

    def _update_worker_grid(self):
        """Refresh worker table from DB."""
        workers = gui_db.get_active_worker_details()
        self._worker_count_label.setText(f"{len(workers)} active")

        # Convert to table format
        table_data = []
        for w in workers:
            table_data.append({
                "worker_id": w["worker_id"] or "-",
                "pid": "-",
                "status": "downloading",
                "city": w["city"],
                "country": w["country"],
                "month": w["month"],
                "tasks_done": "-",
            })
        self._worker_table.update_workers(table_data)

    # ══════════════════════════════════════════
    # FAILED CITIES FILE WATCHER
    # ══════════════════════════════════════════

    def _check_failed_cities_file(self):
        """Auto-detect failed_cities.txt and popup if new/changed."""
        fc_path = os.path.join(os.getcwd(), "failed_cities.txt")
        if not os.path.exists(fc_path):
            self._failed_cities_shown = False
            return

        try:
            mtime = os.path.getmtime(fc_path)
        except OSError:
            return

        # Only popup if file is new or modified since last shown
        if mtime <= self._failed_cities_mtime and self._failed_cities_shown:
            return

        # Read file to check if it has content
        try:
            with open(fc_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
            if not content:
                return
        except Exception:
            return

        # File exists and has content — auto-popup
        self._failed_cities_mtime = mtime
        self._failed_cities_shown = True

        self._log_terminal.append_log({
            "timestamp": time.strftime("%H:%M:%S"),
            "worker_id": 0,
            "level": "WARNING",
            "message": f"⚠ failed_cities.txt detected — auto-opening Failed Cities dialog"
        })

        # Open failed cities dialog automatically
        self._open_failed_cities()

    # ══════════════════════════════════════════
    # SIGNAL HANDLERS
    # ══════════════════════════════════════════

    def _on_log_entry(self, entry):
        """Handle a log entry from any thread."""
        self._log_terminal.append_log(entry)

    def _on_manager_status(self, status):
        """Handle status update from manager."""
        self._status_label.setText(status.get("raw", ""))

    def _on_manager_finished(self, msg):
        """Handle manager thread finishing."""
        self._active_mode = None
        self._set_running_ui(False)
        self._mode_label.setText("Mode: Idle")
        self._status_label.setText("Adaptive Manager finished.")
        self._log_terminal.append_log({
            "timestamp": time.strftime("%H:%M:%S"),
            "worker_id": 0,
            "level": "INFO",
            "message": "Adaptive Manager has finished."
        })

    def _on_batch_started(self, start, end):
        self._status_label.setText(f"Batch {start}-{end} started...")

    def _on_batch_completed(self, start, end):
        self._status_label.setText(f"Batch {start}-{end} completed.")
        self._update_progress()

    def _on_batch_all_done(self):
        self._active_mode = None
        self._set_running_ui(False)
        self._mode_label.setText("Mode: Idle")
        self._status_label.setText("All batches completed!")
        self._update_progress()

    # ══════════════════════════════════════════
    # LOG WATCHER
    # ══════════════════════════════════════════

    def _start_log_watcher(self):
        """Start the log file watcher thread."""
        self._log_watcher = LogWatcherThread(self)
        self._log_watcher.new_log_entry.connect(self._on_log_entry)
        self._log_watcher.start()

    # ══════════════════════════════════════════
    # HELPERS
    # ══════════════════════════════════════════

    @staticmethod
    def _format_seconds(seconds):
        td = timedelta(seconds=int(seconds))
        return str(td)

    def _show_about(self):
        QMessageBox.about(
            self, "About ERA5 Mission Control",
            "<h3>🌍 ERA5 Mission Control</h3>"
            "<p>A PySide6-based dashboard for monitoring and controlling "
            "ERA5 environmental data downloads.</p>"
            "<p><b>Features:</b></p>"
            "<ul>"
            "<li>Adaptive & Batch worker management</li>"
            "<li>Real-time progress monitoring</li>"
            "<li>Global work/rest timer</li>"
            "<li>City browser & failed city retry</li>"
            "<li>Embedded log terminal</li>"
            "</ul>"
            "<p>Built with PySide6 + Dark Mission Control Green Theme.</p>"
        )

    def closeEvent(self, event):
        """Clean up threads on close."""
        if self._log_watcher:
            self._log_watcher.stop()
            self._log_watcher.wait(3000)

        if self._manager_thread and self._manager_thread.isRunning():
            self._manager_thread.stop()
            self._manager_thread.wait(3000)

        if self._batch_thread and self._batch_thread.isRunning():
            self._batch_thread.stop()
            self._batch_thread.wait(3000)

        if self._single_city_thread and self._single_city_thread.isRunning():
            self._single_city_thread.stop()
            self._single_city_thread.wait(3000)

        event.accept()


# ═══════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════
def main():
    app = QApplication(sys.argv)
    app.setApplicationName("ERA5 Mission Control")
    app.setStyle("Fusion")

    # Apply dark green theme
    apply_theme(app)

    window = MainWindow()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()