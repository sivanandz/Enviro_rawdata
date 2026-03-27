"""
ERA5 Mission Control — Custom Widgets
ProgressCard, StatCard, WorkerTableWidget, LogTerminalWidget,
CityBrowserDialog, FailedCitiesDialog.
"""

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QProgressBar,
    QTableWidget, QTableWidgetItem, QTextEdit, QLineEdit,
    QPushButton, QDialog, QCheckBox, QHeaderView, QFrame,
    QTabWidget, QSizePolicy, QScrollArea,
    QGridLayout, QGroupBox, QSpinBox, QDialogButtonBox,
    QMessageBox, QApplication,
)
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QTextCursor, QColor, QFont, QAction

from gui_theme import COLORS, CityRecord, FailedCity


# ═══════════════════════════════════════════════
# STAT CARD — single metric display
# ═══════════════════════════════════════════════
class StatCard(QFrame):
    """A small card showing a label, value, and optional color accent."""

    def __init__(self, label="", value="0", color=None, parent=None):
        super().__init__(parent)
        self.setObjectName("card")
        self._color = color or COLORS["accent_green"]

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 8, 12, 8)
        layout.setSpacing(2)

        self._value_label = QLabel(str(value))
        self._value_label.setObjectName("bigTitleLabel")
        self._value_label.setStyleSheet(f"color: {self._color}; font-size: 24px; font-weight: bold;")
        self._value_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self._value_label)

        self._name_label = QLabel(label)
        self._name_label.setObjectName("dimLabel")
        self._name_label.setAlignment(Qt.AlignCenter)
        self._name_label.setStyleSheet(f"color: {COLORS['text_secondary']}; font-size: 11px;")
        layout.addWidget(self._name_label)

        self.setMinimumSize(100, 70)

    def set_value(self, value, color=None):
        self._value_label.setText(str(value))
        if color:
            self._value_label.setStyleSheet(f"color: {color}; font-size: 24px; font-weight: bold;")

    def set_label(self, label):
        self._name_label.setText(label)


# ═══════════════════════════════════════════════
# PROGRESS CARD — labeled progress bar with %
# ═══════════════════════════════════════════════
class ProgressCard(QFrame):
    """Card with a title, progress bar, and percentage label."""

    def __init__(self, title="Progress", parent=None):
        super().__init__(parent)
        self.setObjectName("card")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 8, 12, 8)
        layout.setSpacing(4)

        # Title row
        title_row = QHBoxLayout()
        self._title_label = QLabel(title)
        self._title_label.setObjectName("titleLabel")
        title_row.addWidget(self._title_label)
        title_row.addStretch()
        self._pct_label = QLabel("0%")
        self._pct_label.setObjectName("accentLabel")
        title_row.addWidget(self._pct_label)
        layout.addLayout(title_row)

        # Progress bar
        self._progress = QProgressBar()
        self._progress.setRange(0, 100)
        self._progress.setValue(0)
        self._progress.setTextVisible(False)
        layout.addWidget(self._progress)

        # Detail label
        self._detail_label = QLabel("0 / 0 tasks")
        self._detail_label.setObjectName("dimLabel")
        layout.addWidget(self._detail_label)

    def set_progress(self, current, total):
        if total <= 0:
            pct = 0
        else:
            pct = int(100 * current / total)
        self._progress.setValue(pct)
        self._pct_label.setText(f"{pct}%")
        self._detail_label.setText(f"{current} / {total} tasks")

    def set_title(self, title):
        self._title_label.setText(title)


# ═══════════════════════════════════════════════
# WORKER TABLE WIDGET
# ═══════════════════════════════════════════════
class WorkerTableWidget(QTableWidget):
    """Scrollable table showing worker status."""

    HEADERS = ["Worker ID", "PID", "Status", "City", "Country", "Month", "Tasks Done"]
    STATUS_COLORS = {
        "idle": COLORS["text_dim"],
        "downloading": COLORS["accent_green"],
        "converting": COLORS["accent_teal"],
        "resting": COLORS["resting"],
        "error": COLORS["error"],
        "stopped": COLORS["text_dim"],
    }

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setColumnCount(len(self.HEADERS))
        self.setHorizontalHeaderLabels(self.HEADERS)
        self.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.setAlternatingRowColors(True)
        self.setEditTriggers(QTableWidget.NoEditTriggers)
        self.setSelectionBehavior(QTableWidget.SelectRows)
        self.verticalHeader().setVisible(False)
        self.setMaximumHeight(250)
        self.setMinimumHeight(120)

    def update_workers(self, workers):
        """Update table with worker info dicts.
        workers: list of dicts with keys: worker_id, pid, status, city, country, month, tasks_done
        """
        self.setRowCount(len(workers))
        for row, w in enumerate(workers):
            status = w.get("status", "idle")
            color = self.STATUS_COLORS.get(status, COLORS["text_primary"])

            items = [
                str(w.get("worker_id", "-")),
                str(w.get("pid", "-")),
                status.upper(),
                str(w.get("city", "-")),
                str(w.get("country", "-")),
                str(w.get("month", "-")),
                str(w.get("tasks_done", "-")),
            ]
            for col, text in enumerate(items):
                item = QTableWidgetItem(text)
                item.setForeground(QColor(color if col == 2 else COLORS["text_primary"]))
                self.setItem(row, col, item)


# ═══════════════════════════════════════════════
# LOG TERMINAL WIDGET
# ═══════════════════════════════════════════════
class LogTerminalWidget(QWidget):
    """Terminal-style log viewer with filter tabs and search."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._filter_level = "ALL"
        self._filter_worker = "ALL"
        self._all_entries = []  # store all LogEntry-like dicts
        self._max_entries = 5000

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        # ── Toolbar ──
        toolbar = QHBoxLayout()

        # Filter tabs
        self._tabs = QTabWidget()
        self._tabs.setFixedHeight(32)
        self._tabs.setStyleSheet("QTabWidget::pane { border: none; }")

        # Create filter buttons instead of tabs for simplicity
        self._btn_all = QPushButton("All")
        self._btn_all.setFixedWidth(50)
        self._btn_all.setCheckable(True)
        self._btn_all.setChecked(True)
        self._btn_all.clicked.connect(lambda: self._set_level_filter("ALL"))

        self._btn_info = QPushButton("INFO")
        self._btn_info.setFixedWidth(55)
        self._btn_info.setCheckable(True)
        self._btn_info.clicked.connect(lambda: self._set_level_filter("INFO"))

        self._btn_warn = QPushButton("WARN")
        self._btn_warn.setFixedWidth(55)
        self._btn_warn.setCheckable(True)
        self._btn_warn.clicked.connect(lambda: self._set_level_filter("WARNING"))

        self._btn_error = QPushButton("ERROR")
        self._btn_error.setFixedWidth(55)
        self._btn_error.setCheckable(True)
        self._btn_error.clicked.connect(lambda: self._set_level_filter("ERROR"))

        for btn in [self._btn_all, self._btn_info, self._btn_warn, self._btn_error]:
            toolbar.addWidget(btn)

        toolbar.addStretch()

        # Search bar
        search_label = QLabel("🔍")
        toolbar.addWidget(search_label)
        self._search = QLineEdit()
        self._search.setPlaceholderText("Search logs...")
        self._search.setFixedWidth(200)
        self._search.textChanged.connect(self._apply_search)
        toolbar.addWidget(self._search)

        # Clear button
        btn_clear = QPushButton("Clear")
        btn_clear.setFixedWidth(60)
        btn_clear.clicked.connect(self.clear)
        toolbar.addWidget(btn_clear)

        layout.addLayout(toolbar)

        # ── Log text area ──
        self._text = QTextEdit()
        self._text.setReadOnly(True)
        self._text.setFont(QFont("Consolas", 10))
        layout.addWidget(self._text)

    def _set_level_filter(self, level):
        self._filter_level = level
        for btn, lvl in [(self._btn_all, "ALL"), (self._btn_info, "INFO"),
                         (self._btn_warn, "WARNING"), (self._btn_error, "ERROR")]:
            btn.setChecked(lvl == level)
        self._refresh_display()

    def _apply_search(self, text):
        self._refresh_display()

    def _refresh_display(self):
        """Re-render filtered log entries."""
        self._text.clear()
        search = self._search.text().lower()
        for entry in self._all_entries:
            if self._filter_level != "ALL" and entry["level"] != self._filter_level:
                continue
            if search and search not in entry["message"].lower():
                continue
            self._append_colored(entry)

    def _append_colored(self, entry):
        """Append a single colored log line to the text widget."""
        color_map = {
            "INFO": COLORS["success"],
            "WARNING": COLORS["warning"],
            "ERROR": COLORS["error"],
        }
        color = color_map.get(entry["level"], COLORS["text_secondary"])
        ts = entry.get("timestamp", "")
        wid = entry.get("worker_id", "")
        msg = entry.get("message", "")
        lvl = entry.get("level", "")

        line = f'<span style="color:{COLORS["text_dim"]}">[{ts}]</span> '
        line += f'<span style="color:{color}; font-weight:bold">[{lvl}]</span> '
        if wid:
            line += f'<span style="color:{COLORS["accent_teal"]}">[W{wid}]</span> '
        line += f'<span style="color:{COLORS["text_primary"]}">{msg}</span>'

        self._text.append(line)
        # Auto-scroll
        cursor = self._text.textCursor()
        cursor.movePosition(QTextCursor.End)
        self._text.setTextCursor(cursor)

    def append_log(self, entry):
        """Add a log entry dict with keys: timestamp, worker_id, level, message."""
        self._all_entries.append(entry)
        if len(self._all_entries) > self._max_entries:
            self._all_entries = self._all_entries[-self._max_entries:]

        # Only append if passes filter
        if self._filter_level != "ALL" and entry["level"] != self._filter_level:
            return
        search = self._search.text().lower()
        if search and search not in entry["message"].lower():
            return
        self._append_colored(entry)

    def clear(self):
        self._text.clear()
        self._all_entries.clear()


# ═══════════════════════════════════════════════
# CITY BROWSER DIALOG
# ═══════════════════════════════════════════════
class CityBrowserDialog(QDialog):
    """Full city list browser with search, filter, and multi-select."""

    cities_selected = Signal(list)  # list of CityRecord

    def __init__(self, cities=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("🌍 City Browser — Select Cities to Download")
        self.setMinimumSize(700, 500)
        self._cities = cities or []
        self._checkboxes = []

        layout = QVBoxLayout(self)

        # ── Search bar ──
        search_row = QHBoxLayout()
        search_label = QLabel("🔍 Search:")
        search_row.addWidget(search_label)
        self._search = QLineEdit()
        self._search.setPlaceholderText("Type city or country name...")
        self._search.textChanged.connect(self._filter_cities)
        search_row.addWidget(self._search)
        layout.addLayout(search_row)

        # ── Select all / none ──
        btn_row = QHBoxLayout()
        btn_select_all = QPushButton("Select All")
        btn_select_all.clicked.connect(self._select_all)
        btn_row.addWidget(btn_select_all)
        btn_select_none = QPushButton("Select None")
        btn_select_none.clicked.connect(self._select_none)
        btn_row.addWidget(btn_select_none)
        btn_row.addStretch()
        self._count_label = QLabel(f"Total: {len(self._cities)} cities")
        self._count_label.setObjectName("dimLabel")
        btn_row.addWidget(self._count_label)
        layout.addLayout(btn_row)

        # ── Scrollable city list ──
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        self._list_widget = QWidget()
        self._list_layout = QVBoxLayout(self._list_widget)
        self._list_layout.setSpacing(2)
        self._list_layout.setContentsMargins(4, 4, 4, 4)

        for city in self._cities:
            cb = QCheckBox(f"{city.city}, {city.country}  ({city.lat:.2f}, {city.lon:.2f})")
            cb.setProperty("city_record", city)
            self._checkboxes.append(cb)
            self._list_layout.addWidget(cb)

        self._list_layout.addStretch()
        scroll.setWidget(self._list_widget)
        layout.addWidget(scroll)

        # ── Year selector + Run button ──
        bottom_row = QHBoxLayout()
        year_label = QLabel("Year:")
        bottom_row.addWidget(year_label)
        self._year_spin = QSpinBox()
        self._year_spin.setRange(2020, 2030)
        self._year_spin.setValue(2025)
        bottom_row.addWidget(self._year_spin)

        bottom_row.addStretch()

        btn_run = QPushButton("▶ Run Selected Cities")
        btn_run.setObjectName("btnGreen")
        btn_run.clicked.connect(self._on_run)
        bottom_row.addWidget(btn_run)

        btn_close = QPushButton("Close")
        btn_close.clicked.connect(self.reject)
        bottom_row.addWidget(btn_close)

        layout.addLayout(bottom_row)

    def _filter_cities(self, text):
        text_lower = text.lower()
        for cb in self._checkboxes:
            city_record = cb.property("city_record")
            match = (text_lower in city_record.city.lower() or
                     text_lower in city_record.country.lower())
            cb.setVisible(match)

    def _select_all(self):
        for cb in self._checkboxes:
            if cb.isVisible():
                cb.setChecked(True)

    def _select_none(self):
        for cb in self._checkboxes:
            cb.setChecked(False)

    def _on_run(self):
        selected = []
        for cb in self._checkboxes:
            if cb.isChecked():
                selected.append(cb.property("city_record"))
        if not selected:
            QMessageBox.warning(self, "No Selection", "Please select at least one city.")
            return
        self.cities_selected.emit(selected)
        self.accept()

    def get_selected_cities(self):
        return [cb.property("city_record") for cb in self._checkboxes if cb.isChecked()]

    def get_year(self):
        return self._year_spin.value()


# ═══════════════════════════════════════════════
# FAILED CITIES DIALOG
# ═══════════════════════════════════════════════
class FailedCitiesDialog(QDialog):
    """Popup showing failed cities with Retry All / Retry Selected buttons."""

    retry_cities = Signal(list)  # list of FailedCity

    def __init__(self, failed_cities=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("⚠ Failed Cities — Missing Data")
        self.setMinimumSize(600, 400)
        self._failed = failed_cities or []
        self._checkboxes = []

        layout = QVBoxLayout(self)

        # Title
        title = QLabel(f"⚠ {len(self._failed)} cities have missing month data")
        title.setObjectName("titleLabel")
        title.setStyleSheet(f"color: {COLORS['warning']}; font-size: 16px;")
        layout.addWidget(title)

        # Scrollable list
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        list_widget = QWidget()
        list_layout = QVBoxLayout(list_widget)
        list_layout.setSpacing(2)
        list_layout.setContentsMargins(4, 4, 4, 4)

        for fc in self._failed:
            months_str = ", ".join(str(m) for m in fc.missing_months)
            cb = QCheckBox(f"{fc.city}, {fc.country} — Missing: {months_str}")
            cb.setChecked(True)
            cb.setProperty("failed_city", fc)
            self._checkboxes.append(cb)
            list_layout.addWidget(cb)

        list_layout.addStretch()
        scroll.setWidget(list_widget)
        layout.addWidget(scroll)

        # Buttons
        btn_row = QHBoxLayout()

        btn_select_all = QPushButton("Select All")
        btn_select_all.clicked.connect(lambda: [cb.setChecked(True) for cb in self._checkboxes])
        btn_row.addWidget(btn_select_all)

        btn_select_none = QPushButton("Select None")
        btn_select_none.clicked.connect(lambda: [cb.setChecked(False) for cb in self._checkboxes])
        btn_row.addWidget(btn_select_none)

        btn_row.addStretch()

        btn_retry_all = QPushButton("🔄 Retry All")
        btn_retry_all.setObjectName("btnGreen")
        btn_retry_all.clicked.connect(self._retry_all)
        btn_row.addWidget(btn_retry_all)

        btn_retry_selected = QPushButton("▶ Retry Selected")
        btn_retry_selected.setObjectName("btnAmber")
        btn_retry_selected.clicked.connect(self._retry_selected)
        btn_row.addWidget(btn_retry_selected)

        btn_close = QPushButton("Close")
        btn_close.clicked.connect(self.reject)
        btn_row.addWidget(btn_close)

        layout.addLayout(btn_row)

    def _retry_all(self):
        self.retry_cities.emit(self._failed)
        self.accept()

    def _retry_selected(self):
        selected = [cb.property("failed_city") for cb in self._checkboxes if cb.isChecked()]
        if not selected:
            QMessageBox.warning(self, "No Selection", "Please select at least one city to retry.")
            return
        self.retry_cities.emit(selected)
        self.accept()

    def get_selected_cities(self):
        return [cb.property("failed_city") for cb in self._checkboxes if cb.isChecked()]