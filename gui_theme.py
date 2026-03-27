"""
ERA5 Mission Control — Dark Green Theme
Color palette, QSS stylesheets, and enum/dataclass definitions.
"""

from enum import Enum
from dataclasses import dataclass

# ──────────────────────────────────────────────
# COLOR PALETTE  (Dark Mission Control – Green)
# ──────────────────────────────────────────────
COLORS = {
    "bg_dark":        "#0d1b0e",
    "bg_primary":     "#1a2e1a",
    "bg_card":        "#1e3a1e",
    "bg_input":       "#253d25",
    "border":         "#2d5a2d",
    "accent_green":   "#00e676",
    "accent_teal":    "#00bfa5",
    "accent_lime":    "#76ff03",
    "text_primary":   "#e8f5e9",
    "text_secondary": "#a5d6a7",
    "text_dim":       "#66bb6a",
    "success":        "#69f0ae",
    "warning":        "#ffd740",
    "error":          "#ff5252",
    "progress_bg":    "#1b5e20",
    "progress_fill":  "#00e676",
    "resting":        "#ffd740",
}

# ──────────────────────────────────────────────
# ENUMS
# ──────────────────────────────────────────────
class WorkerStatus(Enum):
    IDLE = "idle"
    DOWNLOADING = "downloading"
    CONVERTING = "converting"
    RESTING = "resting"
    ERROR = "error"
    STOPPED = "stopped"


class TaskStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    ERROR = "error"


# ──────────────────────────────────────────────
# DATACLASSES
# ──────────────────────────────────────────────
@dataclass
class WorkerInfo:
    worker_id: int
    pid: int
    status: WorkerStatus
    current_city: str
    country: str
    start_time: float
    tasks_completed: int


@dataclass
class LogEntry:
    timestamp: str
    worker_id: int
    level: str       # INFO, WARNING, ERROR
    message: str


@dataclass
class CityRecord:
    id: int
    city: str
    country: str
    lat: float
    lon: float


@dataclass
class FailedCity:
    city: str
    country: str
    missing_months: list


# ──────────────────────────────────────────────
# QSS STYLESHEET
# ──────────────────────────────────────────────
STYLESHEET = f"""
/* ── Global ── */
QWidget {{
    background-color: {COLORS["bg_primary"]};
    color: {COLORS["text_primary"]};
    font-family: "Segoe UI", "Consolas", monospace;
    font-size: 13px;
}}

QMainWindow {{
    background-color: {COLORS["bg_dark"]};
}}

/* ── Cards / Panels ── */
QFrame#card {{
    background-color: {COLORS["bg_card"]};
    border: 1px solid {COLORS["border"]};
    border-radius: 8px;
    padding: 8px;
}}

/* ── Buttons ── */
QPushButton {{
    background-color: {COLORS["bg_card"]};
    border: 1px solid {COLORS["border"]};
    border-radius: 6px;
    padding: 8px 18px;
    color: {COLORS["text_primary"]};
    font-weight: bold;
    min-height: 18px;
}}
QPushButton:hover {{
    background-color: {COLORS["border"]};
    border-color: {COLORS["accent_green"]};
}}
QPushButton:pressed {{
    background-color: {COLORS["accent_green"]};
    color: {COLORS["bg_dark"]};
}}
QPushButton:disabled {{
    background-color: {COLORS["bg_input"]};
    color: {COLORS["text_dim"]};
    border-color: {COLORS["bg_input"]};
}}

/* Accent buttons */
QPushButton#btnGreen {{
    background-color: #1b5e20;
    border-color: {COLORS["accent_green"]};
    color: {COLORS["accent_green"]};
}}
QPushButton#btnGreen:hover {{
    background-color: {COLORS["accent_green"]};
    color: {COLORS["bg_dark"]};
}}

QPushButton#btnRed {{
    background-color: #4a1010;
    border-color: {COLORS["error"]};
    color: {COLORS["error"]};
}}
QPushButton#btnRed:hover {{
    background-color: {COLORS["error"]};
    color: white;
}}

QPushButton#btnAmber {{
    background-color: #4a3a00;
    border-color: {COLORS["warning"]};
    color: {COLORS["warning"]};
}}
QPushButton#btnAmber:hover {{
    background-color: {COLORS["warning"]};
    color: {COLORS["bg_dark"]};
}}

/* ── Line Edit ── */
QLineEdit {{
    background-color: {COLORS["bg_input"]};
    border: 1px solid {COLORS["border"]};
    border-radius: 4px;
    padding: 6px 10px;
    color: {COLORS["text_primary"]};
    selection-background-color: {COLORS["accent_green"]};
}}
QLineEdit:focus {{
    border-color: {COLORS["accent_green"]};
}}

/* ── Spin Box ── */
QSpinBox {{
    background-color: {COLORS["bg_input"]};
    border: 1px solid {COLORS["border"]};
    border-radius: 4px;
    padding: 4px 8px;
    color: {COLORS["text_primary"]};
    min-width: 70px;
}}
QSpinBox:focus {{
    border-color: {COLORS["accent_green"]};
}}

/* ── Progress Bar ── */
QProgressBar {{
    background-color: {COLORS["progress_bg"]};
    border: 1px solid {COLORS["border"]};
    border-radius: 6px;
    text-align: center;
    color: {COLORS["text_primary"]};
    font-weight: bold;
    min-height: 22px;
    max-height: 28px;
}}
QProgressBar::chunk {{
    background-color: {COLORS["progress_fill"]};
    border-radius: 5px;
}}

/* ── Table ── */
QTableWidget {{
    background-color: {COLORS["bg_card"]};
    border: 1px solid {COLORS["border"]};
    gridline-color: {COLORS["border"]};
    color: {COLORS["text_primary"]};
    selection-background-color: {COLORS["border"]};
    alternate-background-color: {COLORS["bg_input"]};
}}
QTableWidget::item {{
    padding: 4px;
}}
QHeaderView::section {{
    background-color: {COLORS["bg_input"]};
    color: {COLORS["accent_green"]};
    border: 1px solid {COLORS["border"]};
    padding: 6px;
    font-weight: bold;
}}

/* ── Tab Bar ── */
QTabWidget::pane {{
    border: 1px solid {COLORS["border"]};
    background-color: {COLORS["bg_card"]};
}}
QTabBar::tab {{
    background-color: {COLORS["bg_input"]};
    border: 1px solid {COLORS["border"]};
    padding: 8px 16px;
    color: {COLORS["text_secondary"]};
    border-top-left-radius: 6px;
    border-top-right-radius: 6px;
    margin-right: 2px;
}}
QTabBar::tab:selected {{
    background-color: {COLORS["bg_card"]};
    color: {COLORS["accent_green"]};
    border-bottom-color: {COLORS["bg_card"]};
}}
QTabBar::tab:hover {{
    color: {COLORS["accent_lime"]};
}}

/* ── Text Edit (Log Terminal) ── */
QTextEdit {{
    background-color: {COLORS["bg_dark"]};
    color: {COLORS["text_secondary"]};
    border: 1px solid {COLORS["border"]};
    border-radius: 4px;
    font-family: "Consolas", "Courier New", monospace;
    font-size: 12px;
    padding: 4px;
    selection-background-color: {COLORS["accent_green"]};
}}

/* ── Scroll Bar ── */
QScrollBar:vertical {{
    background-color: {COLORS["bg_dark"]};
    width: 12px;
    border: none;
}}
QScrollBar::handle:vertical {{
    background-color: {COLORS["border"]};
    border-radius: 6px;
    min-height: 20px;
}}
QScrollBar::handle:vertical:hover {{
    background-color: {COLORS["accent_green"]};
}}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
    height: 0px;
}}
QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {{
    background: none;
}}

/* ── Label ── */
QLabel {{
    color: {COLORS["text_primary"]};
    background: transparent;
    border: none;
}}
QLabel#dimLabel {{
    color: {COLORS["text_dim"]};
}}
QLabel#accentLabel {{
    color: {COLORS["accent_green"]};
    font-weight: bold;
}}
QLabel#titleLabel {{
    color: {COLORS["accent_green"]};
    font-size: 16px;
    font-weight: bold;
}}
QLabel#bigTitleLabel {{
    color: {COLORS["accent_green"]};
    font-size: 20px;
    font-weight: bold;
}}

/* ── Combo Box ── */
QComboBox {{
    background-color: {COLORS["bg_input"]};
    border: 1px solid {COLORS["border"]};
    border-radius: 4px;
    padding: 6px 12px;
    color: {COLORS["text_primary"]};
    min-width: 80px;
}}
QComboBox:hover {{
    border-color: {COLORS["accent_green"]};
}}
QComboBox::drop-down {{
    border: none;
    width: 24px;
}}
QComboBox QAbstractItemView {{
    background-color: {COLORS["bg_card"]};
    color: {COLORS["text_primary"]};
    selection-background-color: {COLORS["border"]};
}}

/* ── Menu Bar ── */
QMenuBar {{
    background-color: {COLORS["bg_dark"]};
    color: {COLORS["text_secondary"]};
    border-bottom: 1px solid {COLORS["border"]};
}}
QMenuBar::item:selected {{
    background-color: {COLORS["bg_card"]};
    color: {COLORS["accent_green"]};
}}
QMenu {{
    background-color: {COLORS["bg_card"]};
    color: {COLORS["text_primary"]};
    border: 1px solid {COLORS["border"]};
}}
QMenu::item:selected {{
    background-color: {COLORS["border"]};
    color: {COLORS["accent_green"]};
}}

/* ── Dialog ── */
QDialog {{
    background-color: {COLORS["bg_primary"]};
}}

/* ── Group Box ── */
QGroupBox {{
    border: 1px solid {COLORS["border"]};
    border-radius: 6px;
    margin-top: 10px;
    padding-top: 14px;
    color: {COLORS["accent_green"]};
    font-weight: bold;
}}
QGroupBox::title {{
    subcontrol-origin: margin;
    left: 12px;
    padding: 0 6px;
}}

/* ── Check Box ── */
QCheckBox {{
    color: {COLORS["text_primary"]};
    spacing: 8px;
}}
QCheckBox::indicator {{
    width: 16px;
    height: 16px;
    border: 1px solid {COLORS["border"]};
    border-radius: 3px;
    background-color: {COLORS["bg_input"]};
}}
QCheckBox::indicator:checked {{
    background-color: {COLORS["accent_green"]};
    border-color: {COLORS["accent_green"]};
}}

/* ── Splitter ── */
QSplitter::handle {{
    background-color: {COLORS["border"]};
    height: 2px;
}}
"""

# ──────────────────────────────────────────────
# HELPER — apply theme to a QApplication
# ──────────────────────────────────────────────
def apply_theme(app):
    """Apply the Dark Mission Control Green theme to the QApplication."""
    app.setStyleSheet(STYLESHEET)