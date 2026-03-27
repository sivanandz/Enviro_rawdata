"""
ERA5 Mission Control — Background Thread Workers
ManagerThread, BatchManagerThread, SingleCityThread, LogWatcherThread.
"""

import subprocess
import sys
import os
import time
import glob
import re
from datetime import datetime

from PySide6.QtCore import QThread, Signal

import gui_db


# ═══════════════════════════════════════════════
# MANAGER THREAD — Wraps adaptive_manager.py
# ═══════════════════════════════════════════════
class ManagerThread(QThread):
    """Launches adaptive_manager.py as a subprocess and monitors it."""

    log_entry = Signal(dict)          # {timestamp, worker_id, level, message}
    status_update = Signal(dict)      # {active, target, pending}
    worker_launched = Signal(int)     # worker pid
    worker_died = Signal(int)         # worker pid
    finished_signal = Signal(str)     # "done" or error message

    def __init__(self, parent=None):
        super().__init__(parent)
        self._process = None
        self._running = False

    def run(self):
        self._running = True
        cmd = [sys.executable, "adaptive_manager.py"]
        self.log_entry.emit({
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            "worker_id": 0,
            "level": "INFO",
            "message": "Starting Adaptive Manager..."
        })

        try:
            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=os.getcwd(),
            )

            # Stream output
            for line in self._process.stdout:
                if not self._running:
                    break
                line = line.strip()
                if line:
                    self._parse_and_emit(line)

            self._process.wait()

        except Exception as e:
            self.log_entry.emit({
                "timestamp": datetime.now().strftime("%H:%M:%S"),
                "worker_id": 0,
                "level": "ERROR",
                "message": f"Manager error: {e}"
            })
        finally:
            self.finished_signal.emit("done")
            self._running = False

    def _parse_and_emit(self, line):
        """Parse adaptive_manager output line and emit signals."""
        ts = datetime.now().strftime("%H:%M:%S")
        level = "INFO"

        # Detect level from line content
        if "SCALING DOWN" in line or "RATE LIMIT" in line:
            level = "WARNING"
        elif "error" in line.lower():
            level = "ERROR"

        # Try to detect worker pid from launch messages
        worker_id = 0
        match = re.search(r"Worker (\d+)", line)
        if match:
            worker_id = int(match.group(1))

        self.log_entry.emit({
            "timestamp": ts,
            "worker_id": worker_id,
            "level": level,
            "message": line
        })

        # Emit status update for key lines
        if "Workers" in line and "Tasks Pending" in line:
            self.status_update.emit({"raw": line})

    def stop(self):
        """Gracefully stop the manager and its workers."""
        self._running = False
        if self._process and self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()


# ═══════════════════════════════════════════════
# BATCH MANAGER THREAD — Wraps run_all_workers.py
# ═══════════════════════════════════════════════
class BatchManagerThread(QThread):
    """Launches run_all_workers.py logic: 100 workers in batches of configurable size."""

    log_entry = Signal(dict)
    batch_started = Signal(int, int)   # batch_start, batch_end
    batch_completed = Signal(int, int)
    all_done = Signal()
    progress_update = Signal(dict)     # {batch, total_batches, active}

    def __init__(self, year=2025, batch_size=5, num_workers=100, parent=None):
        super().__init__(parent)
        self._year = year
        self._batch_size = batch_size
        self._num_workers = num_workers
        self._running = False
        self._processes = []

    def run(self):
        self._running = True
        os.makedirs("logs", exist_ok=True)

        total_batches = (self._num_workers + self._batch_size - 1) // self._batch_size

        self.log_entry.emit({
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            "worker_id": 0,
            "level": "INFO",
            "message": f"Starting Batch Mode: {self._num_workers} workers, batch size {self._batch_size}"
        })

        try:
            for batch_start in range(1, self._num_workers + 1, self._batch_size):
                if not self._running:
                    break

                batch_end = min(batch_start + self._batch_size - 1, self._num_workers)
                self.batch_started.emit(batch_start, batch_end)

                self.log_entry.emit({
                    "timestamp": datetime.now().strftime("%H:%M:%S"),
                    "worker_id": 0,
                    "level": "INFO",
                    "message": f"Starting batch {batch_start} to {batch_end}"
                })

                # Launch batch
                batch_procs = []
                for i in range(batch_start, batch_end + 1):
                    if not self._running:
                        break
                    cmd = [
                        sys.executable, "worker_main.py",
                        "--worker-id", str(i),
                        "--year", str(self._year),
                    ]
                    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                    batch_procs.append((i, p))
                    self._processes.append((i, p))
                    time.sleep(2)  # Stagger

                # Wait for batch to complete
                while self._running:
                    active = sum(1 for _, p in batch_procs if p.poll() is None)
                    if active == 0:
                        break
                    batch_num = (batch_start - 1) // self._batch_size + 1
                    self.progress_update.emit({
                        "batch": batch_num,
                        "total_batches": total_batches,
                        "active": active,
                    })
                    time.sleep(5)

                self.batch_completed.emit(batch_start, batch_end)

            if self._running:
                self.all_done.emit()
                self.log_entry.emit({
                    "timestamp": datetime.now().strftime("%H:%M:%S"),
                    "worker_id": 0,
                    "level": "INFO",
                    "message": "All batches completed!"
                })

        except Exception as e:
            self.log_entry.emit({
                "timestamp": datetime.now().strftime("%H:%M:%S"),
                "worker_id": 0,
                "level": "ERROR",
                "message": f"Batch error: {e}"
            })
        finally:
            self._running = False

    def stop(self):
        """Stop all running worker processes."""
        self._running = False
        for wid, p in self._processes:
            if p.poll() is None:
                p.terminate()
        # Give them a moment then kill
        time.sleep(2)
        for wid, p in self._processes:
            if p.poll() is None:
                try:
                    p.kill()
                except Exception:
                    pass


# ═══════════════════════════════════════════════
# SINGLE CITY THREAD — Downloads data for one city
# ═══════════════════════════════════════════════
class SingleCityThread(QThread):
    """Downloads data for specific cities using adaptive_worker.py logic."""

    download_complete = Signal(str)     # city name
    download_error = Signal(str, str)   # city name, error
    progress = Signal(dict)             # {city, month, status}
    log_entry = Signal(dict)

    def __init__(self, cities, year, parent=None):
        """cities: list of CityRecord dataclasses. year: int."""
        super().__init__(parent)
        self._cities = cities
        self._year = year
        self._running = False

    def run(self):
        self._running = True

        for city_rec in self._cities:
            if not self._running:
                break

            city = city_rec.city
            country = city_rec.country
            lat = city_rec.lat
            lon = city_rec.lon

            self.log_entry.emit({
                "timestamp": datetime.now().strftime("%H:%M:%S"),
                "worker_id": 0,
                "level": "INFO",
                "message": f"Starting download for {city}, {country} ({self._year})"
            })

            # Reset tasks for this city
            gui_db.reset_tasks_for_city(city_rec.id, self._year)

            # Launch an adaptive worker that will pick up this city's tasks
            worker_id = int(time.time() * 1000) % 10000
            cmd = [
                sys.executable, "adaptive_worker.py",
                "--worker-id", str(worker_id),
            ]

            try:
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                )

                # Stream output for this city (with timeout)
                start_time = time.time()
                timeout = 600  # 10 min per city

                while self._running and proc.poll() is None:
                    elapsed = time.time() - start_time
                    if elapsed > timeout:
                        proc.terminate()
                        self.log_entry.emit({
                            "timestamp": datetime.now().strftime("%H:%M:%S"),
                            "worker_id": worker_id,
                            "level": "ERROR",
                            "message": f"Timeout downloading {city}"
                        })
                        break

                    try:
                        line = proc.stdout.readline()
                        if line:
                            line = line.strip()
                            if line:
                                self.log_entry.emit({
                                    "timestamp": datetime.now().strftime("%H:%M:%S"),
                                    "worker_id": worker_id,
                                    "level": "INFO",
                                    "message": line
                                })
                    except Exception:
                        pass
                    time.sleep(0.1)

                if proc.poll() is None:
                    proc.terminate()
                    proc.wait(timeout=5)

                # Check if completed
                counts = gui_db.get_task_counts(self._year)
                self.download_complete.emit(city)

            except Exception as e:
                self.download_error.emit(city, str(e))
                self.log_entry.emit({
                    "timestamp": datetime.now().strftime("%H:%M:%S"),
                    "worker_id": 0,
                    "level": "ERROR",
                    "message": f"Error downloading {city}: {e}"
                })

    def stop(self):
        self._running = False


# ═══════════════════════════════════════════════
# LOG WATCHER THREAD — Tails log files
# ═══════════════════════════════════════════════
class LogWatcherThread(QThread):
    """Watches logs/ directory for new entries, emits parsed log signals."""

    new_log_entry = Signal(dict)  # {timestamp, worker_id, level, message}

    def __init__(self, parent=None):
        super().__init__(parent)
        self._running = False
        self._file_positions = {}  # filepath -> last position
        self._poll_interval = 1.0  # seconds

    def run(self):
        self._running = True
        os.makedirs("logs", exist_ok=True)

        while self._running:
            log_files = glob.glob(os.path.join("logs", "*.log"))

            for filepath in log_files:
                try:
                    self._tail_file(filepath)
                except Exception:
                    pass

            time.sleep(self._poll_interval)

    def _tail_file(self, filepath):
        """Read new lines from a log file starting from last position."""
        pos = self._file_positions.get(filepath, 0)

        try:
            file_size = os.path.getsize(filepath)
            if file_size < pos:
                # File was truncated/rotated, start from beginning
                pos = 0

            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                f.seek(pos)
                for line in f:
                    line = line.strip()
                    if line:
                        entry = self._parse_log_line(line)
                        if entry:
                            self.new_log_entry.emit(entry)
                self._file_positions[filepath] = f.tell()

        except FileNotFoundError:
            pass
        except Exception:
            pass

    def _parse_log_line(self, line):
        """Parse a log line into a structured dict.

        Expected formats:
        [Worker 123] 2024-01-01 12:00:00 - INFO - Message here
        2024-01-01 12:00:00 - INFO - Message here
        """
        entry = {
            "timestamp": "",
            "worker_id": 0,
            "level": "INFO",
            "message": line,
        }

        # Try to extract worker ID
        wid_match = re.search(r'\[Worker\s+(\d+)\]', line)
        if wid_match:
            entry["worker_id"] = int(wid_match.group(1))

        # Try to extract timestamp
        ts_match = re.search(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', line)
        if ts_match:
            full_ts = ts_match.group(1)
            # Extract just time for display
            time_part = full_ts.split(" ")[1] if " " in full_ts else full_ts
            entry["timestamp"] = time_part

        # Try to extract level
        for lvl in ["INFO", "WARNING", "ERROR", "CRITICAL", "DEBUG"]:
            if f" - {lvl} - " in line:
                entry["level"] = lvl
                break

        # Extract message part (after last " - ")
        parts = line.split(" - ")
        if len(parts) >= 3:
            entry["message"] = " - ".join(parts[2:])

        return entry

    def stop(self):
        self._running = False