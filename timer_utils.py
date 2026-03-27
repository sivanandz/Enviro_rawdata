import sqlite3
import time
import os
from datetime import datetime, timedelta

from db_config import DB_FILE

class GlobalTimer:
    def __init__(self, work_duration=4*3600, rest_duration=1*3600):
        self.work_duration = work_duration
        self.rest_duration = rest_duration
        self._ensure_table()

    def _ensure_table(self):
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS system_state (
                id INTEGER PRIMARY KEY DEFAULT 1,
                phase TEXT DEFAULT 'WORKING',
                last_transition_time REAL,
                work_accumulated REAL DEFAULT 0,
                CHECK (id = 1)
            )
        """)
        # Initialize if empty
        c.execute("SELECT id FROM system_state WHERE id = 1")
        if not c.fetchone():
            c.execute("INSERT INTO system_state (id, phase, last_transition_time, work_accumulated) VALUES (1, 'WORKING', ?, 0)", (time.time(),))
        conn.commit()
        conn.close()

    def get_state(self):
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT phase, last_transition_time, work_accumulated FROM system_state WHERE id = 1")
        row = c.fetchone()
        conn.close()
        if row:
            return {
                "phase": row[0],
                "last_transition_time": row[1],
                "work_accumulated": row[2]
            }
        return None

    def update_work_accumulated(self, seconds):
        """Called by the Manager or some primary process to increment global work time."""
        if seconds <= 0: return
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Check current state first
        c.execute("SELECT phase, work_accumulated FROM system_state WHERE id = 1")
        phase, acc = c.fetchone()
        
        if phase == 'WORKING':
            new_acc = acc + seconds
            if new_acc >= self.work_duration:
                print(f"\n[TIMER] Work limit reached ({new_acc/3600:.2f}h). Switching to RESTING phase.")
                c.execute("UPDATE system_state SET phase = 'RESTING', last_transition_time = ?, work_accumulated = 0 WHERE id = 1", (time.time(),))
            else:
                c.execute("UPDATE system_state SET work_accumulated = ? WHERE id = 1", (new_acc,))
        
        conn.commit()
        conn.close()

    def tick(self, seconds):
        """Syncs work time and checks for rest transitions. Replaces check_rest_timer for simplicity."""
        state = self.get_state()
        if state["phase"] == "WORKING":
            self.update_work_accumulated(seconds)
        else:
            # Check if rest is over
            elapsed = time.time() - state["last_transition_time"]
            if elapsed >= self.rest_duration:
                # print(f"\n[TIMER] Rest period over. Switching to WORKING phase.") # Handled by Manager usually, but let's be safe
                conn = sqlite3.connect(DB_FILE)
                c = conn.cursor()
                c.execute("UPDATE system_state SET phase = 'WORKING', last_transition_time = ?, work_accumulated = 0 WHERE id = 1", (time.time(),))
                conn.commit()
                conn.close()

    def check_wait(self, worker_id, logger=None):
        """Called by workers before each task. Blocks if RESTING."""
        first_wait = True
        while True:
            state = self.get_state()
            if state["phase"] == "WORKING":
                if not first_wait:
                    print(f"\n[Worker {worker_id}] Work Resumed!")
                return

            first_wait = False
            # Calc remaining rest time
            elapsed = time.time() - state["last_transition_time"]
            remaining = max(0, self.rest_duration - elapsed)
            
            msg = f"RESTING PHASE ACTIVE. Resume in: {self.format_seconds(remaining)}"
            if logger:
                # Log only occasionally to avoid bloat
                if int(remaining) % 60 == 0:
                    logger.info(msg)
            
            # Print to console (overwriting line)
            print(f"\r[Worker {worker_id}] {msg}          ", end="", flush=True)
            
            if remaining <= 0:
                break
                
            time.sleep(5)

    @staticmethod
    def format_seconds(seconds):
        td = timedelta(seconds=int(seconds))
        return str(td)

    def get_summary(self):
        state = self.get_state()
        if not state: return "Timer Error"
        
        if state["phase"] == "WORKING":
            rem = max(0, self.work_duration - state["work_accumulated"])
            return f"PHASE: WORKING | Time to Rest: {self.format_seconds(rem)} | Progress: {state['work_accumulated']/3600:.2f}/{self.work_duration/3600:.2f}h"
        else:
            elapsed = time.time() - state["last_transition_time"]
            rem = max(0, self.rest_duration - elapsed)
            return f"PHASE: RESTING | Resume in: {self.format_seconds(rem)}"

    def get_short_status(self):
        state = self.get_state()
        if not state: return "Timer Error"
        if state["phase"] == "WORKING":
            rem = max(0, self.work_duration - state["work_accumulated"])
            return f"Work Rem: {self.format_seconds(rem)}"
        else:
            elapsed = time.time() - state["last_transition_time"]
            rem = max(0, self.rest_duration - elapsed)
            return f"Resting: {self.format_seconds(rem)}"
