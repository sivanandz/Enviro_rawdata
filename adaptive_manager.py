import subprocess
import sys
import time
import os
import sqlite3

# ==========================================
# CONFIGURATION
# ==========================================
MAX_WORKERS = 10 # Hard cap
INITIAL_WORKERS = 2
COOLDOWN_SECONDS = 300 # 5 minutes cooldown after rejection
SCALE_UP_INTERVAL = 300 # Try scaling up every 5 minutes of stability
script_name = "adaptive_worker.py"
DB_FILE = "tasks.db"

def get_pending_count():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM tasks WHERE status = 'pending'")
        count = c.fetchone()[0]
        conn.close()
        return count
    except:
        return 0

def main():
    target_workers = INITIAL_WORKERS
    processes = {} # pid -> Popen object
    
    last_rejection_time = 0
    last_scale_up_time = time.time()
    
    print(f"=== Adaptive Manager Started ===")
    print(f"Initial target: {target_workers} workers")
    
    try:
        while True:
            current_time = time.time()
            pending_tasks = get_pending_count()
            
            if pending_tasks == 0 and len(processes) == 0:
                print("No pending tasks. System idle.")
                break # Or wait? Let's stay alive in case tasks are added.
            
            # 1. CLEANUP DEAD WORKERS
            dead_pids = []
            for pid, p in processes.items():
                if p.poll() is not None:
                    dead_pids.append(pid)
                    # Check exit code
                    exit_code = p.returncode
                    if exit_code == 42:
                        print(f"Worker {pid} exited with RATE LIMIT (42).")
                        # TRIGGER BACKOFF
                        last_rejection_time = current_time
                        if target_workers > 1:
                            target_workers -= 1
                            print(f" >>> SCALING DOWN to {target_workers} workers.")
                        else:
                            print(" >>> Already at minimum workers. Pausing launches.")
                    elif exit_code != 0:
                        print(f"Worker {pid} exited with error code {exit_code}.")
            
            for pid in dead_pids:
                del processes[pid]
                
            # 2. CHECK SCALING RULES
            
            # Can we scale up? 
            # If no rejection in COOLDOWN_SECONDS AND we haven't scaled up recently
            time_since_rejection = current_time - last_rejection_time
            time_since_scale_up = current_time - last_scale_up_time
            
            if (time_since_rejection > COOLDOWN_SECONDS and 
                time_since_scale_up > SCALE_UP_INTERVAL and 
                target_workers < MAX_WORKERS):
                
                target_workers += 1
                last_scale_up_time = current_time
                print(f" >>> Stability detected. SCALING UP to {target_workers} workers.")
                
            # If we are in cooldown (rejection recently), force target down if needed?
            # Actually, the exit code handling already reduced target_workers.
            # We just need to make sure we don't launch if in deep cooldown/pause?
            # Actually, launching 1 worker is fine even in cooldown, as long as we respect target.
            
            # 3. LAUNCH WORKERS (Up to Target)
            active_count = len(processes)
            if active_count < target_workers and pending_tasks > 0:
                # If we just had a rejection < 60s ago, maybe wait a bit before even launching 1?
                # The worker slept 60s inside? No, it exited.
                # Let's enforce a hard pause if rejection was < 60s ago
                if time_since_rejection < 60:
                    pass # Don't launch anything immediately after rejection
                else:
                    needed = target_workers - active_count
                    for _ in range(needed):
                        # Generate a simple worker ID (pid serves as unique enough, or random)
                        wid = int(time.time() * 1000) % 10000 
                        cmd = [sys.executable, script_name, "--worker-id", str(wid)]
                        p = subprocess.Popen(cmd)
                        processes[p.pid] = p
                        print(f"Launched Worker {wid} (PID: {p.pid}). Total Active: {len(processes)}")
                        time.sleep(1) # Stagger
        
            print(f"Status: {len(processes)}/{target_workers} Workers | Tasks Pending: {pending_tasks}", end="\r")
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\nStopping Manager...")
        for p in processes.values():
            p.terminate()

if __name__ == "__main__":
    main()
