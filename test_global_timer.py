import time
import subprocess
import sys
import sqlite3
from timer_utils import GlobalTimer
from db_config import DB_FILE

# Use accelerated timing: 30s work, 10s rest for testing
WORK_TIME = 30
REST_TIME = 10

def reset_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE system_state SET phase = 'WORKING', last_transition_time = ?, work_accumulated = 0 WHERE id = 1", (time.time(),))
    conn.commit()
    conn.close()

def main():
    print("=== Accelerated Timer Test ===")
    
    # Initialize Global Timer first to ensure table exists
    timer = GlobalTimer(work_duration=WORK_TIME, rest_duration=REST_TIME)
    
    reset_db()
    
    # Start a dummy worker process that just waits
    worker_cmd = [sys.executable, "-c", "import time; from timer_utils import GlobalTimer; t=GlobalTimer(30,10); print('Worker started'); t.check_wait(999); print('Worker resumed!')"]
    
    start_time = time.time()
    
    # Simulate Manager loop
    while True:
        elapsed = time.time() - start_time
        state = timer.get_state()
        
        if state["phase"] == "WORKING":
            timer.update_work_accumulated(1) # Simulating 1s of work
            print(f"\r{timer.get_summary()}", end="")
        else:
            print(f"\r{timer.get_summary()}", end="")
            if timer.check_rest_timer():
                print("\nTest Passed: Transitioned back to WORKING.")
                break
        
        # At 35s mark, if we are resting, start a worker and see if it waits
        if 31 < (time.time() - start_time) < 33 and state["phase"] == "RESTING":
             print("\nLaunching test worker during REST phase...")
             p = subprocess.Popen(worker_cmd)
             # Wait for worker to finish or timeout
             try:
                 p.wait(timeout=15)
                 print("Worker process finished.")
             except subprocess.TimeoutExpired:
                 print("Worker timed out (as expected, it should wait for rest to end).")
                 p.terminate()

        time.sleep(1)

if __name__ == "__main__":
    main()
