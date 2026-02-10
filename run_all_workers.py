import subprocess
import sys
import time
import os

def main():
    num_workers = 2 # Reduced to 2 for stability
    processes = []
    
    print(f"Starting {num_workers} parallel workers...")
    
    # Ensure logs directory exists so workers don't race to create it
    os.makedirs("logs", exist_ok=True)
    os.makedirs("era5_data_2025", exist_ok=True)

    for i in range(1, num_workers + 1):
        cmd = [sys.executable, "worker_main.py", "--worker-id", str(i)]
        
        # We start them using Popen so they run in parallel
        # We redirect stdout/stderr to header so we can see some output, 
        # but mostly they log to files.
        p = subprocess.Popen(cmd)
        processes.append((i, p))
        print(f"Launched Worker {i} (PID: {p.pid})")
        
        # Stagger starts slightly to avoid hammering the API all at exact same millisecond
        time.sleep(2) 

    print("\nAll workers launched. Monitoring processes...\n")
    
    try:
        while True:
            active_workers = 0
            for i, p in processes:
                if p.poll() is None:
                    active_workers += 1
            
            if active_workers == 0:
                print("All workers have finished.")
                break
                
            print(f"Active workers: {active_workers}/{num_workers}", end="\r")
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\nStopping all workers...")
        for i, p in processes:
            p.terminate()
        print("Terminated.")

if __name__ == "__main__":
    main()
