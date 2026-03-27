import sqlite3
from db_config import DB_FILE

def reset_tasks():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("UPDATE tasks SET status = 'pending' WHERE status = 'completed'")
        print(f"Success: Reset {c.rowcount} tasks to pending.")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    reset_tasks()
