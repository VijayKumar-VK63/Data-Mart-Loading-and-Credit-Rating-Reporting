import sqlite3, os

db_path = "data/credit_rating_dm.db"

print("DB File Path:", os.path.abspath(db_path))
print("File Exists:", os.path.exists(db_path))
print("File Size (bytes):", os.path.getsize(db_path) if os.path.exists(db_path) else "N/A")

if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT name, type FROM sqlite_master WHERE type IN ('table','view')")
    print("Tables/Views Found:", cur.fetchall())
    conn.close()
