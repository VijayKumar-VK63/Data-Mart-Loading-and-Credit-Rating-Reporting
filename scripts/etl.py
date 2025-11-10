import pandas as pd
import sqlite3

# Load cleaned dataset
cleaned_path = "data/credit_rating_dataset_cleaned.csv"
df = pd.read_csv(cleaned_path)

# Create SQLite connection
db_path = "data/credit_rating_dm.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

# Drop tables if they exist
tables = ["fact_rating_history", "dim_security", "dim_vendor", "dim_rating_type", "dim_exchange", "dim_date"]
for tbl in tables:
    cur.execute(f"DROP TABLE IF EXISTS {tbl}")

# ----------------------
# Create Dimension Tables
# ----------------------
cur.execute("""
CREATE TABLE dim_security (
    security_key INTEGER PRIMARY KEY AUTOINCREMENT,
    security_id TEXT UNIQUE,
    security_name TEXT,
    country TEXT,
    sector TEXT
)
""")

cur.execute("""
CREATE TABLE dim_vendor (
    vendor_key INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor_id INTEGER UNIQUE,
    vendor_name TEXT,
    vendor_code TEXT
)
""")

cur.execute("""
CREATE TABLE dim_rating_type (
    rating_type_key INTEGER PRIMARY KEY AUTOINCREMENT,
    rating_type TEXT UNIQUE
)
""")

cur.execute("""
CREATE TABLE dim_exchange (
    exchange_key INTEGER PRIMARY KEY AUTOINCREMENT,
    exchange_code TEXT UNIQUE
)
""")

cur.execute("""
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY AUTOINCREMENT,
    rating_date TEXT UNIQUE,
    year INTEGER,
    month INTEGER,
    day INTEGER
)
""")

# ----------------------
# Create Fact Table (SCD Type 2)
# ----------------------
cur.execute("""
CREATE TABLE fact_rating_history (
    rating_key INTEGER PRIMARY KEY AUTOINCREMENT,
    security_key INTEGER,
    vendor_key INTEGER,
    rating_type_key INTEGER,
    exchange_key INTEGER,
    date_key INTEGER,
    rating_code TEXT,
    rating_score INTEGER,
    rating_reason TEXT,
    effective_date TEXT,
    expiry_date TEXT,
    is_active BOOLEAN,
    FOREIGN KEY(security_key) REFERENCES dim_security(security_key),
    FOREIGN KEY(vendor_key) REFERENCES dim_vendor(vendor_key),
    FOREIGN KEY(rating_type_key) REFERENCES dim_rating_type(rating_type_key),
    FOREIGN KEY(exchange_key) REFERENCES dim_exchange(exchange_key),
    FOREIGN KEY(date_key) REFERENCES dim_date(date_key)
)
""")

# ----------------------
# Load Dimension Tables
# ----------------------
def load_dim_table(df, table, cols, unique_col):
    unique_df = df[cols].drop_duplicates(subset=[unique_col]).reset_index(drop=True)
    unique_df.to_sql(table, conn, if_exists="append", index=False)
    return pd.read_sql(f"SELECT * FROM {table}", conn)

dim_security = load_dim_table(df, "dim_security", ["Security_Id", "Security_Name", "Country", "Sector"], "Security_Id")
dim_vendor = load_dim_table(df, "dim_vendor", ["Vendor_Id", "Vendor_Name", "Vendor_Code"], "Vendor_Id")
dim_rating_type = load_dim_table(df, "dim_rating_type", ["Rating_Type"], "Rating_Type")
dim_exchange = load_dim_table(df, "dim_exchange", ["Exchange_Code"], "Exchange_Code")
dim_date = load_dim_table(df, "dim_date", ["Rating_Date"], "Rating_Date")

# Fix lowercase column name issue
date_col = "rating_date" if "rating_date" in dim_date.columns else "Rating_Date"

# Add year, month, day to date dimension
dim_date["year"] = pd.to_datetime(dim_date[date_col]).dt.year
dim_date["month"] = pd.to_datetime(dim_date[date_col]).dt.month
dim_date["day"] = pd.to_datetime(dim_date[date_col]).dt.day

# Update SQLite table with date components
for _, row in dim_date.iterrows():
    cur.execute("""UPDATE dim_date SET year=?, month=?, day=? WHERE rating_date=?""",
                (row["year"], row["month"], row["day"], row[date_col]))

conn.commit()

# ----------------------
# Helper: Get surrogate key
# ----------------------
def get_key(table, id_col, id_val):
    q = f"SELECT ROWID FROM {table} WHERE {id_col}=?"
    res = cur.execute(q, (id_val,)).fetchone()
    return res[0] if res else None

# ----------------------
# Insert Fact Records with SCD2 Logic
# ----------------------
records_inserted = 0
records_updated = 0

for _, row in df.iterrows():
    security_key = get_key("dim_security", "security_id", row["Security_Id"])
    vendor_key = get_key("dim_vendor", "vendor_id", row["Vendor_Id"])
    rating_type_key = get_key("dim_rating_type", "rating_type", row["Rating_Type"])
    exchange_key = get_key("dim_exchange", "exchange_code", row["Exchange_Code"])
    date_key = get_key("dim_date", "rating_date", row["Rating_Date"])

    # Check for existing active record for same (Security, Vendor)
    active_row = cur.execute("""
        SELECT rating_key, rating_code FROM fact_rating_history
        WHERE security_key=? AND vendor_key=? AND is_active=1
    """, (security_key, vendor_key)).fetchone()

    if active_row:
        if active_row[1] != row["Rating_Code"]:
            cur.execute("""UPDATE fact_rating_history SET is_active=0, expiry_date=? WHERE rating_key=?""",
                        (row["Rating_Date"], active_row[0]))
            records_updated += 1

    # Insert new record
    cur.execute("""
        INSERT INTO fact_rating_history (
            security_key, vendor_key, rating_type_key, exchange_key, date_key, 
            rating_code, rating_score, rating_reason, effective_date, expiry_date, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        security_key, vendor_key, rating_type_key, exchange_key, date_key,
        row["Rating_Code"], row["Rating_Score"], row["Rating_Reason"],
        row["Rating_Date"], None, row["Is_Active"]
    ))
    records_inserted += 1

conn.commit()

summary = pd.DataFrame([{
    "Records Inserted": records_inserted,
    "Records Updated (Expired)": records_updated,
    "Total Fact Rows": cur.execute("SELECT COUNT(*) FROM fact_rating_history").fetchone()[0],
    "Total Securities": cur.execute("SELECT COUNT(*) FROM dim_security").fetchone()[0],
    "Total Vendors": cur.execute("SELECT COUNT(*) FROM dim_vendor").fetchone()[0],
    "Total Rating Types": cur.execute("SELECT COUNT(*) FROM dim_rating_type").fetchone()[0]
}])

conn.close()
