import sqlite3
import pandas as pd

# Reconnect to the existing SQLite Data Mart
db_path = "data/credit_rating_dm.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

# --------------------------
# 1️⃣ Rating Frequency per Vendor
# --------------------------
cur.execute("DROP VIEW IF EXISTS vw_rating_frequency")
cur.execute("""
CREATE VIEW vw_rating_frequency AS
SELECT
    v.vendor_name,
    s.security_name,
    COUNT(f.rating_key) AS total_ratings,
    SUM(CASE WHEN f.is_active = 1 THEN 1 ELSE 0 END) AS active_ratings,
    COUNT(DISTINCT d.year) AS years_covered
FROM fact_rating_history f
JOIN dim_vendor v ON f.vendor_key = v.vendor_key
JOIN dim_security s ON f.security_key = s.security_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY v.vendor_name, s.security_name
""")

# --------------------------
# 2️⃣ Outlier Detection per Security
# --------------------------
cur.execute("DROP VIEW IF EXISTS vw_outlier_detection")
cur.execute("""
CREATE VIEW vw_outlier_detection AS
SELECT
    s.security_name,
    d.year,
    AVG(f.rating_score) AS avg_rating_score,
    MAX(f.rating_score) - MIN(f.rating_score) AS rating_range,
    CASE
        WHEN (MAX(f.rating_score) - MIN(f.rating_score)) >= 3 THEN 'YES'
        ELSE 'NO'
    END AS has_outlier
FROM fact_rating_history f
JOIN dim_security s ON f.security_key = s.security_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY s.security_name, d.year
""")

# --------------------------
# 3️⃣ Precision Metrics (Vendor Agreement)
# --------------------------
cur.execute("DROP VIEW IF EXISTS vw_precision_metrics")
cur.execute("""
CREATE VIEW vw_precision_metrics AS
WITH vendor_dispersion AS (
    SELECT
        s.security_name,
        d.year,
        MAX(f.rating_score) AS max_score,
        MIN(f.rating_score) AS min_score
    FROM fact_rating_history f
    JOIN dim_security s ON f.security_key = s.security_key
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY s.security_name, d.year
)
SELECT
    year,
    COUNT(DISTINCT security_name) AS total_securities,
    SUM(CASE WHEN (max_score - min_score) = 0 THEN 1 ELSE 0 END) AS same_rating_count,
    ROUND(SUM(CASE WHEN (max_score - min_score) = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT security_name), 2) AS precision_percentage
FROM vendor_dispersion
GROUP BY year
""")

conn.commit()

# --------------------------
# Fetch Samples for Verification
# --------------------------
rating_freq_df = pd.read_sql("SELECT * FROM vw_rating_frequency LIMIT 5", conn)
outlier_df = pd.read_sql("SELECT * FROM vw_outlier_detection LIMIT 5", conn)
precision_df = pd.read_sql("SELECT * FROM vw_precision_metrics LIMIT 5", conn)

conn.close()
