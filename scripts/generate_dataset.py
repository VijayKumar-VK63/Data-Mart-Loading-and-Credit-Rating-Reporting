import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

# Initialize faker and random seed for reproducibility
fake = Faker()
np.random.seed(42)
random.seed(42)

# Define vendors, exchanges, sectors, and rating scales
vendors = [
    {"Vendor_Id": 10, "Vendor_Name": "Moody's", "Vendor_Code": "MDY", "Source_Feed_Id": 7},
    {"Vendor_Id": 20, "Vendor_Name": "S&P", "Vendor_Code": "SNP", "Source_Feed_Id": 9},
    {"Vendor_Id": 30, "Vendor_Name": "Fitch", "Vendor_Code": "FTC", "Source_Feed_Id": 11}
]

exchanges = ["NSE", "BSE", "LON", "SGX", "NYSE"]
countries = ["India", "USA", "UK", "Singapore", "Germany"]
sectors = ["Finance", "Automotive", "Energy", "Technology", "Healthcare"]
rating_types = ["Long Term", "Short Term", "Recovery", "Viability"]
ratings_scale = ["AAA", "AA+", "AA", "AA-", "A+", "A", "BBB", "BB", "B", "CCC"]
rating_map = {r: i for i, r in enumerate(ratings_scale[::-1], 1)}  # CCC=1, AAA=10

# Generate securities
num_securities = 100
securities = [f"SEC{str(i).zfill(4)}" for i in range(1, num_securities + 1)]

records = []

for security in securities:
    security_name = fake.company()
    country = random.choice(countries)
    sector = random.choice(sectors)
    exchange = random.choice(exchanges)
    rating_type = random.choice(rating_types)
    
    for vendor in vendors:
        # Each vendor provides multiple ratings for same security over time
        num_ratings = random.randint(2, 5)
        start_date = datetime(2022, 1, 1)
        
        for i in range(num_ratings):
            rating_date = start_date + timedelta(days=random.randint(30, 365) * i)
            rating_code = random.choice(ratings_scale)
            rating_score = rating_map[rating_code]
            rating_reason = random.choice([
                "Improved financial stability",
                "Decreased liquidity ratio",
                "Upgraded due to strong quarterly earnings",
                "Downgraded after debt increase",
                "Stable outlook maintained"
            ])
            is_active = True if i == num_ratings - 1 else False
            
            records.append({
                "Security_Id": security,
                "Security_Name": security_name,
                "Vendor_Id": vendor["Vendor_Id"],
                "Vendor_Name": vendor["Vendor_Name"],
                "Vendor_Code": vendor["Vendor_Code"],
                "Source_Feed_Id": vendor["Source_Feed_Id"],
                "Rating_Type": rating_type,
                "Exchange_Code": exchange,
                "Country": country,
                "Sector": sector,
                "Rating_Date": rating_date.strftime("%Y-%m-%d"),
                "Rating_Code": rating_code,
                "Rating_Score": rating_score,
                "Rating_Reason": rating_reason,
                "Is_Active": is_active
            })

# Convert to DataFrame
df = pd.DataFrame(records)

# Shuffle and limit to ~1000 rows
df = df.sample(frac=1).reset_index(drop=True)
df_final = df.head(1000)

# Save to CSV
output_path = "data/credit_rating_dataset_final.csv"
df_final.to_csv(output_path, index=False)
