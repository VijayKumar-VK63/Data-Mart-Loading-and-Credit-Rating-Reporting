import pandas as pd
import numpy as np

# Load the dataset
file_path = "data/credit_rating_dataset_final.csv"
df = pd.read_csv(file_path)

# ----- 1. Handle Missing Values -----
# Check missing values
missing_summary = df.isnull().sum()

# Fill missing values where logical; otherwise drop
df["Rating_Reason"].fillna("Not Available", inplace=True)
df.dropna(subset=["Security_Id", "Rating_Code", "Vendor_Id"], inplace=True)

# ----- 2. Remove Duplicates -----
df.drop_duplicates(inplace=True)

# ----- 3. Standardize Formats -----
# Ensure consistent date format
df["Rating_Date"] = pd.to_datetime(df["Rating_Date"], errors="coerce")

# Standardize text columns (upper case for IDs and codes)
df["Security_Id"] = df["Security_Id"].str.upper()
df["Rating_Code"] = df["Rating_Code"].str.upper()
df["Exchange_Code"] = df["Exchange_Code"].str.upper()
df["Country"] = df["Country"].str.title()
df["Sector"] = df["Sector"].str.title()
df["Vendor_Name"] = df["Vendor_Name"].str.title()

# ----- 4. Normalize Ratings -----
rating_map = {
    "AAA": 10, "AA+": 9, "AA": 8, "AA-": 7,
    "A+": 6, "A": 5, "BBB": 4, "BB": 3, "B": 2, "CCC": 1
}
df["Rating_Score"] = df["Rating_Code"].map(rating_map)

# ----- 5. Create Derived Columns -----
df["Rating_Year"] = df["Rating_Date"].dt.year
df["Rating_Month"] = df["Rating_Date"].dt.month
df["Effective_Date"] = df.groupby(["Security_Id", "Vendor_Id"])["Rating_Date"].transform("min")

# ----- 6. Validate Business Rules -----
# Ensure each (Security_Id, Vendor_Id) pair has only one active rating
active_check = df[df["Is_Active"] == True].groupby(["Security_Id", "Vendor_Id"]).size().max()

# Ensure ratings within valid range
invalid_ratings = df[~df["Rating_Code"].isin(rating_map.keys())]

# ----- 7. Export Cleaned Data -----
output_path_cleaned = "data/credit_rating_dataset_cleaned.csv"
df.to_csv(output_path_cleaned, index=False)
