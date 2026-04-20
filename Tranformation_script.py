# ============================================================
# NYC Taxi — Local ETL Script
# Run this on your laptop BEFORE uploading to S3
# Requirements: pip install pandas pyarrow
# ============================================================

import pandas as pd
import os

# ── Auto-detect the parquet file in the current directory ──
INPUT_FILE  = None
for fname in os.listdir("."):
    if fname.startswith("yellow_tripdata") and fname.endswith(".parquet"):
        INPUT_FILE = fname
        break

if INPUT_FILE is None:
    raise FileNotFoundError(
        "No 'yellow_tripdata*.parquet' file found in the current directory.\n"
        "Download from: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    )

OUTPUT_FILE = "nyc_taxi_clean.parquet"   # upload THIS to S3

print("=" * 50)
print("NYC Taxi ETL — Local Processing")
print(f"Input file : {INPUT_FILE}")
print("=" * 50)

# ── 1. Load raw data ─────────────────────────────────────
df = pd.read_parquet(INPUT_FILE)
print(f"\nRAW row count       : {len(df):,}")
print(f"Columns             : {list(df.columns)}")

# ── 2. Remove invalid trips ──────────────────────────────
df = df[df["trip_distance"]  > 0]
df = df[df["fare_amount"]    > 0]
df = df[df["fare_amount"]    < 500]
df = df[df["total_amount"]   > 0]
df = df[df["tip_amount"]     >= 0]

# passenger_count can have NaN in newer datasets — fill & filter
if "passenger_count" in df.columns:
    df["passenger_count"] = df["passenger_count"].fillna(1)
    df = df[df["passenger_count"].between(1, 6)]

print(f"After basic filters : {len(df):,}")

# ── 3. Fix timestamps ────────────────────────────────────
df["tpep_pickup_datetime"]  = pd.to_datetime(df["tpep_pickup_datetime"])
df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

# Remove trips where dropoff is before or equal to pickup
df = df[df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"]]
print(f"After time filter   : {len(df):,}")

# ── 4. Keep only records from the file's own month ───────
year  = df["tpep_pickup_datetime"].dt.year.mode()[0]
month = df["tpep_pickup_datetime"].dt.month.mode()[0]
df = df[
    (df["tpep_pickup_datetime"].dt.year  == year) &
    (df["tpep_pickup_datetime"].dt.month == month)
]
print(f"After date filter ({year}-{month:02d})  : {len(df):,}")

# ── 5. Remove extreme trip durations ─────────────────────
df["trip_duration_mins"] = (
    (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
    .dt.total_seconds() / 60
).round(2)
df = df[df["trip_duration_mins"].between(1, 240)]
print(f"After duration filter: {len(df):,}")

# ── 6. Engineer features ──────────────────────────────────
df["revenue_per_mile"] = (df["total_amount"] / df["trip_distance"]).round(2)
df["tip_pct"]          = (df["tip_amount"]   / df["fare_amount"] * 100).round(1)
df["pickup_hour"]      = df["tpep_pickup_datetime"].dt.hour
df["pickup_dow"]       = df["tpep_pickup_datetime"].dt.dayofweek   # 0=Mon, 6=Sun
df["pickup_day"]       = df["tpep_pickup_datetime"].dt.day_name()
df["pickup_month"]     = df["tpep_pickup_datetime"].dt.month
df["year"]             = df["tpep_pickup_datetime"].dt.year
df["is_weekend"]       = df["pickup_day"].isin(["Saturday", "Sunday"])

# Airport trip flag — RatecodeID may not exist in all versions
if "RatecodeID" in df.columns:
    df["is_airport_trip"] = df["RatecodeID"].isin([2, 3]).astype(int)
else:
    df["is_airport_trip"] = 0

df["fare_category"] = pd.cut(
    df["trip_distance"],
    bins=[0, 2, 10, 9999],
    labels=["Short", "Medium", "Long"]
)

df["payment_label"] = df["payment_type"].map({
    1: "Credit Card",
    2: "Cash",
    3: "No Charge",
    4: "Dispute",
    5: "Unknown"
}).fillna("Other")

# ── 7. Drop duplicates ────────────────────────────────────
before_dedup = len(df)
df = df.drop_duplicates(
    subset=["tpep_pickup_datetime", "PULocationID",
            "DOLocationID", "total_amount"]
)
print(f"Duplicates removed  : {before_dedup - len(df):,}")
print(f"FINAL clean count   : {len(df):,}")

# ── 8. Select final columns (only those that exist) ───────
desired_cols = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance",
    "PULocationID", "DOLocationID",
    "payment_type", "payment_label",
    "fare_amount", "tip_amount", "total_amount",
    "trip_duration_mins", "revenue_per_mile", "tip_pct",
    "pickup_hour", "pickup_dow", "pickup_day",
    "pickup_month", "year",
    "is_weekend", "is_airport_trip", "fare_category",
]
# Optional columns — include only if present in dataset
optional_cols = [
    "VendorID", "RatecodeID", "extra", "mta_tax",
    "tolls_amount", "improvement_surcharge", "congestion_surcharge",
    "airport_fee",
]
final_cols = [c for c in desired_cols if c in df.columns] + \
             [c for c in optional_cols if c in df.columns]

df = df[final_cols]

# ── 9. Save as Parquet (compressed) ──────────────────────
df.to_parquet(OUTPUT_FILE, index=False, compression="snappy")
file_size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)

raw_count = before_dedup  # approximate original after basic cleaning intent
print(f"\nOutput file         : {OUTPUT_FILE}")
print(f"File size           : {file_size_mb:.1f} MB")
print(f"\nData quality summary:")
print(f"  Clean rows        : {len(df):,}")
print(f"\nDone! Upload {OUTPUT_FILE} to s3://your-bucket/processed/")
print("=" * 50)