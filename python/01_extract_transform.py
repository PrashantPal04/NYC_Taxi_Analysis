import pandas as pd
import os
df = pd.read_parquet("data/raw/yellow_tripdata_2025-01.parquet")

# Having a look at the data

print(df.shape)
print("─" * 40)
print(df.dtypes)
print("─" * 40)
print(df.head(3))
print("─" * 40)
print(df.isnull().sum())

# ══════════════════════════════════════════
# EXTRACT — 6 months ka data load karo
# ══════════════════════════════════════════

months = [
    "data/raw/yellow_tripdata_2025-01.parquet",
    "data/raw/yellow_tripdata_2025-02.parquet",
    "data/raw/yellow_tripdata_2025-03.parquet",
]

print("Loading files...")
df = pd.concat([pd.read_parquet(f) for f in months], ignore_index=True)
print(f"Total rows loaded: {df.shape[0]:,}")

# ══════════════════════════════════════════
# TRANSFORM — Step 1: Null rows drop karo
# ══════════════════════════════════════════

df = df.dropna(subset=[
    'passenger_count',
    'RatecodeID',
    'store_and_fwd_flag',
    'congestion_surcharge',
    'Airport_fee'
])
print(f"After null drop: {df.shape[0]:,}")

# ══════════════════════════════════════════
# TRANSFORM — Step 2: Outliers remove karo
# ══════════════════════════════════════════

df = df[
    (df['trip_distance'] > 0) & 
    (df['trip_distance'] < 100) &
    (df['fare_amount'] > 0) & 
    (df['fare_amount'] < 500) &
    (df['passenger_count'] > 0) & 
    (df['passenger_count'] <= 6) &
    (df['total_amount'] > 0)
]
print(f"After outlier removal: {df.shape[0]:,}")

# ══════════════════════════════════════════
# TRANSFORM — Step 3: Feature Engineering
# ══════════════════════════════════════════

# Trip duration
df['trip_duration_min'] = (
    df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']
).dt.total_seconds() / 60

# Duration outliers
df = df[
    (df['trip_duration_min'] > 0) &
    (df['trip_duration_min'] < 180)
]
print(f"After duration filter: {df.shape[0]:,}")

# Time features
df['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour
df['day_name'] = df['tpep_pickup_datetime'].dt.day_name()
df['month'] = df['tpep_pickup_datetime'].dt.month
df['month_name'] = df['tpep_pickup_datetime'].dt.strftime('%B')
df['year'] = df['tpep_pickup_datetime'].dt.year
df['full_date'] = df['tpep_pickup_datetime'].dt.date
df['is_weekend'] = df['tpep_pickup_datetime'].dt.dayofweek >= 5

# Time slot

def assign_time_slot(hour):
    if 7 <= hour <= 9:
        return 'Morning rush'
    elif 17 <= hour <= 19:
        return 'Evening rush'
    elif 22 <= hour <= 23:
        return 'Late night'
    else:
        return 'Off-peak'

df['time_slot'] = df['pickup_hour'].apply(assign_time_slot)
# time_key — YYYYMMDDHH format (dim_time ka PK)
df['time_key'] = (
    df['tpep_pickup_datetime'].dt.strftime('%Y%m%d%H')
).astype(int)

# passenger_count float → int
df['passenger_count'] = df['passenger_count'].astype(int)

# ══════════════════════════════════════════
# TRANSFORM — Step 4: Sirf zaroori columns rakho
# ══════════════════════════════════════════

df = df[[
    'time_key',
    'full_date',
    'pickup_hour',
    'day_name',
    'month',
    'month_name',
    'year',
    'is_weekend',
    'time_slot',
    'VendorID',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'tip_amount',
    'cbd_congestion_fee',
    'total_amount',
    'trip_duration_min',
]]

print(f"Final shape: {df.shape}")
print(f"Columns: {list(df.columns)}")

# ══════════════════════════════════════════
# EXPORT — processed folder mein save karo
# ══════════════════════════════════════════

os.makedirs("data/processed", exist_ok=True)
df.to_csv("data/processed/trips_clean.csv", index=False)
print("Saved: data/processed/trips_clean.csv")

# Zone lookup bhi copy karo processed mein
zones = pd.read_csv("data/raw/taxi_zone_lookup.csv")
zones.to_csv("data/processed/zones_clean.csv", index=False)
print("Saved: data/processed/zones_clean.csv")

print("\nTransform complete!")