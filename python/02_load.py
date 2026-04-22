import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os

# ══════════════════════════════════════════
# CONNECTION — PostgreSQL se connect karo
# ══════════════════════════════════════════

conn = psycopg2.connect(
    host     = "localhost",
    database = "uber_analytics",
    user     = "postgres",  
    password = "",  
    port     = 5432
)
cursor = conn.cursor()
print("Connected to PostgreSQL ✓")

# ══════════════════════════════════════════
# LOAD — dim_vendor (sabse chhoti, pehle)
# ══════════════════════════════════════════

print("Loading dim_vendor...")
cursor.execute("""
    INSERT INTO dim_vendor (vendor_id, vendor_name)
    VALUES (1, 'Creative Mobile Technologies'),
           (2, 'VeriFone Inc')
    ON CONFLICT (vendor_id) DO NOTHING;
""")
conn.commit()
print("dim_vendor done ✓")

# ══════════════════════════════════════════
# LOAD — dim_zone (zones_clean.csv se)
# ══════════════════════════════════════════

print("Loading dim_zone...")
zones = pd.read_csv("data/processed/zones_clean.csv")
zones = zones.rename(columns={
    'LocationID'   : 'location_id',
    'Zone'         : 'zone_name',
    'Borough'      : 'borough',
    'service_zone' : 'service_zone'
})

zone_records = list(zones[['location_id','zone_name',
                            'borough','service_zone']].itertuples(
                                index=False, name=None))

execute_values(cursor, """
    INSERT INTO dim_zone (location_id, zone_name, borough, service_zone)
    VALUES %s
    ON CONFLICT (location_id) DO NOTHING;
""", zone_records)
conn.commit()
print(f"dim_zone done ✓ — {len(zone_records)} zones")

# ══════════════════════════════════════════
# LOAD — dim_time (trips_clean.csv se)
# ══════════════════════════════════════════

print("Loading dim_time...")
df = pd.read_csv("data/processed/trips_clean.csv")
print(f"  trips_clean loaded: {len(df):,} rows")

time_df = df[[
    'time_key', 'full_date', 'pickup_hour',
    'day_name', 'month', 'month_name',
    'year', 'is_weekend', 'time_slot'
]].drop_duplicates(subset=['time_key'])

time_records = list(time_df.itertuples(index=False, name=None))

execute_values(cursor, """
    INSERT INTO dim_time (time_key, full_date, hour, day_name,
                          month, month_name, year, 
                          is_weekend, time_slot)
    VALUES %s
    ON CONFLICT (time_key) DO NOTHING;
""", time_records)
conn.commit()
print(f"dim_time done ✓ — {len(time_records):,} unique hours")

# ══════════════════════════════════════════
# LOAD — fact_trips (sabse badi, batches mein)
# ══════════════════════════════════════════

print("Loading fact_trips (batch wise)...")

# zone_key mapping banao
cursor.execute("SELECT location_id, zone_key FROM dim_zone;")
zone_map = {row[0]: row[1] for row in cursor.fetchall()}

# vendor_key mapping banao
cursor.execute("SELECT vendor_id, vendor_key FROM dim_vendor;")
vendor_map = {row[0]: row[1] for row in cursor.fetchall()}

# Columns select karo
fact_df = df[[
    'time_key', 'PULocationID', 'DOLocationID', 'VendorID',
    'passenger_count', 'trip_distance', 'fare_amount',
    'tip_amount', 'cbd_congestion_fee', 'total_amount',
    'trip_duration_min', 'pickup_hour'
]].copy()

# Keys map karo
fact_df['pickup_zone_key']  = fact_df['PULocationID'].map(zone_map)
fact_df['dropoff_zone_key'] = fact_df['DOLocationID'].map(zone_map)
fact_df['vendor_key']       = fact_df['VendorID'].map(vendor_map)

# Unmapped rows drop karo
fact_df = fact_df.dropna(subset=[
    'pickup_zone_key', 'dropoff_zone_key', 'vendor_key'
])
fact_df['pickup_zone_key']  = fact_df['pickup_zone_key'].astype(int)
fact_df['dropoff_zone_key'] = fact_df['dropoff_zone_key'].astype(int)
fact_df['vendor_key']       = fact_df['vendor_key'].astype(int)

# Batch mein insert karo — 100K rows at a time
BATCH_SIZE = 100_000
total      = len(fact_df)
inserted   = 0

cols = [
    'time_key', 'pickup_zone_key', 'dropoff_zone_key',
    'vendor_key', 'passenger_count', 'trip_distance',
    'fare_amount', 'tip_amount', 'cbd_congestion_fee',
    'total_amount', 'trip_duration_min', 'pickup_hour'
]

for start in range(0, total, BATCH_SIZE):
    batch = fact_df[cols].iloc[start:start + BATCH_SIZE]
    records = list(batch.itertuples(index=False, name=None))

    execute_values(cursor, """
        INSERT INTO fact_trips (
            time_key, pickup_zone_key, dropoff_zone_key,
            vendor_key, passenger_count, trip_distance,
            fare_amount, tip_amount, cbd_congestion_fee,
            total_amount, trip_duration_min, pickup_hour
        ) VALUES %s
    """, records)
    conn.commit()

    inserted += len(batch)
    pct       = (inserted / total) * 100
    print(f"  Progress: {inserted:,} / {total:,} ({pct:.1f}%)")

print(f"fact_trips done ✓ — {inserted:,} rows")

# ══════════════════════════════════════════
# VERIFY — final counts check karo
# ══════════════════════════════════════════

print("\n── Final Row Counts ──")
for table in ['dim_vendor','dim_zone','dim_time','fact_trips']:
    cursor.execute(f"SELECT COUNT(*) FROM {table};")
    count = cursor.fetchone()[0]
    print(f"  {table:20s} → {count:>10,}")

cursor.close()
conn.close()
print("\nLoad complete! ✓")