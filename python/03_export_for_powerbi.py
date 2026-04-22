import pandas as pd
from sqlalchemy import create_engine
import os

os.makedirs("data/powerbi", exist_ok=True)

engine = create_engine(
    "postgresql+psycopg2://postgres@localhost:5432/uber_analytics"
)

# ── Export 1: Peak Hours ──
pd.read_sql("""
    SELECT
        ft.pickup_hour,
        COUNT(*)                                     AS total_trips,
        ROUND(AVG(ft.fare_amount)::NUMERIC, 2)       AS avg_fare,
        ROUND(AVG(ft.trip_duration_min)::NUMERIC, 1) AS avg_duration_min
    FROM fact_trips ft
    JOIN dim_time dt ON ft.time_key = dt.time_key
    GROUP BY ft.pickup_hour
    ORDER BY ft.pickup_hour;
""", engine).to_csv("data/powerbi/01_peak_hours.csv", index=False)
print("01_peak_hours.csv ✓")

# ── Export 2: Borough Revenue ──
pd.read_sql("""
    SELECT
        dz.borough,
        COUNT(*)                                     AS total_trips,
        ROUND(SUM(ft.total_amount)::NUMERIC, 0)      AS total_revenue,
        ROUND(AVG(ft.fare_amount)::NUMERIC, 2)       AS avg_fare,
        ROUND(AVG(ft.trip_distance)::NUMERIC, 2)     AS avg_distance
    FROM fact_trips ft
    JOIN dim_zone dz ON ft.pickup_zone_key = dz.zone_key
    WHERE dz.borough NOT IN ('Unknown', 'NaN')
    GROUP BY dz.borough
    ORDER BY total_revenue DESC;
""", engine).to_csv("data/powerbi/02_borough_revenue.csv", index=False)
print("02_borough_revenue.csv ✓")

# ── Export 3: Time Slot ──
pd.read_sql("""
    SELECT
        dt.time_slot,
        COUNT(*)                                      AS total_trips,
        ROUND(AVG(ft.fare_amount)::NUMERIC, 2)        AS avg_fare,
        ROUND(AVG(ft.trip_duration_min)::NUMERIC, 1)  AS avg_duration_min,
        ROUND(SUM(ft.total_amount)::NUMERIC, 0)       AS total_revenue
    FROM fact_trips ft
    JOIN dim_time dt ON ft.time_key = dt.time_key
    GROUP BY dt.time_slot
    ORDER BY total_trips DESC;
""", engine).to_csv("data/powerbi/03_time_slots.csv", index=False)
print("03_time_slots.csv ✓")

# ── Export 4: MoM Growth ──
pd.read_sql("""
    WITH monthly AS (
        SELECT
            dt.month,
            dt.month_name,
            COUNT(*)                                 AS total_trips,
            ROUND(SUM(ft.total_amount)::NUMERIC, 0)  AS total_revenue
        FROM fact_trips ft
        JOIN dim_time dt ON ft.time_key = dt.time_key
        GROUP BY dt.month, dt.month_name
    )
    SELECT
        month,
        month_name,
        total_trips,
        total_revenue,
        ROUND(
            100.0 * (total_trips - LAG(total_trips)
            OVER (ORDER BY month))
            / NULLIF(LAG(total_trips)
            OVER (ORDER BY month), 0), 1
        ) AS trips_growth_pct,
        ROUND(
            100.0 * (total_revenue - LAG(total_revenue)
            OVER (ORDER BY month))
            / NULLIF(LAG(total_revenue)
            OVER (ORDER BY month), 0), 1
        ) AS revenue_growth_pct
    FROM monthly
    ORDER BY month;
""", engine).to_csv("data/powerbi/04_mom_growth.csv", index=False)
print("04_mom_growth.csv ✓")

# ── Export 5: Underserved Zones ──
pd.read_sql("""
    WITH pickup_counts AS (
        SELECT ft.pickup_zone_key AS zone_key, COUNT(*) AS pickups
        FROM fact_trips ft
        GROUP BY ft.pickup_zone_key
    ),
    dropoff_counts AS (
        SELECT ft.dropoff_zone_key AS zone_key, COUNT(*) AS dropoffs
        FROM fact_trips ft
        GROUP BY ft.dropoff_zone_key
    )
    SELECT
        dz.zone_name,
        dz.borough,
        pc.pickups,
        dc.dropoffs,
        pc.pickups - dc.dropoffs   AS supply_gap,
        ROUND(
            100.0 * (pc.pickups - dc.dropoffs)
            / NULLIF(pc.pickups, 0), 1
        )                          AS gap_pct
    FROM pickup_counts pc
    JOIN dropoff_counts dc ON pc.zone_key = dc.zone_key
    JOIN dim_zone dz       ON pc.zone_key = dz.zone_key
    WHERE pc.pickups > 10000
    ORDER BY supply_gap DESC
    LIMIT 15;
""", engine).to_csv("data/powerbi/05_underserved_zones.csv", index=False)
print("05_underserved_zones.csv ✓")

# ── Export 6: Congestion Pricing ──
pd.read_sql("""
    SELECT
        dt.month_name,
        dt.month,
        COUNT(*)                                         AS total_trips,
        COUNT(CASE WHEN ft.cbd_congestion_fee > 0
                   THEN 1 END)                           AS cbd_trips,
        ROUND(
            100.0 * COUNT(CASE WHEN ft.cbd_congestion_fee > 0
                               THEN 1 END)
            / COUNT(*), 1
        )                                                AS cbd_pct,
        ROUND(AVG(CASE WHEN ft.cbd_congestion_fee > 0
                       THEN ft.total_amount
                       END)::NUMERIC, 2)                 AS avg_fare_with_cbd,
        ROUND(AVG(CASE WHEN ft.cbd_congestion_fee = 0
                       THEN ft.total_amount
                       END)::NUMERIC, 2)                 AS avg_fare_without_cbd
    FROM fact_trips ft
    JOIN dim_time dt ON ft.time_key = dt.time_key
    GROUP BY dt.month_name, dt.month
    ORDER BY dt.month;
""", engine).to_csv("data/powerbi/06_congestion_pricing.csv", index=False)
print("06_congestion_pricing.csv ✓")

engine.dispose()
print("\nAll exports done — data/powerbi/ folder ready ✓")