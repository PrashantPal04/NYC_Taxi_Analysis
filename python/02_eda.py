import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# ══════════════════════════════════════════
# SETUP
# ══════════════════════════════════════════

os.makedirs("python/charts", exist_ok=True)
sns.set_theme(style="whitegrid")

print("Loading cleaned data...")
df = pd.read_csv("data/processed/trips_clean.csv")
print(f"Rows loaded: {len(df):,}")

df = df[
    (df['year'] == 2025) &
    (df['month'].isin([1, 2, 3]))
]
print(f"After month filter: {len(df):,}")

# ══════════════════════════════════════════
# CHART 1: Heatmap — Hour vs Day Demand
# ══════════════════════════════════════════

print("Chart 1: Heatmap...")

day_order = ['Monday','Tuesday','Wednesday',
             'Thursday','Friday','Saturday','Sunday']

pivot = (
    df.groupby(['day_name','pickup_hour'])['trip_distance']
    .count()
    .unstack()
)
pivot = pivot.reindex(day_order)

plt.figure(figsize=(16, 5))
sns.heatmap(
    pivot,
    cmap = 'YlOrRd',
    linewidths = 0.3,
    annot = False,
    fmt = '.0f'
)
plt.title("Trip Demand by Hour and Day of Week — Q1 2025",
          fontsize=14, pad=12)
plt.xlabel("Hour of Day (0–23)")
plt.ylabel("Day of Week")
plt.tight_layout()
plt.savefig("python/charts/01_heatmap_demand.png", dpi=150)
plt.close()
print("  Saved: 01_heatmap_demand.png ✓")

# ══════════════════════════════════════════
# CHART 2: Bar Chart — Peak Hours Top 10
# ══════════════════════════════════════════

print("Chart 2: Peak hours...")

hourly = (
    df.groupby('pickup_hour')['trip_distance']
    .count()
    .reset_index()
    .rename(columns={'trip_distance': 'trips'})
    .sort_values('trips', ascending=False)
    .head(10)
)

plt.figure(figsize=(10, 5))
bars = sns.barplot(
    data = hourly,
    x = 'pickup_hour',
    y = 'trips',
    palette = 'YlOrRd_r',
    order = hourly['pickup_hour']
)
plt.title("Top 10 Peak Hours by Trip Volume — Q1 2025",
          fontsize=14, pad=12)
plt.xlabel("Hour of Day")
plt.ylabel("Total Trips")
plt.tight_layout()
plt.savefig("python/charts/02_peak_hours.png", dpi=150)
plt.close()
print("  Saved: 02_peak_hours.png ✓")

# ══════════════════════════════════════════
# CHART 3: Boxplot — Trip Duration by Hour
# ══════════════════════════════════════════

print("Chart 3: Duration boxplot...")

# Sample lenge warna bohot slow hoga
sample_df = df[df['trip_duration_min'] < 60].sample(
    n=200_000, random_state=42
)

plt.figure(figsize=(16, 5))
sns.boxplot(
    data = sample_df,
    x = 'pickup_hour',
    y = 'trip_duration_min',
    color = '#1d9e75',
    fliersize = 1,
    linewidth = 0.8
)
plt.title("Trip Duration Distribution by Hour — Q1 2025",
          fontsize=14, pad=12)
plt.xlabel("Pickup Hour")
plt.ylabel("Duration (minutes)")
plt.tight_layout()
plt.savefig("python/charts/03_duration_boxplot.png", dpi=150)
plt.close()
print("  Saved: 03_duration_boxplot.png ✓")

# ══════════════════════════════════════════
# CHART 4: Line Chart — MoM Trip Growth
# ══════════════════════════════════════════

print("Chart 4: MoM growth...")

monthly = (
    df.groupby(['month','month_name'])['trip_distance']
    .count()
    .reset_index()
    .rename(columns={'trip_distance': 'trips'})
    .sort_values('month')
)

plt.figure(figsize=(8, 5))
plt.plot(
    monthly['month_name'],
    monthly['trips'],
    marker = 'o',
    linewidth = 2.5,
    color = '#1d9e75',
    markersize = 8
)

for _, row in monthly.iterrows():
    plt.annotate(
        f"{row['trips']:,.0f}",
        (row['month_name'], row['trips']),
        textcoords = "offset points",
        xytext = (0, 12),
        ha = 'center',
        fontsize = 10
    )

plt.title("Monthly Trip Volume — Q1 2025", fontsize=14, pad=12)
plt.xlabel("Month")
plt.ylabel("Total Trips")
plt.tight_layout()
plt.savefig("python/charts/04_mom_growth.png", dpi=150)
plt.close()
print("  Saved: 04_mom_growth.png ✓")

# ══════════════════════════════════════════
# CHART 5: Bar Chart — Congestion Pricing
# ══════════════════════════════════════════

print("Chart 5: Congestion pricing...")

cbd_monthly = (
    df.groupby(['month', 'month_name'])
    .apply(lambda x: pd.Series({
        'cbd_trips' : (x['cbd_congestion_fee'] > 0).sum(),
        'non_cbd_trips' : (x['cbd_congestion_fee'] == 0).sum()
    }))
    .reset_index()
    .sort_values('month')
)

fig, ax = plt.subplots(figsize=(8, 5))
x = range(len(cbd_monthly))
width = 0.35

ax.bar(x, cbd_monthly['cbd_trips'],
       width, label='CBD Trips', color='#E8593C')
ax.bar([i + width for i in x], cbd_monthly['non_cbd_trips'],
       width, label='Non-CBD Trips', color='#1d9e75')

ax.set_xticks([i + width/2 for i in x])
ax.set_xticklabels(cbd_monthly['month_name'])
ax.set_title("CBD vs Non-CBD Trips by Month — Q1 2025",
             fontsize=14, pad=12)
ax.set_ylabel("Total Trips")
ax.legend()
plt.tight_layout()
plt.savefig("python/charts/05_congestion_pricing.png", dpi=150)
plt.close()
print("  Saved: 05_congestion_pricing.png ✓")

print("\nAll charts saved to python/charts/ ✓")