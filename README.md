# 🚖 NYC Ride-Share Demand Intelligence --- Q1 2025

## 📊 Dashboard Preview

### Page 1 — Overview
<img width="2293" height="1285" alt="image" src="https://github.com/user-attachments/assets/0ebe0f11-f25f-4db3-8858-b3ee67e6cd17" />

### Page 2 — Demand Analysis
<img width="1152" height="649" alt="Screenshot 2026-04-22 at 5 21 48 PM" src="https://github.com/user-attachments/assets/9db4ac0a-094d-49aa-bd74-8cc1fd07e691" />

### Page 3 — Zone Insights
<img width="2302" height="1294" alt="image" src="https://github.com/user-attachments/assets/d308aacd-b978-4c05-8d78-37529ebbd343" />

### Page 4 — Fare 
<img width="2295" height="1285" alt="image" src="https://github.com/user-attachments/assets/589d8c6d-d696-45a8-a1d1-3df0f52100f9" />




## Business Problem (SCQA Framework)

### Situation

The NYC Taxi & Limousine Commission (TLC) has over **8.5M Yellow Taxi trip records** available from **January to March 2025**.  

Each record includes pickup/dropoff timestamps, zone locations, fare, distance, passenger count, and congestion fee.

### Complication

- Driver supply does not align with peak demand windows  

- Surge pricing is triggered during the evening rush (5--7 PM)  

- Airport zones show a **67--81% supply gap**  

- The impact of congestion pricing (introduced in Jan 2025) on trip behavior was unclear

### Question

1\. When and where is demand highest?  

2\. Which zones are underserved?  

3\. What is the impact of congestion pricing on Q1 2025?

### Answer

Using **Python EDA + PostgreSQL time-series analysis + Power BI dashboards**,  

we identified peak hours, underserved zones, and the impact of congestion pricing.

---

## 🛠 Tech Stack

| Layer | Tool |

|------|------|

| Data Cleaning & EDA | Python (Pandas, Seaborn, Matplotlib) |

| Database | PostgreSQL (Star Schema) |

| Advanced Queries | SQL (CTEs, Window Functions - LAG) |

| Visualization | Power BI |

| Dataset | NYC TLC Official Data |

---

## 📂 Dataset

- **Source:** NYC Taxi & Limousine Commission (Official)  

- **Period:** Q1 2025 (January --- March)  

- **Raw Rows:** 11.2 Million  

- **Clean Rows:** 8.5 Million (after ETL pipeline)  

- **Download:** https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

---

## 🧱 Star Schema Design
fact_trips (8.5M rows)\
├── dim_time (2,165 unique hours)\
├── dim_zone (265 NYC zones) ← Role-Playing Dimension\
│ ├── pickup_zone_key\
│ └── dropoff_zone_key\
└── dim_vendor (2 vendors)

---

## 📊 Key Findings

### 1. Peak Demand

- Hour **18 (6 PM)** is the busiest --- **623K trips**  

- Evening rush (5--7 PM) accounts for **21% of total trips**

### 2. Borough Dominance

- **Manhattan** generates **90% of total trips**  

- Revenue: **$178.5M in Q1**  

- **Queens** ranks second due to airport traffic  

- Avg airport fare: **$53**

### 3. Underserved Zones

- **JFK Airport:** 81% supply gap → 319K unmet trips  

- **LaGuardia:** 67% gap → 181K trips  

- **Penn Station:** 41% gap → high demand urban zone

### 4. Month-over-Month Growth

- February: **-5.6% decline** (short month + pricing shock)  

- March: **+15.9% trip growth** and **+21% revenue growth**

### 5. Congestion Pricing Impact

- CBD adoption: **65% (Jan) → 76% (Mar)**  

- Initial uncertainty stabilized by March  

- Avg congestion fee: **$0.75 (constant)**

---

## ⚙️ ETL Pipeline
Raw Parquet Files (11.2M rows)\
↓\
Python (Pandas)\
- Null rows removed: 2.26M\
- Outliers removed: 355K\
- Duration filter: 31K\
↓\
PostgreSQL (Star Schema)\
- fact_trips: 8,547,769 rows\
- dim_zone: 265 zones\
- dim_time: 2,165 unique hours\
- dim_vendor: 2 vendors\
↓\
Power BI Dashboard (4 Pages)
---

🚀 What Makes This Project Stand Out
------------------------------------

-   Uses **2025 real-world data** (not outdated datasets)
-   Covers **congestion pricing impact** (new policy insight)
-   Implements **advanced Star Schema (role-playing dimension)**
-   Built on **official government dataset**
-   Complete **end-to-end pipeline** (ETL → SQL → BI Dashboard)

* * * * *

📌 Author
---------

**Prashant Pal**\
B.Tech (ECE) | Data Analyst | QA Enthusiast\
Skills: Python, SQL, Power BI, PostgreSQL, Excel
