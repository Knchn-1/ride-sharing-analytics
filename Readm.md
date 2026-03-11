# 🚖 Real-Time Ride-Sharing Analytics Pipeline

A production-grade, end-to-end streaming data engineering project that simulates a ride-sharing platform (like Uber/Ola) with real-time surge pricing, driver zone recommendations, and a live Power BI dashboard.

---

## 🏗️ Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Data Generator │────▶│   Apache Kafka  │────▶│  Apache Spark   │
│  (Python)       │     │  2 Topics       │     │  Structured     │
│                 │     │  - ride_requests│     │  Streaming      │
│                 │     │  - driver_locs  │     │                 │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                          │
                                          ┌───────────────┼───────────────┐
                                          ▼               ▼               ▼
                                   ┌──────────┐   ┌──────────────┐  ┌──────────┐
                                   │PostgreSQL│   │ Parquet Files│  │  Console │
                                   │  4 Tables│   │  Data Lake   │  │  Output  │
                                   └────┬─────┘   └──────────────┘  └──────────┘
                                        │
                                        ▼
                                 ┌──────────────┐
                                 │  Power BI    │
                                 │  Dashboard   │
                                 │  (Live)      │
                                 └──────────────┘
```

---

## ✨ Features

- **Real-time data ingestion** — Kafka producer simulates continuous ride requests and driver location updates every 2 seconds
- **Dual Kafka topics** — `ride_requests` and `driver_locations` processed simultaneously
- **Surge pricing engine** — 2-minute tumbling window calculates demand-based multipliers (1.0x → 2.5x)
- **Driver zone recommendations** — Grid-based (~2km cells) hotspot detection ranks zones by ride demand
- **Dual storage** — Data written to both PostgreSQL (for analytics) and Parquet files (data lake)
- **Live Power BI dashboard** — 8 visuals including KPIs, trend charts, zone scatter map, and driver status

---

## 📊 Dashboard Visuals

| Visual | Type | Description |
|--------|------|-------------|
| Total Rides | Card | Running count of all ride requests |
| Surge Multiplier | Card | Current pricing multiplier |
| Surge KPI | KPI | Multiplier vs baseline target (1.0x) |
| Driver Status | Donut Chart | Available / On Trip / Offline breakdown |
| Ride Demand Trend | Line Chart | Ride count over 2-minute windows |
| Top Demand Zones | Bar Chart | Highest demand grid zones |
| Zone Scatter Map | Scatter Chart | Lat/long bubbles sized by demand |
| Latest Rides | Table | Most recent 10 ride requests |

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Message Broker | Apache Kafka 7.5.0 (Docker) |
| Stream Processing | Apache Spark 3.5.1 (PySpark) |
| Data Generator | Python, kafka-python |
| Storage — Warehouse | PostgreSQL |
| Storage — Data Lake | Apache Parquet |
| Containerization | Docker + Docker Compose |
| Visualization | Microsoft Power BI Desktop |
| Language | Python 3.x |

---

## 📁 Project Structure

```
ride-sharing-analytics/
│
├── docker/
│   └── docker-compose.yml          # Kafka + Zookeeper setup
│
├── data_generator/
│   └── generate_rides.py           # Kafka producer — ride + driver events
│
├── spark_streaming/
│   └── stream_processor.py         # PySpark streaming — 4 streams
│
├── data/
│   └── output/                     # Parquet data lake output
│       ├── rides/
│       ├── drivers/
│       ├── surge/
│       └── zones/
│
├── checkpoint/                     # Spark streaming checkpoints
├── setup_postgres.sql              # PostgreSQL table creation script
├── create_topics.sh                # Kafka topic creation script
├── postgresql-42.6.0.jar           # PostgreSQL JDBC driver
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Java 8+ (required for Spark)
- Docker Desktop
- PostgreSQL
- Power BI Desktop (Windows)

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/ride-sharing-analytics.git
cd ride-sharing-analytics
```

### 2. Create virtual environment

```bash
python -m venv .venv
.\.venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install pyspark==3.5.1 kafka-python faker psycopg2-binary
```

### 4. Download PostgreSQL JDBC JAR

```bash
python -c "import urllib.request; urllib.request.urlretrieve('https://jdbc.postgresql.org/download/postgresql-42.6.0.jar', 'postgresql-42.6.0.jar'); print('Done!')"
```

### 5. Start Kafka

```bash
cd docker
docker compose up -d
```

### 6. Create Kafka topics

```bash
bash create_topics.sh
```

### 7. Setup PostgreSQL

```bash
# Create database 'ride_analytics' in pgAdmin, then run:
psql -U postgres -d ride_analytics -f setup_postgres.sql
```

### 8. Update credentials

In `spark_streaming/stream_processor.py`, update:
```python
PG_PASSWORD = "your_password"
```

### 9. Run the pipeline

```bash
# Terminal 1 — already running (Kafka)

# Terminal 2 — start producer
python data_generator/generate_rides.py

# Terminal 3 — start Spark streaming
python spark_streaming/stream_processor.py
```

---

## 📈 Surge Pricing Logic

| Rides in 2-min Window | Multiplier | Status |
|-----------------------|------------|--------|
| 1 – 2 | 1.0x | 🟢 Normal |
| 3 – 5 | 1.5x | 🟡 Moderate Surge |
| 6 – 9 | 2.0x | 🟠 High Surge |
| 10+ | 2.5x | 🔴 Peak Surge |

---

## 🗺️ Driver Zone Logic

The Nagpur map area is divided into a grid of **0.02° × 0.02° cells (~2km each)**. Each cell is assigned a zone ID in the format `Z_<lat_cell>_<long_cell>`. Zones are ranked by ride demand every 10 seconds:

| Ride Demand | Recommendation |
|-------------|----------------|
| 5+ rides | HIGH DEMAND — Head here now |
| 3–4 rides | MODERATE — Worth moving here |
| 1–2 rides | LOW — Monitor zone |

---

## 🗄️ PostgreSQL Schema

```sql
ride_events     — ride_id, user_id, pickup_lat, pickup_long, timestamp
driver_events   — driver_id, driver_lat, driver_long, status, timestamp
surge_pricing   — window_start, window_end, ride_count, surge_multiplier, pricing_status
driver_zones    — zone_id, zone_center_lat, zone_center_long, ride_demand, recommendation
```

---

## 🔮 Future Improvements

- [ ] Deploy to AWS (MSK + EMR + RDS + QuickSight)
- [ ] Add driver-to-ride matching stream join
- [ ] Implement Delta Lake for ACID transactions
- [ ] Add data quality checks with Great Expectations
- [ ] CI/CD pipeline with GitHub Actions
- [ ] Containerize full pipeline with Docker Compose

---

## 👤 Author

**Kanchan** — Data Engineering Student  
📍 Nagpur, Maharashtra, India

---
