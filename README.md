# Industrial Sensor ELT Pipeline & Predictive Maintenance Analytics

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-2.9.3-017CEE.svg)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791.svg)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker_Compose-v2-2496ED.svg)](https://www.docker.com/)

An end-to-end, production-grade **ELT (Extract, Load, Transform)** data pipeline designed for industrial equipment telemetry, predictive maintenance modeling, and statistical anomaly detection.

Orchestrated using **Apache Airflow**, containerized with **Docker**, modeled in **PostgreSQL (Kimball Star Schema)**, and protected by **Automated Data Quality Gates**.

---

## Architecture & Data Flow

```
                                [ INDUSTRIAL SENSOR ELT ARCHITECTURE ]

    Source CSV / IoT Stream (UCI AI4I 2020 Predictive Maintenance)
                                 │
                                 ▼
                     ┌───────────────────────┐
                     │ [Airflow] extract_raw │
                     │ Python + psycopg2     │
                     └───────────┬───────────┘
                                 │  Idempotent batch load (ON CONFLICT DO UPDATE)
                                 ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        PostgreSQL Data Warehouse (sensor_dw)                           │
│                                                                                        │
│  [1. Raw Layer]                raw.sensor_telemetry (10,000 records)                   │
│                                raw.ingestion_log (Batch audit trail)                   │
│                                        │                                               │
│                                        ▼  (CTE Transformations: Unit conversions,      │
│                                            Power kW, Temperature Differential)         │
│  [2. Staging Layer]            staging.stg_sensor_telemetry                            │
│                                        │                                               │
│                                        ▼  (Dimensional Modeling / Star Schema)         │
│  [3. Warehouse Layer]          warehouse.dim_equipment (Surrogate PK: equipment_key)  │
│                                warehouse.dim_failure_mode (Failure catalog)            │
│                                warehouse.fact_sensor_telemetry (Fact metrics)          │
│                                        │                                               │
│                                        ▼  (Advanced SQL Analytics & Features)          │
│  [4. Analytics Layer]          analytics.v_sensor_anomaly_features (Z-Score > 2.5)     │
│                                analytics.v_failure_root_cause_summary (DENSE_RANK)     │
│                                analytics.v_high_risk_equipment_leaderboard             │
│                                                                                        │
│  [5. Quality Audit Trail]      quality.check_log (Automated assertion history)         │
└────────────────────────────────────────────────────────────────────────────────────────┘
                                 ▲
                                 │  Airflow Circuit Breaker (Halts on critical failure)
                     ┌───────────┴───────────┐
                     │ [Airflow] quality_gate│
                     │ Automated Assertions  │
                     └───────────────────────┘
```

---

## Key Highlights & Engineering Design

1. **True ELT Philosophy**: Raw data is landed directly with lineage metadata (`batch_id`, `source_dataset`, `ingested_at`). All heavy transformations, physical feature calculations, and dimensional models are computed in PostgreSQL.
2. **Dual-Database Isolation**: Airflow's internal transactional metadata DB is strictly isolated from the analytical Data Warehouse DB, mirroring production best practices.
3. **Dimensional Star Schema**: Implements surrogate keys (`SERIAL equipment_key`), normalized descriptive attributes, and indexed foreign keys for sub-second analytical queries.
4. **Advanced SQL Features**:
   - **Chained CTEs** for modular, self-documenting data transformations.
   - **Window Functions** (`AVG() OVER (ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)`, `STDDEV()`, `LAG()`) to smooth sensor noise and detect delta surges.
   - **Statistical Anomaly Detection**: Real-time Z-Score computation $(x - \mu) / \sigma > 2.5$.
   - **Ranking**: `DENSE_RANK()` and `NTILE(5)` quintile failure distribution.
5. **Data Quality Circuit Breakers**: 10 automated assertions covering completeness (0% nulls), physical sensor boundaries, row-count thresholds, and referential integrity.
6. **Airflow Orchestration**: Linear DAG with automated retries, XCom-driven lineage tracking, and failure alerting.

---

## Repository Structure

```
elt_pipeline/
├── dags/
│   └── dag_sensor_elt_pipeline.py    # Master Airflow DAG
├── pipelines/
│   ├── extractors/
│   │   └── sensor_extractor.py       # Batch extraction & idempotent upsert
│   ├── transformers/
│   │   └── sql_runner.py             # Transactional SQL transformation runner
│   └── quality/
│       └── validators.py             # Automated quality assertion suite
├── sql/
│   ├── raw/
│   │   └── 01_create_raw_schema.sql  # Raw tables & audit log schema
│   ├── staging/
│   │   └── 01_create_staging_schema.sql # Staging schema with physical features
│   ├── warehouse/
│   │   └── 01_create_warehouse_schema.sql # Star schema (Facts & Dimensions)
│   ├── transforms/
│   │   ├── 01_load_staging.sql       # Raw -> Staging CTE transform
│   │   └── 02_build_star_schema.sql  # Staging -> Warehouse load
│   └── analytics/
│       ├── 01_create_analytics_views.sql # Rolling stats & anomaly views
│       └── 02_interview_queries.sql  # 5 Standout SQL interview queries
├── dashboard/
│   ├── api.py                        # FastAPI backend for warehouse observability
│   └── static/
│       ├── index.html                # Responsive web dashboard interface
│       ├── styles.css                # Glassmorphic dark theme styling
│       └── app.js                    # Interactive JS client with live SQL sandbox
├── data/
│   └── raw/                          # Source telemetry dataset
├── scripts/
│   ├── download_data.py              # Automated dataset downloader
│   ├── setup_dw.py                   # Data warehouse initialization script
│   └── run_dashboard.py              # Single-command dashboard launcher
├── docker-compose.yml                # Airflow + PostgreSQL infrastructure
├── requirements.txt                  # Python dependencies
└── .env.example                      # Environment configuration template
```

---

## Interactive Observability Dashboard UI

A full interactive web dashboard is included to inspect all data warehouse layers, anomalies, quality checks, and execute live queries.

```bash
# Launch the dashboard locally
python3 scripts/run_dashboard.py
```

Open **[http://localhost:8000](http://localhost:8000)** in your browser to access:
- **Live ELT Data Flow Architecture**: Interactive pipeline diagram (**Raw -> Staging -> Warehouse -> Analytics & Quality**) with schema definitions and live sample data.
- **Telemetry & Anomaly Explorer**: High-risk equipment leaderboard, failure root-cause distribution, and Z-score anomaly detector ($|Z| > 2.5\sigma$).
- **Quality Gates & Governance**: Live monitoring of all 8 data quality assertions and persistent SLA audit logs (`quality.check_log`).
- **Interactive SQL Sandbox**: Preloaded interview queries (Rolling Averages, Dense Ranking, Anti-joins, Star Schema joins) with real-time query execution benchmarking.
- **One-Click Pipeline Execution**: Trigger full end-to-end ELT pipeline runs with live progress tracking.

---

## Quickstart Guide

### 1. Clone & Set Up Environment
```bash
git clone https://github.com/ShriAmogh/Industrial-Sensor-ELT-Pipeline.git
cd Industrial-Sensor-ELT-Pipeline

# Create Python virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

### 2. Start Docker Infrastructure
```bash
# Spins up Airflow Webserver, Scheduler, and PostgreSQL Data Warehouse
docker compose up -d
```

* **Airflow UI**: [http://localhost:8080](http://localhost:8080) (Username: `admin` / Password: `admin`)
* **Interactive Dashboard UI**: [http://localhost:8000](http://localhost:8000)
* **PostgreSQL Warehouse**: `localhost:5433` (DB: `sensor_dw`, User: `dw_admin`, Pass: `dw_password123`)

### 3. Initialize & Run Pipeline
```bash
# 1. Download source dataset
python3 scripts/download_data.py

# 2. Run initial warehouse schema setup
python3 scripts/setup_dw.py

# 3. Ingest raw telemetry & run transformations
python3 pipelines/extractors/sensor_extractor.py
python3 pipelines/transformers/sql_runner.py
python3 pipelines/quality/validators.py

# 4. Launch Observability Dashboard
python3 scripts/run_dashboard.py
```

### 4. Trigger Orchestrated DAG in Airflow (Optional)
```bash
docker exec airflow_webserver airflow dags trigger industrial_sensor_elt_pipeline
```

---

## Sample SQL Analytics Queries

### 1. Statistical Anomaly Detection (Z-Score > 2.5)
```sql
WITH rolling_stats AS (
    SELECT
        f.udi, eq.product_id, eq.quality_grade, f.torque_nm,
        AVG(f.torque_nm) OVER (
            PARTITION BY eq.product_type 
            ORDER BY f.udi 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS rolling_mean,
        COALESCE(STDDEV(f.torque_nm) OVER (
            PARTITION BY eq.product_type 
            ORDER BY f.udi 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ), 0.01) AS rolling_std,
        f.torque_nm - LAG(f.torque_nm, 1, f.torque_nm) OVER (
            PARTITION BY eq.product_type ORDER BY f.udi
        ) AS step_delta
    FROM warehouse.fact_sensor_telemetry f
    JOIN warehouse.dim_equipment eq ON f.equipment_key = eq.equipment_key
)
SELECT udi, product_id, quality_grade, torque_nm, 
       ROUND(rolling_mean::NUMERIC, 2) AS rolling_mean, 
       ROUND(step_delta::NUMERIC, 2) AS step_delta, 
       ROUND(((torque_nm - rolling_mean) / rolling_std)::NUMERIC, 2) AS z_score
FROM rolling_stats
WHERE ABS((torque_nm - rolling_mean) / rolling_std) > 2.5
ORDER BY ABS(step_delta) DESC
LIMIT 5;
```

**Output:**
```
╒═══════╤══════════════╤════════════════════╤═════════════╤════════════════╤══════════════╤═══════════╕
│   udi │ product_id   │ quality_grade      │   torque_nm │   rolling_mean │   step_delta │   z_score │
╞═══════╪══════════════╪════════════════════╪═════════════╪════════════════╪══════════════╪═══════════╡
│   464 │ L47643       │ Low (50% variant)  │         4.2 │          40.44 │        -39.2 │     -2.53 │
│   848 │ L48027       │ Low (50% variant)  │         5.6 │          36.78 │        -35.9 │     -2.52 │
│  2637 │ L49816       │ Low (50% variant)  │        61.3 │          39.26 │         29.3 │      2.58 │
│  9940 │ H39353       │ High (20% variant) │        12.8 │          37.89 │        -20.4 │     -2.51 │
│  3313 │ L50492       │ Low (50% variant)  │        28.9 │          46.00 │        -15.8 │     -2.53 │
╘═══════╧══════════════╧════════════════════╧═════════════╧════════════════╧══════════════╧═══════════╛
```

### 2. Operating Speed Quintiles (`NTILE`) & Equipment Failure Rates
```sql
WITH speed_quintiles AS (
    SELECT f.udi, f.rotational_speed_rpm, f.is_failure, 
           NTILE(5) OVER (ORDER BY f.rotational_speed_rpm) AS speed_quintile
    FROM warehouse.fact_sensor_telemetry f
)
SELECT speed_quintile, MIN(rotational_speed_rpm) AS min_rpm, MAX(rotational_speed_rpm) AS max_rpm, 
       COUNT(*) AS total_readings, SUM(CASE WHEN is_failure THEN 1 ELSE 0 END) AS failures, 
       ROUND((100.0 * SUM(CASE WHEN is_failure THEN 1 ELSE 0 END) / COUNT(*))::NUMERIC, 2) AS failure_rate_pct
FROM speed_quintiles
GROUP BY speed_quintile
ORDER BY speed_quintile;
```

**Key Finding**: Low RPM operational zones ($< 1405\text{ RPM}$) experience **12.1% failure rates** due to extreme torque overstrain, whereas mid-range operating conditions ($1541 - 1644\text{ RPM}$) maintain high stability with only **0.45% failures**.

---

## Data Quality Framework & SLA Health

The validation suite in `pipelines/quality/validators.py` executes before analytical reporting is refreshed:

```
╒═════════════════════════════╤═══════════════════════╤═════════════════════════════════╤══════════════════════╤══════════╤═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╕
│ Check Name                  │ Category              │ Table                           │ Column               │ Status   │ Details                                                                                                                     │
╞═════════════════════════════╪═══════════════════════╪═════════════════════════════════╪══════════════════════╪══════════╪═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╡
│ check_row_count             │ Volume                │ raw.sensor_telemetry            │ -                    │ PASSED   │ Found 10,000 rows (expected >= 10,000)                                                                                      │
│ check_row_count             │ Volume                │ warehouse.fact_sensor_telemetry │ -                    │ PASSED   │ Found 10,000 rows (expected >= 10,000)                                                                                      │
│ check_null_percentage       │ Completeness          │ warehouse.fact_sensor_telemetry │ equipment_key        │ PASSED   │ 0 nulls out of 10,000 (0.00% nulls, threshold <= 0.0%)                                                                      │
│ check_numeric_range         │ Validity              │ warehouse.fact_sensor_telemetry │ rotational_speed_rpm │ PASSED   │ 0 out-of-bounds rows found. Allowed [1000, 3500].                                                                           │
│ check_numeric_range         │ Validity              │ warehouse.fact_sensor_telemetry │ torque_nm            │ PASSED   │ 0 out-of-bounds rows found. Allowed [1.0, 100.0].                                                                           │
│ check_referential_integrity │ Referential Integrity │ warehouse.fact_sensor_telemetry │ equipment_key        │ PASSED   │ 0 orphan records found in warehouse.fact_sensor_telemetry.equipment_key with no match in warehouse.dim_equipment.equipment_key │
╘═════════════════════════════╧═══════════════════════╧═════════════════════════════════╧══════════════════════╧══════════╧═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╛
```

---

## Author

**Amogh Arora**  
* GitHub: [github.com/ShriAmogh](https://github.com/ShriAmogh)
* Focus: Production Data Engineering, Machine Learning Pipelines & Distributed Systems
