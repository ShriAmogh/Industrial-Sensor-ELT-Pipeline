import os
import sys
import time
from typing import Optional, Dict, Any, List
from pathlib import Path
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, text

# Add workspace root to sys.path to allow importing from pipelines/
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from pipelines.extractors.sensor_extractor import ingest_sensor_data
from pipelines.transformers.sql_runner import execute_sql_file
from pipelines.quality.validators import DataQualityValidator

load_dotenv(ROOT_DIR / ".env")

app = FastAPI(
    title="Industrial Sensor ELT Pipeline Dashboard",
    description="Interactive monitoring and observability dashboard for Industrial Sensor ELT",
    version="1.0.0"
)

# Database connection setup
DW_USER = os.getenv("DW_POSTGRES_USER", "dw_admin")
DW_PASS = os.getenv("DW_POSTGRES_PASSWORD", "dw_password123")
DW_PORT = os.getenv("DW_HOST_PORT", "5433")
DW_DB = os.getenv("DW_POSTGRES_DB", "sensor_dw")
DW_HOST = "localhost"

DATABASE_URI = f"postgresql://{DW_USER}:{DW_PASS}@{DW_HOST}:{DW_PORT}/{DW_DB}"
engine = create_engine(DATABASE_URI, pool_pre_ping=True, pool_size=5, max_overflow=10)

STATIC_DIR = Path(__file__).resolve().parent / "static"


class QueryRequest(BaseModel):
    query: str
    limit: Optional[int] = 100


@app.get("/api/health")
def get_health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1;"))
        return {"status": "healthy", "database": "connected", "warehouse": DW_DB}
    except Exception as e:
        return {"status": "unhealthy", "database": "disconnected", "error": str(e)}


@app.get("/api/pipeline/summary")
def get_pipeline_summary():
    """Returns row counts and metadata across all layers of the ELT warehouse."""
    try:
        with engine.connect() as conn:
            raw_count = conn.execute(text("SELECT COUNT(*) FROM raw.sensor_telemetry;")).scalar() or 0
            staging_count = conn.execute(text("SELECT COUNT(*) FROM staging.stg_sensor_telemetry;")).scalar() or 0
            fact_count = conn.execute(text("SELECT COUNT(*) FROM warehouse.fact_sensor_telemetry;")).scalar() or 0
            equipment_count = conn.execute(text("SELECT COUNT(*) FROM warehouse.dim_equipment;")).scalar() or 0
            failure_dim_count = conn.execute(text("SELECT COUNT(*) FROM warehouse.dim_failure_mode;")).scalar() or 0
            
            # Anomaly count from view
            anomaly_count = conn.execute(text("""
                SELECT COUNT(*) FROM analytics.v_sensor_anomaly_features 
                WHERE is_torque_anomaly = TRUE;
            """)).scalar() or 0

            # Latest quality checks summary
            total_checks = conn.execute(text("SELECT COUNT(*) FROM quality.check_log;")).scalar() or 0
            latest_batch_checks = conn.execute(text("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE status = 'PASSED') as passed,
                    COUNT(*) FILTER (WHERE status = 'FAILED') as failed
                FROM quality.check_log
                WHERE checked_at >= (SELECT MAX(checked_at) - INTERVAL '5 minutes' FROM quality.check_log);
            """)).mappings().first()

            latest_batch = conn.execute(text("""
                SELECT batch_id, status, row_count, ingested_at 
                FROM raw.ingestion_log 
                ORDER BY ingested_at DESC LIMIT 1;
            """)).mappings().first()

        return {
            "counts": {
                "raw": raw_count,
                "staging": staging_count,
                "fact": fact_count,
                "dim_equipment": equipment_count,
                "dim_failure_mode": failure_dim_count,
                "anomalies": anomaly_count
            },
            "quality": {
                "total_historical_checks": total_checks,
                "latest_run": dict(latest_batch_checks) if latest_batch_checks else {"total": 0, "passed": 0, "failed": 0}
            },
            "last_ingestion": dict(latest_batch) if latest_batch else None,
            "architecture": {
                "paradigm": "ELT (Extract-Load-Transform)",
                "database": "PostgreSQL 16 (Warehouse Instance)",
                "orchestration": "Apache Airflow 2.9.3",
                "modeling": "Kimball Star Schema (Dim + Fact with Surrogate Keys)"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch summary: {str(e)}")


@app.get("/api/pipeline/layers/{layer_name}")
def get_layer_details(layer_name: str):
    """Provides deep structural details, rationale, and live sample rows for a specific layer."""
    layer_configs = {
        "raw": {
            "title": "Raw Layer (Bronze)",
            "table": "raw.sensor_telemetry",
            "description": "Exact 1:1 replica of source telemetry as ingested. Unmodified physics units (Kelvin), exact source types, enriched with ingestion audit metadata.",
            "why": "Preserves immutable source-of-truth. If downstream logic changes, staging can be recomputed without re-extracting from source. Supports idempotency and lineage tracking.",
            "sql_file": "sql/raw/01_create_raw_schema.sql",
            "query": "SELECT udi, product_id, air_temperature_k, process_temperature_k, rotational_speed_rpm, torque_nm, tool_wear_min, machine_failure, batch_id, ingested_at FROM raw.sensor_telemetry ORDER BY udi ASC LIMIT 15;"
        },
        "staging": {
            "title": "Staging Layer (Silver)",
            "table": "staging.stg_sensor_telemetry",
            "description": "Cleaned, standardized, and enriched dataset. Converts temperatures from Kelvin to Celsius, computes mechanical power (kW) and overstrain fatigue metrics, maps failure flags to human-readable labels.",
            "why": "Domain feature engineering layer. Decouples raw ingestion schema from analytical consumers. Calculates physics formulas inside PostgreSQL for speed.",
            "sql_file": "sql/transforms/01_load_staging.sql",
            "query": "SELECT udi, product_id, product_type, product_quality_grade, air_temp_c, process_temp_c, temp_differential_c, rotational_speed_rpm, torque_nm, mechanical_power_kw, tool_wear_min, overstrain_metric, failure_mode FROM staging.stg_sensor_telemetry ORDER BY udi ASC LIMIT 15;"
        },
        "warehouse_fact": {
            "title": "Warehouse Fact Table (Gold)",
            "table": "warehouse.fact_sensor_telemetry",
            "description": "High-density measurement table referencing Dimension tables via integer surrogate keys (equipment_key, failure_mode_key). Contains all continuous numerical metrics.",
            "why": "Dimensional modeling for lightning-fast aggregation and slicing. Integer surrogate keys optimize join performance over string natural keys.",
            "sql_file": "sql/transforms/02_build_star_schema.sql",
            "query": "SELECT telemetry_key, udi, equipment_key, failure_mode_key, air_temp_c, temp_differential_c, rotational_speed_rpm, torque_nm, mechanical_power_kw, tool_wear_min, overstrain_metric, is_failure FROM warehouse.fact_sensor_telemetry ORDER BY telemetry_key ASC LIMIT 15;"
        },
        "warehouse_dim_equipment": {
            "title": "Dimension: Equipment",
            "table": "warehouse.dim_equipment",
            "description": "Conformed dimension tracking distinct machines, product types (L, M, H), quality grades (Low, Medium, High), and metadata updates.",
            "why": "Normalizes equipment attributes to prevent repetitive text storage across millions of fact records. Enables clean slicing by machine category.",
            "sql_file": "sql/transforms/02_build_star_schema.sql",
            "query": "SELECT equipment_key, product_id, product_type, quality_grade, created_at, updated_at FROM warehouse.dim_equipment ORDER BY equipment_key ASC LIMIT 15;"
        },
        "warehouse_dim_failure_mode": {
            "title": "Dimension: Failure Mode",
            "table": "warehouse.dim_failure_mode",
            "description": "Domain-specific dimension classifying failure categories (No Failure, Heat Dissipation, Power Failure, Overstrain, Tool Wear, Random Failure) with severity rankings.",
            "why": "Enables categorical failure drilldowns and root-cause analysis with standardized severity definitions.",
            "sql_file": "sql/warehouse/01_create_warehouse_schema.sql",
            "query": "SELECT failure_mode_key, failure_mode_name, severity_level, root_cause_category FROM warehouse.dim_failure_mode ORDER BY failure_mode_key ASC;"
        },
        "analytics": {
            "title": "Analytics Views (Gold Feature Layer)",
            "table": "analytics.v_sensor_anomaly_features",
            "description": "Live feature views powered by SQL Window Functions (AVG OVER, STDDEV OVER, LAG, DENSE_RANK). Computes dynamic Z-Scores for statistical anomaly detection.",
            "why": "Zero-storage, always-fresh views ready for BI dashboards and Machine Learning inference. Pushes heavy time-series math down to the database engine.",
            "sql_file": "sql/analytics/01_create_analytics_views.sql",
            "query": "SELECT udi, product_id, product_type, rotational_speed_rpm, torque_nm, rolling_avg_torque, rolling_std_torque, torque_step_delta, torque_z_score, is_torque_anomaly, failure_mode_name FROM analytics.v_sensor_anomaly_features ORDER BY udi ASC LIMIT 15;"
        },
        "quality": {
            "title": "Quality Governance & Audit Log",
            "table": "quality.check_log",
            "description": "Persistent audit trail recording automated data quality assertions (Volume, Null Checks, Referential Integrity, Physical Range Checks).",
            "why": "Circuit breaker mechanism: If critical checks fail, the pipeline halts before bad data reaches reporting. Provides SLA transparency.",
            "sql_file": "pipelines/quality/validators.py",
            "query": "SELECT check_id, batch_id, check_name, check_category, table_name, column_name, status, observed_value, threshold_value, severity, details, checked_at FROM quality.check_log ORDER BY checked_at DESC, check_id DESC LIMIT 20;"
        }
    }

    if layer_name not in layer_configs:
        raise HTTPException(status_code=404, detail=f"Layer '{layer_name}' not found. Valid: {list(layer_configs.keys())}")

    cfg = layer_configs[layer_name]
    try:
        with engine.connect() as conn:
            # Fetch sample rows
            result = conn.execute(text(cfg["query"]))
            columns = list(result.keys())
            rows = [dict(row) for row in result.mappings()]
            
            # Fetch table column definitions if it's a table or view
            schema_name, table_name = cfg["table"].split(".")
            cols_info = conn.execute(text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table
                ORDER BY ordinal_position;
            """), {"schema": schema_name, "table": table_name}).mappings().all()

        return {
            "layer": layer_name,
            "title": cfg["title"],
            "table": cfg["table"],
            "description": cfg["description"],
            "why": cfg["why"],
            "sql_file": cfg["sql_file"],
            "schema_columns": [dict(c) for c in cols_info],
            "sample_columns": columns,
            "sample_rows": rows
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query layer details: {str(e)}")


@app.get("/api/analytics/anomalies")
def get_anomalies(limit: int = 50, anomaly_only: bool = True):
    """Returns sensor telemetry records enriched with rolling averages, lag deltas, and Z-score anomalies."""
    try:
        where_clause = "WHERE is_torque_anomaly = TRUE" if anomaly_only else ""
        query = f"""
            SELECT 
                udi,
                product_id,
                product_type,
                quality_grade,
                rotational_speed_rpm,
                torque_nm,
                temp_differential_c,
                mechanical_power_kw,
                tool_wear_min,
                failure_mode_name,
                is_failure,
                rolling_avg_torque,
                rolling_std_torque,
                torque_step_delta,
                torque_z_score,
                is_torque_anomaly
            FROM analytics.v_sensor_anomaly_features
            {where_clause}
            ORDER BY ABS(torque_z_score) DESC, udi ASC
            LIMIT :limit;
        """
        with engine.connect() as conn:
            result = conn.execute(text(query), {"limit": limit})
            rows = [dict(r) for r in result.mappings()]
        return {"count": len(rows), "records": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query anomalies: {str(e)}")


@app.get("/api/analytics/failures")
def get_failure_summary():
    """Returns aggregated failure root cause analysis by machine type."""
    try:
        query = """
            SELECT 
                product_type,
                quality_grade,
                failure_mode_name,
                occurrence_count,
                avg_temp_diff_c,
                avg_rpm,
                avg_torque_nm,
                avg_power_kw,
                failure_rank_in_tier,
                failure_pct_share
            FROM analytics.v_failure_root_cause_summary
            ORDER BY product_type ASC, failure_rank_in_tier ASC;
        """
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = [dict(r) for r in result.mappings()]
        return {"count": len(rows), "records": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query failure summary: {str(e)}")


@app.get("/api/analytics/high-risk")
def get_high_risk_leaderboard():
    """Returns top high-risk operating units ranked by overstrain mechanical metric."""
    try:
        query = """
            SELECT 
                risk_rank,
                udi,
                product_id,
                quality_grade,
                overstrain_metric,
                tool_wear_min,
                torque_nm,
                temp_differential_c,
                mechanical_power_kw,
                failure_mode_name,
                is_failure
            FROM analytics.v_high_risk_equipment_leaderboard
            ORDER BY risk_rank ASC
            LIMIT 25;
        """
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = [dict(r) for r in result.mappings()]
        return {"count": len(rows), "records": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query high risk leaderboard: {str(e)}")


@app.get("/api/quality/checks")
def get_quality_checks():
    """Returns recent quality checks and overall governance pass/fail rate."""
    try:
        query = """
            SELECT 
                check_id,
                batch_id,
                check_name,
                check_category,
                table_name,
                column_name,
                status,
                observed_value,
                threshold_value,
                severity,
                details,
                checked_at
            FROM quality.check_log
            ORDER BY checked_at DESC, check_id DESC
            LIMIT 50;
        """
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = [dict(r) for r in result.mappings()]
            
            # Aggregate stats
            stats = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_checks,
                    COUNT(*) FILTER (WHERE status = 'PASSED') as total_passed,
                    COUNT(*) FILTER (WHERE status = 'FAILED') as total_failed
                FROM quality.check_log;
            """)).mappings().first()

        return {
            "stats": dict(stats) if stats else {"total_checks": 0, "total_passed": 0, "total_failed": 0},
            "recent_checks": rows
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch quality checks: {str(e)}")


@app.post("/api/query/run")
def run_custom_query(req: QueryRequest):
    """Executes safe read-only SQL queries with timing benchmarks."""
    cleaned = req.query.strip().rstrip(";")
    upper = cleaned.upper()
    
    # Simple safety guard: disallow destructive DDL/DML in sandbox
    forbidden = ["DROP", "TRUNCATE", "DELETE", "UPDATE", "INSERT", "ALTER", "GRANT", "REVOKE"]
    for keyword in forbidden:
        # Check whole word keyword
        if f" {keyword} " in f" {upper} " or upper.startswith(f"{keyword} "):
            raise HTTPException(
                status_code=400, 
                detail=f"Security Guard: Write or modification keyword '{keyword}' is not permitted in read-only sandbox mode."
            )

    start_time = time.perf_counter()
    try:
        with engine.connect() as conn:
            # Enforce limit if not already limited
            if "LIMIT" not in upper:
                cleaned = f"{cleaned} LIMIT {req.limit}"
                
            result = conn.execute(text(cleaned))
            columns = list(result.keys()) if result.returns_rows else []
            rows = [dict(r) for r in result.mappings()] if result.returns_rows else []
            
        elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)
        return {
            "status": "success",
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "execution_time_ms": elapsed_ms,
            "query": cleaned
        }
    except Exception as e:
        elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)
        return {
            "status": "error",
            "error": str(e),
            "execution_time_ms": elapsed_ms,
            "query": cleaned
        }


@app.post("/api/pipeline/run-sync")
def trigger_pipeline_sync():
    """Runs the end-to-end ELT sequence synchronously and returns execution logs."""
    logs = []
    start_all = time.perf_counter()
    csv_path = ROOT_DIR / "data" / "raw" / "ai4i2020.csv"
    if not csv_path.exists():
        csv_path = ROOT_DIR / "data" / "ai4i2020.csv"
    
    if not csv_path.exists():
        raise HTTPException(status_code=400, detail=f"Data file not found in data/raw/ai4i2020.csv or data/ai4i2020.csv. Run download script first.")

    try:
        # Step 1: Extraction
        t0 = time.perf_counter()
        batch_id = ingest_sensor_data(csv_path)
        logs.append({
            "stage": "1. Extract & Raw Ingestion",
            "status": "SUCCESS",
            "details": f"Upserted source records into raw.sensor_telemetry (Batch: {batch_id})",
            "duration_s": round(time.perf_counter() - t0, 2)
        })

        # Step 2: Staging Transform
        t0 = time.perf_counter()
        execute_sql_file(ROOT_DIR / "sql" / "transforms" / "01_load_staging.sql")
        logs.append({
            "stage": "2. Staging Enrichment",
            "status": "SUCCESS",
            "details": "Computed Celsius, Mechanical Power (kW), Overstrain, and Failure labels",
            "duration_s": round(time.perf_counter() - t0, 2)
        })

        # Step 3: Warehouse Star Schema
        t0 = time.perf_counter()
        execute_sql_file(ROOT_DIR / "sql" / "transforms" / "02_build_star_schema.sql")
        logs.append({
            "stage": "3. Warehouse Star Schema",
            "status": "SUCCESS",
            "details": "Populated dim_equipment and fact_sensor_telemetry with surrogate keys",
            "duration_s": round(time.perf_counter() - t0, 2)
        })

        # Step 4: Analytics Views
        t0 = time.perf_counter()
        execute_sql_file(ROOT_DIR / "sql" / "analytics" / "01_create_analytics_views.sql")
        logs.append({
            "stage": "4. Analytics Views Refresh",
            "status": "SUCCESS",
            "details": "Refreshed window functions, rolling averages, Z-Scores, and leaderboard views",
            "duration_s": round(time.perf_counter() - t0, 2)
        })

        # Step 5: Data Quality Suite
        t0 = time.perf_counter()
        validator = DataQualityValidator()
        validator.run_full_suite(batch_id=batch_id)
        passed_count = len([r for r in validator.results if r.status == "PASSED"])
        total_count = len(validator.results)
        logs.append({
            "stage": "5. Data Quality Gates",
            "status": "SUCCESS",
            "details": f"Evaluated {total_count} quality rules ({passed_count}/{total_count} passed)",
            "duration_s": round(time.perf_counter() - t0, 2)
        })

        total_duration = round(time.perf_counter() - start_all, 2)
        return {
            "status": "success",
            "batch_id": batch_id,
            "total_duration_s": total_duration,
            "steps": logs
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "steps": logs
        }


# Mount static assets
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
def serve_index():
    index_file = STATIC_DIR / "index.html"
    if index_file.exists():
        return FileResponse(str(index_file))
    return JSONResponse({"message": "Industrial Sensor ELT Pipeline API is active. Static UI not yet compiled."})
