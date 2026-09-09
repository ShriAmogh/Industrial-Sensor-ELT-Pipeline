"""
Industrial Sensor Telemetry Extractor & Ingestion Module
Handles loading AI4I 2020 Predictive Maintenance data into raw.sensor_telemetry with batch auditing.
"""

import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

RAW_COLUMN_MAP = {
    "UDI": "udi",
    "Product ID": "product_id",
    "Type": "product_type",
    "Air temperature [K]": "air_temperature_k",
    "Process temperature [K]": "process_temperature_k",
    "Rotational speed [rpm]": "rotational_speed_rpm",
    "Torque [Nm]": "torque_nm",
    "Tool wear [min]": "tool_wear_min",
    "Machine failure": "machine_failure",
    "TWF": "twf",
    "HDF": "hdf",
    "PWF": "pwf",
    "OSF": "osf",
    "RNF": "rnf"
}

def get_db_connection(is_airflow_container: bool = False):
    """Establishes connection to the target PostgreSQL Data Warehouse."""
    if is_airflow_container or os.getenv("RUNNING_IN_DOCKER"):
        host = os.getenv("DW_POSTGRES_HOST", "pipeline-db")
        port = int(os.getenv("DW_POSTGRES_PORT", "5432"))
    else:
        host = os.getenv("DW_POSTGRES_HOST_LOCAL", "localhost")
        port = int(os.getenv("DW_HOST_PORT", "5433"))

    return psycopg2.connect(
        dbname=os.getenv("DW_POSTGRES_DB", "sensor_dw"),
        user=os.getenv("DW_POSTGRES_USER", "dw_admin"),
        password=os.getenv("DW_POSTGRES_PASSWORD", "dw_password123"),
        host=host,
        port=port
    )

def ingest_sensor_data(file_path: Path, is_airflow_container: bool = False) -> str:
    """
    Ingests CSV telemetry into raw.sensor_telemetry and logs the batch run.
    Ensures idempotency using ON CONFLICT (udi) DO UPDATE or DO NOTHING.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Source file not found at: {file_path}")

    batch_id = f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    source_name = file_path.name

    print(f"[{batch_id}] Loading telemetry from {source_name}...")
    
    df = pd.read_csv(file_path)
    df = df.rename(columns=RAW_COLUMN_MAP)
    total_rows = len(df)

    # Lineage and audit metadata
    now_utc = datetime.now(timezone.utc)
    df["source_dataset"] = source_name
    df["batch_id"] = batch_id
    df["ingested_at"] = now_utc

    conn = get_db_connection(is_airflow_container=is_airflow_container)
    cursor = conn.cursor()

    try:
        # 1. Audit log start
        cursor.execute(
            """
            INSERT INTO raw.ingestion_log (batch_id, source_dataset, row_count, status, ingested_at)
            VALUES (%s, %s, %s, %s, %s);
            """,
            (batch_id, source_name, total_rows, "IN_PROGRESS", now_utc)
        )
        conn.commit()

        # 2. Bulk upsert into raw.sensor_telemetry
        cols = list(RAW_COLUMN_MAP.values()) + ["source_dataset", "batch_id", "ingested_at"]
        records = [tuple(x) for x in df[cols].to_numpy()]

        insert_query = f"""
            INSERT INTO raw.sensor_telemetry ({', '.join(cols)})
            VALUES %s
            ON CONFLICT (udi) DO UPDATE SET
                air_temperature_k = EXCLUDED.air_temperature_k,
                process_temperature_k = EXCLUDED.process_temperature_k,
                rotational_speed_rpm = EXCLUDED.rotational_speed_rpm,
                torque_nm = EXCLUDED.torque_nm,
                tool_wear_min = EXCLUDED.tool_wear_min,
                machine_failure = EXCLUDED.machine_failure,
                batch_id = EXCLUDED.batch_id,
                ingested_at = EXCLUDED.ingested_at;
        """
        
        execute_values(cursor, insert_query, records, page_size=2000)

        # 3. Audit log success
        cursor.execute(
            """
            UPDATE raw.ingestion_log 
            SET status = 'SUCCESS' 
            WHERE batch_id = %s;
            """,
            (batch_id,)
        )
        conn.commit()
        print(f"[{batch_id}] Ingestion completed: {total_rows} records loaded successfully.")
        return batch_id

    except Exception as e:
        conn.rollback()
        cursor.execute(
            """
            UPDATE raw.ingestion_log 
            SET status = 'FAILED', error_message = %s 
            WHERE batch_id = %s;
            """,
            (str(e), batch_id)
        )
        conn.commit()
        raise e
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    csv_path = BASE_DIR / "data" / "raw" / "ai4i2020.csv"
    ingest_sensor_data(csv_path)
