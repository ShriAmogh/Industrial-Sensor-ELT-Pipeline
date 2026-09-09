"""
Automated Data Quality Framework & Assertion Suite
Performs validation gates across staging and warehouse layers, logging all results to quality.check_log.
"""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, List
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from tabulate import tabulate

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

class DataQualityException(Exception):
    """Raised when one or more CRITICAL quality assertions fail."""
    pass

@dataclass
class QualityResult:
    check_name: str
    category: str
    table_name: str
    column_name: Optional[str]
    status: str
    observed_value: float
    threshold_value: float
    severity: str
    details: str

class DataQualityValidator:
    def __init__(self, is_airflow_container: bool = False):
        if is_airflow_container or os.getenv("RUNNING_IN_DOCKER"):
            host = os.getenv("DW_POSTGRES_HOST", "pipeline-db")
            port = os.getenv("DW_POSTGRES_PORT", "5432")
        else:
            host = os.getenv("DW_POSTGRES_HOST_LOCAL", "localhost")
            port = os.getenv("DW_HOST_PORT", "5433")

        user = os.getenv("DW_POSTGRES_USER", "dw_admin")
        password = os.getenv("DW_POSTGRES_PASSWORD", "dw_password123")
        db = os.getenv("DW_POSTGRES_DB", "sensor_dw")

        db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
        self.engine = create_engine(db_url)
        self.results: List[QualityResult] = []

    def log_result_to_db(self, res: QualityResult, batch_id: Optional[str] = None):
        """Persists validation result to quality.check_log table."""
        insert_sql = text("""
            INSERT INTO quality.check_log (
                batch_id, check_name, check_category, table_name, column_name,
                status, observed_value, threshold_value, severity, details, checked_at
            ) VALUES (
                :batch_id, :check_name, :category, :table_name, :column_name,
                :status, :observed_value, :threshold_value, :severity, :details, :checked_at
            );
        """)
        with self.engine.begin() as conn:
            conn.execute(insert_sql, {
                "batch_id": batch_id,
                "check_name": res.check_name,
                "category": res.category,
                "table_name": res.table_name,
                "column_name": res.column_name,
                "status": res.status,
                "observed_value": res.observed_value,
                "threshold_value": res.threshold_value,
                "severity": res.severity,
                "details": res.details,
                "checked_at": datetime.now(timezone.utc)
            })

    def check_row_count(self, table: str, min_expected: int, severity: str = "CRITICAL", batch_id: Optional[str] = None):
        """Validates that table contains at least min_expected rows."""
        query = text(f"SELECT COUNT(*) FROM {table};")
        with self.engine.connect() as conn:
            actual_count = conn.execute(query).scalar()

        status = "PASSED" if actual_count >= min_expected else "FAILED"
        details = f"Found {actual_count:,} rows (expected >= {min_expected:,})"
        res = QualityResult("check_row_count", "Volume", table, None, status, float(actual_count), float(min_expected), severity, details)
        self.results.append(res)
        self.log_result_to_db(res, batch_id)

    def check_null_percentage(self, table: str, column: str, max_allowed_pct: float = 0.0, severity: str = "CRITICAL", batch_id: Optional[str] = None):
        """Validates that null percentage in column is <= max_allowed_pct."""
        query = text(f"""
            SELECT 
                COUNT(*) AS total,
                COUNT(CASE WHEN {column} IS NULL THEN 1 END) AS null_count
            FROM {table};
        """)
        with self.engine.connect() as conn:
            row = conn.execute(query).fetchone()
            total, null_count = row[0], row[1]

        null_pct = (null_count / total * 100.0) if total > 0 else 0.0
        status = "PASSED" if null_pct <= max_allowed_pct else "FAILED"
        details = f"{null_count:,} nulls out of {total:,} ({null_pct:.2f}% nulls, threshold <= {max_allowed_pct}%)"
        res = QualityResult("check_null_percentage", "Completeness", table, column, status, round(null_pct, 4), max_allowed_pct, severity, details)
        self.results.append(res)
        self.log_result_to_db(res, batch_id)

    def check_numeric_range(self, table: str, column: str, min_val: float, max_val: float, severity: str = "CRITICAL", batch_id: Optional[str] = None):
        """Asserts that all numeric values in a column fall within physical bounds."""
        query = text(f"""
            SELECT 
                COUNT(*) AS out_of_bounds,
                MIN({column}) AS min_found,
                MAX({column}) AS max_found
            FROM {table}
            WHERE {column} < :min_val OR {column} > :max_val;
        """)
        with self.engine.connect() as conn:
            row = conn.execute(query, {"min_val": min_val, "max_val": max_val}).fetchone()
            bad_count, min_found, max_found = row[0], row[1], row[2]

        status = "PASSED" if bad_count == 0 else "FAILED"
        details = f"{bad_count} out-of-bounds rows found. Allowed [{min_val}, {max_val}]."
        res = QualityResult("check_numeric_range", "Validity", table, column, status, float(bad_count), 0.0, severity, details)
        self.results.append(res)
        self.log_result_to_db(res, batch_id)

    def check_referential_integrity(self, fact_table: str, fact_fk: str, dim_table: str, dim_pk: str, severity: str = "CRITICAL", batch_id: Optional[str] = None):
        """Asserts that no orphan foreign keys exist in fact table."""
        query = text(f"""
            SELECT COUNT(*)
            FROM {fact_table} f
            LEFT JOIN {dim_table} d ON f.{fact_fk} = d.{dim_pk}
            WHERE d.{dim_pk} IS NULL;
        """)
        with self.engine.connect() as conn:
            orphans = conn.execute(query).scalar()

        status = "PASSED" if orphans == 0 else "FAILED"
        details = f"{orphans} orphan records found in {fact_table}.{fact_fk} with no match in {dim_table}.{dim_pk}"
        res = QualityResult("check_referential_integrity", "Referential Integrity", fact_table, fact_fk, status, float(orphans), 0.0, severity, details)
        self.results.append(res)
        self.log_result_to_db(res, batch_id)

    def run_full_suite(self, batch_id: Optional[str] = None) -> bool:
        """Executes all data quality gates."""
        print(f"\n🧪 [Data Quality Gate] Running validation suite...")
        self.results = []

        # 1. Volume Checks
        self.check_row_count("raw.sensor_telemetry", min_expected=10000, batch_id=batch_id)
        self.check_row_count("warehouse.fact_sensor_telemetry", min_expected=10000, batch_id=batch_id)
        self.check_row_count("warehouse.dim_equipment", min_expected=1000, batch_id=batch_id)

        # 2. Completeness (Null Checks)
        self.check_null_percentage("warehouse.fact_sensor_telemetry", "equipment_key", max_allowed_pct=0.0, batch_id=batch_id)
        self.check_null_percentage("warehouse.fact_sensor_telemetry", "torque_nm", max_allowed_pct=0.0, batch_id=batch_id)
        self.check_null_percentage("warehouse.fact_sensor_telemetry", "rotational_speed_rpm", max_allowed_pct=0.0, batch_id=batch_id)

        # 3. Physical Boundary Checks (Sanity Checks)
        # Rotational speed must be positive and realistic (1000 to 3500 RPM)
        self.check_numeric_range("warehouse.fact_sensor_telemetry", "rotational_speed_rpm", min_val=1000, max_val=3500, batch_id=batch_id)
        # Torque must be between 1 and 100 Nm
        self.check_numeric_range("warehouse.fact_sensor_telemetry", "torque_nm", min_val=1.0, max_val=100.0, batch_id=batch_id)
        # Temperature in Celsius should be reasonable operating temp (10°C to 50°C)
        self.check_numeric_range("warehouse.fact_sensor_telemetry", "air_temp_c", min_val=10.0, max_val=50.0, batch_id=batch_id)
        # Tool wear cannot be negative
        self.check_numeric_range("warehouse.fact_sensor_telemetry", "tool_wear_min", min_val=0, max_val=300, batch_id=batch_id)

        # 4. Referential Integrity Checks
        self.check_referential_integrity("warehouse.fact_sensor_telemetry", "equipment_key", "warehouse.dim_equipment", "equipment_key", batch_id=batch_id)
        self.check_referential_integrity("warehouse.fact_sensor_telemetry", "failure_mode_key", "warehouse.dim_failure_mode", "failure_mode_key", batch_id=batch_id)

        # Display Summary Table
        table_data = [
            [r.check_name, r.category, r.table_name, r.column_name or "-", r.status, r.details]
            for r in self.results
        ]
        headers = ["Check Name", "Category", "Table", "Column", "Status", "Details"]
        print(tabulate(table_data, headers=headers, tablefmt="fancy_grid"))

        critical_failures = [r for r in self.results if r.status == "FAILED" and r.severity == "CRITICAL"]
        if critical_failures:
            failed_msgs = "; ".join([f"{f.check_name} on {f.table_name}" for f in critical_failures])
            raise DataQualityException(f"🚨 Pipeline halted! {len(critical_failures)} CRITICAL quality assertion(s) failed: {failed_msgs}")

        print("✅ All Data Quality Gates PASSED!\n")
        return True

if __name__ == "__main__":
    validator = DataQualityValidator()
    validator.run_full_suite()
