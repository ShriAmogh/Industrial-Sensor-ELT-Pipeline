"""
Industrial Sensor ELT Pipeline - Master Orchestration DAG
Orchestrates raw telemetry ingestion, SQL transformations, star schema loading, and quality assertions.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments inherited by all tasks
default_args = {
    "owner": "amogh_arora",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

def task_extract_raw(**context):
    """Ingests raw CSV sensor telemetry into raw.sensor_telemetry."""
    from pipelines.extractors.sensor_extractor import ingest_sensor_data
    
    # Path inside Airflow Docker container
    csv_path = Path("/opt/airflow/data/raw/ai4i2020.csv")
    batch_id = ingest_sensor_data(csv_path, is_airflow_container=True)
    
    # Push batch_id to XCom for downstream lineage
    context["ti"].xcom_push(key="batch_id", value=batch_id)
    print(f"Extraction task completed for batch: {batch_id}")
    return batch_id

def task_transform_staging(**context):
    """Executes staging schema creation and transformation."""
    from pipelines.transformers.sql_runner import execute_sql_file
    
    execute_sql_file(Path("/opt/airflow/sql/staging/01_create_staging_schema.sql"), is_airflow_container=True)
    execute_sql_file(Path("/opt/airflow/sql/transforms/01_load_staging.sql"), is_airflow_container=True)
    print("Staging transformation completed.")

def task_build_star_schema(**context):
    """Executes dimensional warehouse schema creation and fact/dimension loading."""
    from pipelines.transformers.sql_runner import execute_sql_file
    
    execute_sql_file(Path("/opt/airflow/sql/warehouse/01_create_warehouse_schema.sql"), is_airflow_container=True)
    execute_sql_file(Path("/opt/airflow/sql/transforms/02_build_star_schema.sql"), is_airflow_container=True)
    print("Star schema build completed.")

def task_build_analytics_views(**context):
    """Builds analytical rolling metric views and anomaly detection logic."""
    from pipelines.transformers.sql_runner import execute_sql_file
    
    execute_sql_file(Path("/opt/airflow/sql/analytics/01_create_analytics_views.sql"), is_airflow_container=True)
    print("Analytics views refreshed.")

def task_run_quality_gates(**context):
    """Executes automated quality assertions with circuit breaker."""
    from pipelines.quality.validators import DataQualityValidator
    
    batch_id = context["ti"].xcom_pull(key="batch_id", task_ids="extract_raw_telemetry")
    
    # Ensure quality schema exists
    from pipelines.transformers.sql_runner import execute_sql_file
    execute_sql_file(Path("/opt/airflow/sql/quality/01_create_quality_schema.sql"), is_airflow_container=True)
    
    validator = DataQualityValidator(is_airflow_container=True)
    validator.run_full_suite(batch_id=batch_id)
    print("Data quality gates successfully passed!")

def task_publish_summary(**context):
    """Summarizes pipeline metrics for observability."""
    from pipelines.transformers.sql_runner import get_engine
    import pandas as pd
    
    engine = get_engine(is_airflow_container=True)
    with engine.connect() as conn:
        counts = pd.read_sql("""
            SELECT 
                (SELECT COUNT(*) FROM raw.sensor_telemetry) AS raw_rows,
                (SELECT COUNT(*) FROM warehouse.fact_sensor_telemetry) AS fact_rows,
                (SELECT COUNT(*) FROM warehouse.fact_sensor_telemetry WHERE is_failure = TRUE) AS failure_rows,
                (SELECT COUNT(*) FROM quality.check_log WHERE status = 'PASSED') AS passed_quality_checks
        """, conn)
        
    print("\n================ PIPELINE RUN SUMMARY ================")
    print(counts.to_string(index=False))
    print("======================================================\n")

# DAG Definition
with DAG(
    dag_id="industrial_sensor_elt_pipeline",
    default_args=default_args,
    description="End-to-End Industrial Sensor ELT Pipeline with Quality Gates and Star Schema",
    schedule_interval="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["sensor", "elt", "predictive_maintenance"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_raw_telemetry",
        python_callable=task_extract_raw,
    )

    staging_task = PythonOperator(
        task_id="transform_to_staging",
        python_callable=task_transform_staging,
    )

    star_schema_task = PythonOperator(
        task_id="build_star_schema",
        python_callable=task_build_star_schema,
    )

    analytics_views_task = PythonOperator(
        task_id="build_analytics_views",
        python_callable=task_build_analytics_views,
    )

    quality_gates_task = PythonOperator(
        task_id="data_quality_gates",
        python_callable=task_run_quality_gates,
    )

    summary_task = PythonOperator(
        task_id="publish_health_summary",
        python_callable=task_publish_summary,
    )

    # Define Linear Task Dependencies
    extract_task >> staging_task >> star_schema_task >> analytics_views_task >> quality_gates_task >> summary_task
