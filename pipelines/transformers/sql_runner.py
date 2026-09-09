"""
SQL Transformation Runner Module
Executes sequential SQL transformations (Raw -> Staging -> Warehouse -> Analytics) with execution logging.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

def get_engine(is_airflow_container: bool = False):
    """Creates a SQLAlchemy engine for the PostgreSQL Data Warehouse."""
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
    return create_engine(db_url)

def execute_sql_file(file_path: Path, is_airflow_container: bool = False):
    """Executes a given SQL file inside a transaction."""
    if not file_path.exists():
        raise FileNotFoundError(f"SQL file not found: {file_path}")

    print(f"Executing: {file_path.name}...")
    with open(file_path, "r") as f:
        sql_commands = f.read()

    engine = get_engine(is_airflow_container=is_airflow_container)
    with engine.begin() as conn:
        conn.execute(text(sql_commands))
    print(f"✓ Completed {file_path.name}")

def run_all_transformations(is_airflow_container: bool = False):
    """Runs all schema creations and transformations in sequence."""
    steps = [
        BASE_DIR / "sql" / "staging" / "01_create_staging_schema.sql",
        BASE_DIR / "sql" / "transforms" / "01_load_staging.sql",
        BASE_DIR / "sql" / "warehouse" / "01_create_warehouse_schema.sql",
        BASE_DIR / "sql" / "transforms" / "02_build_star_schema.sql",
        BASE_DIR / "sql" / "analytics" / "01_create_analytics_views.sql",
    ]

    print("\n--- Starting Full ELT SQL Transformation ---")
    for step in steps:
        execute_sql_file(step, is_airflow_container=is_airflow_container)
    print("--- All Transformations Completed Successfully! ---\n")

if __name__ == "__main__":
    run_all_transformations()
