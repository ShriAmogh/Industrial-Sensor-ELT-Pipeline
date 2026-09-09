"""
Database Initialization Script
Executes schema creation scripts against the target PostgreSQL data warehouse.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load environment variables
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# Database credentials
DB_USER = os.getenv("DW_POSTGRES_USER", "dw_admin")
DB_PASS = os.getenv("DW_POSTGRES_PASSWORD", "dw_password123")
DB_HOST = os.getenv("DW_POSTGRES_HOST_LOCAL", "localhost")
DB_PORT = os.getenv("DW_HOST_PORT", "5433")
DB_NAME = os.getenv("DW_POSTGRES_DB", "sensor_dw")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def run_sql_file(engine, file_path: Path):
    print(f"Executing: {file_path.name} ...")
    with open(file_path, "r") as f:
        sql_content = f.read()
    
    with engine.connect() as conn:
        conn.execute(text(sql_content))
        conn.commit()
    print(f"Successfully executed {file_path.name}")

def main():
    print(f"Connecting to Data Warehouse at {DB_HOST}:{DB_PORT}/{DB_NAME}...")
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();")).fetchone()
            print(f"Connected to PostgreSQL: {result[0]}")
            
        # Run raw schema
        raw_sql_path = BASE_DIR / "sql" / "raw" / "01_create_raw_schema.sql"
        if raw_sql_path.exists():
            run_sql_file(engine, raw_sql_path)
            
        print("Data Warehouse initialization complete!")
    except Exception as e:
        print(f"Error connecting or running setup: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
