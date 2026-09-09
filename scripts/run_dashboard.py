#!/usr/bin/env python3
"""
Industrial Sensor ELT Pipeline - Dashboard Launcher
Starts the interactive observability dashboard on port 8000.
"""

import sys
import uvicorn
from pathlib import Path

# Add project root to sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

def main():
    print("=" * 70)
    print("⚡ Starting Industrial Sensor ELT Pipeline Observability Dashboard")
    print("   Connected to: PostgreSQL 16 (sensor_dw on port 5433)")
    print("   Dashboard URL: http://localhost:8000")
    print("=" * 70)
    
    uvicorn.run(
        "dashboard.api:app",
        host="0.0.0.0",
        port=8000,
        log_level="info",
        reload=False
    )

if __name__ == "__main__":
    main()
