-- =============================================================================
-- RAW SCHEMA & TELEMETRY TABLES
-- Objective: Land source industrial telemetry data exactly as-is with metadata.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS raw;

-- 1. Raw Ingestion Log Table (Batch audit trail)
CREATE TABLE IF NOT EXISTS raw.ingestion_log (
    batch_id VARCHAR(100) PRIMARY KEY,
    source_dataset VARCHAR(100) NOT NULL,
    row_count INT NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('SUCCESS', 'FAILED', 'IN_PROGRESS')),
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT
);

-- 2. Raw Industrial Sensor Telemetry Table
CREATE TABLE IF NOT EXISTS raw.sensor_telemetry (
    udi INT PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    product_type CHAR(1) NOT NULL,
    air_temperature_k NUMERIC(8, 2),
    process_temperature_k NUMERIC(8, 2),
    rotational_speed_rpm INT,
    torque_nm NUMERIC(8, 2),
    tool_wear_min INT,
    machine_failure INT,
    twf INT,
    hdf INT,
    pwf INT,
    osf INT,
    rnf INT,
    source_dataset VARCHAR(100) NOT NULL,
    batch_id VARCHAR(100) NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_telemetry_product 
ON raw.sensor_telemetry(product_id);

CREATE INDEX IF NOT EXISTS idx_raw_telemetry_failure 
ON raw.sensor_telemetry(machine_failure);
