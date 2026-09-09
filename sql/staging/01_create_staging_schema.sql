-- =============================================================================
-- STAGING SCHEMA & TRANSFORMATION
-- Objective: Clean, validate, and compute domain-specific physical engineering features.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS staging;

-- 1. Staging Telemetry Table
CREATE TABLE IF NOT EXISTS staging.stg_sensor_telemetry (
    udi INT PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    product_type CHAR(1) NOT NULL,
    product_quality_grade VARCHAR(20) NOT NULL,
    air_temp_c NUMERIC(8, 2) NOT NULL,
    process_temp_c NUMERIC(8, 2) NOT NULL,
    temp_differential_c NUMERIC(8, 2) NOT NULL,
    rotational_speed_rpm INT NOT NULL,
    torque_nm NUMERIC(8, 2) NOT NULL,
    mechanical_power_kw NUMERIC(8, 2) NOT NULL,
    tool_wear_min INT NOT NULL,
    overstrain_metric NUMERIC(10, 2) NOT NULL,
    machine_failure INT NOT NULL,
    failure_mode VARCHAR(50) NOT NULL,
    batch_id VARCHAR(100) NOT NULL,
    transformed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stg_product ON staging.stg_sensor_telemetry(product_id);
CREATE INDEX IF NOT EXISTS idx_stg_failure ON staging.stg_sensor_telemetry(machine_failure);
