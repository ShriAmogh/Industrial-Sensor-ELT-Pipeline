-- =============================================================================
-- WAREHOUSE SCHEMA & DIMENSIONAL MODEL (STAR SCHEMA)
-- Objective: Organize data for OLAP, BI dashboards, and ML feature stores.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS warehouse;

-- 1. Dimension: Equipment (Equipment Catalog & Profile)
CREATE TABLE IF NOT EXISTS warehouse.dim_equipment (
    equipment_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    product_type CHAR(1) NOT NULL,
    quality_grade VARCHAR(20) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Dimension: Failure Mode Catalog
CREATE TABLE IF NOT EXISTS warehouse.dim_failure_mode (
    failure_mode_key SERIAL PRIMARY KEY,
    failure_mode_name VARCHAR(50) UNIQUE NOT NULL,
    severity_level VARCHAR(20) NOT NULL,
    root_cause_category VARCHAR(50) NOT NULL
);

-- Pre-populate Failure Mode Dimension
INSERT INTO warehouse.dim_failure_mode (failure_mode_name, severity_level, root_cause_category)
VALUES
    ('Normal Operation', 'None', 'Healthy'),
    ('Heat Dissipation Failure', 'Critical', 'Thermal Dissipation'),
    ('Power Failure', 'Critical', 'Electrical / Mechanical Power Load'),
    ('Overstrain Failure', 'High', 'Mechanical Stress / Fatigue'),
    ('Tool Wear Failure', 'Medium', 'Abrasive Wear'),
    ('Random Failure', 'Low', 'Stochastic Noise'),
    ('Unclassified Failure', 'High', 'Unknown')
ON CONFLICT (failure_mode_name) DO NOTHING;

-- 3. Fact: Sensor Telemetry
CREATE TABLE IF NOT EXISTS warehouse.fact_sensor_telemetry (
    telemetry_key BIGSERIAL PRIMARY KEY,
    udi INT UNIQUE NOT NULL,
    equipment_key INT NOT NULL REFERENCES warehouse.dim_equipment(equipment_key),
    failure_mode_key INT NOT NULL REFERENCES warehouse.dim_failure_mode(failure_mode_key),
    air_temp_c NUMERIC(8, 2) NOT NULL,
    process_temp_c NUMERIC(8, 2) NOT NULL,
    temp_differential_c NUMERIC(8, 2) NOT NULL,
    rotational_speed_rpm INT NOT NULL,
    torque_nm NUMERIC(8, 2) NOT NULL,
    mechanical_power_kw NUMERIC(8, 2) NOT NULL,
    tool_wear_min INT NOT NULL,
    overstrain_metric NUMERIC(10, 2) NOT NULL,
    is_failure BOOLEAN NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_equipment_key ON warehouse.fact_sensor_telemetry(equipment_key);
CREATE INDEX IF NOT EXISTS idx_fact_failure_mode ON warehouse.fact_sensor_telemetry(failure_mode_key);
CREATE INDEX IF NOT EXISTS idx_fact_is_failure ON warehouse.fact_sensor_telemetry(is_failure);
