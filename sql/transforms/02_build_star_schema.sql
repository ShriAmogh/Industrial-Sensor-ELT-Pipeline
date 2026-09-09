-- =============================================================================
-- TRANSFORM: STAGING -> WAREHOUSE STAR SCHEMA
-- Populates Dimension and Fact tables using surrogate key lookups.
-- =============================================================================

-- Step 1: Upsert Dimension dim_equipment
INSERT INTO warehouse.dim_equipment (
    product_id,
    product_type,
    quality_grade,
    updated_at
)
SELECT DISTINCT
    product_id,
    product_type,
    product_quality_grade,
    CURRENT_TIMESTAMP
FROM staging.stg_sensor_telemetry
ON CONFLICT (product_id) DO UPDATE SET
    quality_grade = EXCLUDED.quality_grade,
    updated_at = CURRENT_TIMESTAMP;

-- Step 2: Populate Fact Table fact_sensor_telemetry
INSERT INTO warehouse.fact_sensor_telemetry (
    udi,
    equipment_key,
    failure_mode_key,
    air_temp_c,
    process_temp_c,
    temp_differential_c,
    rotational_speed_rpm,
    torque_nm,
    mechanical_power_kw,
    tool_wear_min,
    overstrain_metric,
    is_failure,
    loaded_at
)
SELECT 
    stg.udi,
    eq.equipment_key,
    fm.failure_mode_key,
    stg.air_temp_c,
    stg.process_temp_c,
    stg.temp_differential_c,
    stg.rotational_speed_rpm,
    stg.torque_nm,
    stg.mechanical_power_kw,
    stg.tool_wear_min,
    stg.overstrain_metric,
    (stg.machine_failure = 1) AS is_failure,
    CURRENT_TIMESTAMP
FROM staging.stg_sensor_telemetry stg
JOIN warehouse.dim_equipment eq 
    ON stg.product_id = eq.product_id
JOIN warehouse.dim_failure_mode fm 
    ON stg.failure_mode = fm.failure_mode_name
ON CONFLICT (udi) DO UPDATE SET
    equipment_key = EXCLUDED.equipment_key,
    failure_mode_key = EXCLUDED.failure_mode_key,
    air_temp_c = EXCLUDED.air_temp_c,
    process_temp_c = EXCLUDED.process_temp_c,
    temp_differential_c = EXCLUDED.temp_differential_c,
    rotational_speed_rpm = EXCLUDED.rotational_speed_rpm,
    torque_nm = EXCLUDED.torque_nm,
    mechanical_power_kw = EXCLUDED.mechanical_power_kw,
    tool_wear_min = EXCLUDED.tool_wear_min,
    overstrain_metric = EXCLUDED.overstrain_metric,
    is_failure = EXCLUDED.is_failure,
    loaded_at = CURRENT_TIMESTAMP;
