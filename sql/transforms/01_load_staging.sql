-- =============================================================================
-- TRANSFORM: RAW -> STAGING
-- Uses CTEs to compute engineering features and classify failure modes.
-- =============================================================================

WITH raw_source AS (
    SELECT 
        udi,
        product_id,
        product_type,
        air_temperature_k,
        process_temperature_k,
        rotational_speed_rpm,
        torque_nm,
        tool_wear_min,
        machine_failure,
        twf,
        hdf,
        pwf,
        osf,
        rnf,
        batch_id
    FROM raw.sensor_telemetry
),

enriched_calculations AS (
    SELECT
        udi,
        product_id,
        product_type,
        CASE 
            WHEN product_type = 'L' THEN 'Low (50% variant)'
            WHEN product_type = 'M' THEN 'Medium (30% variant)'
            WHEN product_type = 'H' THEN 'High (20% variant)'
            ELSE 'Unknown'
        END AS product_quality_grade,
        
        -- Temperature in Celsius: K - 273.15
        ROUND((air_temperature_k - 273.15)::NUMERIC, 2) AS air_temp_c,
        ROUND((process_temperature_k - 273.15)::NUMERIC, 2) AS process_temp_c,
        ROUND(((process_temperature_k - 273.15) - (air_temperature_k - 273.15))::NUMERIC, 2) AS temp_differential_c,
        
        rotational_speed_rpm,
        torque_nm,
        
        -- Mechanical Power (kW) = (2 * PI * RPM * Torque / 60) / 1000
        ROUND(((2 * 3.1415926535 * rotational_speed_rpm * torque_nm) / 60000.0)::NUMERIC, 2) AS mechanical_power_kw,
        
        tool_wear_min,
        
        -- Overstrain metric = Tool wear * Torque
        ROUND((tool_wear_min * torque_nm)::NUMERIC, 2) AS overstrain_metric,
        
        machine_failure,
        
        -- Categorize the dominant failure mode
        CASE 
            WHEN hdf = 1 THEN 'Heat Dissipation Failure'
            WHEN pwf = 1 THEN 'Power Failure'
            WHEN osf = 1 THEN 'Overstrain Failure'
            WHEN twf = 1 THEN 'Tool Wear Failure'
            WHEN rnf = 1 THEN 'Random Failure'
            WHEN machine_failure = 1 THEN 'Unclassified Failure'
            ELSE 'Normal Operation'
        END AS failure_mode,
        
        batch_id
    FROM raw_source
)

INSERT INTO staging.stg_sensor_telemetry (
    udi,
    product_id,
    product_type,
    product_quality_grade,
    air_temp_c,
    process_temp_c,
    temp_differential_c,
    rotational_speed_rpm,
    torque_nm,
    mechanical_power_kw,
    tool_wear_min,
    overstrain_metric,
    machine_failure,
    failure_mode,
    batch_id,
    transformed_at
)
SELECT 
    udi,
    product_id,
    product_type,
    product_quality_grade,
    air_temp_c,
    process_temp_c,
    temp_differential_c,
    rotational_speed_rpm,
    torque_nm,
    mechanical_power_kw,
    tool_wear_min,
    overstrain_metric,
    machine_failure,
    failure_mode,
    batch_id,
    CURRENT_TIMESTAMP
FROM enriched_calculations
ON CONFLICT (udi) DO UPDATE SET
    product_quality_grade = EXCLUDED.product_quality_grade,
    air_temp_c = EXCLUDED.air_temp_c,
    process_temp_c = EXCLUDED.process_temp_c,
    temp_differential_c = EXCLUDED.temp_differential_c,
    rotational_speed_rpm = EXCLUDED.rotational_speed_rpm,
    torque_nm = EXCLUDED.torque_nm,
    mechanical_power_kw = EXCLUDED.mechanical_power_kw,
    tool_wear_min = EXCLUDED.tool_wear_min,
    overstrain_metric = EXCLUDED.overstrain_metric,
    machine_failure = EXCLUDED.machine_failure,
    failure_mode = EXCLUDED.failure_mode,
    batch_id = EXCLUDED.batch_id,
    transformed_at = CURRENT_TIMESTAMP;
