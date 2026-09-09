-- =============================================================================
-- ANALYTICS VIEWS (ADVANCED SQL: WINDOW FUNCTIONS & CTEs)
-- Objective: Showcase rolling statistics, anomaly detection, lag/lead deltas, and ranking.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- -----------------------------------------------------------------------------
-- 1. Rolling Telemetry & Anomaly Detection View
-- Demonstrates: AVG/STDDEV OVER ROWS, LAG, Anomaly Z-scores
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.v_sensor_anomaly_features AS
WITH rolling_stats AS (
    SELECT
        f.udi,
        eq.product_id,
        eq.product_type,
        eq.quality_grade,
        f.rotational_speed_rpm,
        f.torque_nm,
        f.temp_differential_c,
        f.mechanical_power_kw,
        f.tool_wear_min,
        f.overstrain_metric,
        fm.failure_mode_name,
        f.is_failure,
        
        -- Window Function 1: 10-reading rolling average of Torque
        AVG(f.torque_nm) OVER (
            PARTITION BY eq.product_type 
            ORDER BY f.udi 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_torque,
        
        -- Window Function 2: 10-reading rolling standard deviation of Torque
        COALESCE(STDDEV(f.torque_nm) OVER (
            PARTITION BY eq.product_type 
            ORDER BY f.udi 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ), 0.01) AS rolling_std_torque,
        
        -- Window Function 3: Lag to compute step-change surge in Torque
        f.torque_nm - LAG(f.torque_nm, 1, f.torque_nm) OVER (
            PARTITION BY eq.product_type 
            ORDER BY f.udi
        ) AS torque_step_delta,
        
        -- Window Function 4: Rolling average of Mechanical Power
        AVG(f.mechanical_power_kw) OVER (
            PARTITION BY eq.product_type 
            ORDER BY f.udi 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_power_kw
        
    FROM warehouse.fact_sensor_telemetry f
    JOIN warehouse.dim_equipment eq ON f.equipment_key = eq.equipment_key
    JOIN warehouse.dim_failure_mode fm ON f.failure_mode_key = fm.failure_mode_key
)
SELECT 
    *,
    -- Z-Score Anomaly metric: (Current Value - Rolling Mean) / Rolling StdDev
    ROUND(((torque_nm - rolling_avg_torque) / rolling_std_torque)::NUMERIC, 2) AS torque_z_score,
    
    -- Flag if sensor deviates by > 2.5 standard deviations from rolling mean
    CASE 
        WHEN ABS((torque_nm - rolling_avg_torque) / rolling_std_torque) > 2.5 THEN TRUE 
        ELSE FALSE 
    END AS is_torque_anomaly
FROM rolling_stats;

-- -----------------------------------------------------------------------------
-- 2. Failure Distribution & Root Cause Aggregation
-- Demonstrates: CTEs, Group By, Window Partition Ranking
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.v_failure_root_cause_summary AS
WITH failure_counts AS (
    SELECT 
        eq.product_type,
        eq.quality_grade,
        fm.failure_mode_name,
        COUNT(*) AS occurrence_count,
        ROUND(AVG(f.temp_differential_c)::NUMERIC, 2) AS avg_temp_diff_c,
        ROUND(AVG(f.rotational_speed_rpm)::NUMERIC, 0) AS avg_rpm,
        ROUND(AVG(f.torque_nm)::NUMERIC, 2) AS avg_torque_nm,
        ROUND(AVG(f.mechanical_power_kw)::NUMERIC, 2) AS avg_power_kw
    FROM warehouse.fact_sensor_telemetry f
    JOIN warehouse.dim_equipment eq ON f.equipment_key = eq.equipment_key
    JOIN warehouse.dim_failure_mode fm ON f.failure_mode_key = fm.failure_mode_key
    WHERE f.is_failure = TRUE
    GROUP BY eq.product_type, eq.quality_grade, fm.failure_mode_name
)
SELECT 
    product_type,
    quality_grade,
    failure_mode_name,
    occurrence_count,
    avg_temp_diff_c,
    avg_rpm,
    avg_torque_nm,
    avg_power_kw,
    -- Rank failure modes within each product type
    DENSE_RANK() OVER (
        PARTITION BY product_type 
        ORDER BY occurrence_count DESC
    ) AS failure_rank_in_tier,
    
    -- % Share of failures in that product type
    ROUND((100.0 * occurrence_count / SUM(occurrence_count) OVER (PARTITION BY product_type))::NUMERIC, 2) AS failure_pct_share
FROM failure_counts;

-- -----------------------------------------------------------------------------
-- 3. Top High-Risk Operating Units Leaderboard
-- Demonstrates: Global ranking using DENSE_RANK() and OVERSTRAIN filtering
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.v_high_risk_equipment_leaderboard AS
SELECT 
    f.udi,
    eq.product_id,
    eq.quality_grade,
    f.overstrain_metric,
    f.tool_wear_min,
    f.torque_nm,
    f.temp_differential_c,
    f.mechanical_power_kw,
    fm.failure_mode_name,
    f.is_failure,
    DENSE_RANK() OVER (ORDER BY f.overstrain_metric DESC) AS risk_rank
FROM warehouse.fact_sensor_telemetry f
JOIN warehouse.dim_equipment eq ON f.equipment_key = eq.equipment_key
JOIN warehouse.dim_failure_mode fm ON f.failure_mode_key = fm.failure_mode_key
LIMIT 25;
