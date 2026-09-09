-- =============================================================================
-- 5 STANDOUT SQL QUERIES FOR DATA ENGINEERING INTERVIEWS
-- Demonstrates: CTEs, Window Functions (Rolling Avg/StdDev, Lag/Lead, Dense Rank),
-- Z-score anomaly detection, and Star Schema aggregations.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Query 1: Statistical Anomaly Detection (Z-Score > 2.5 Standard Deviations)
-- Objective: Detect sudden mechanical load spikes compared against a 10-sample rolling mean.
-- -----------------------------------------------------------------------------
WITH rolling_stats AS (
    SELECT
        f.udi,
        eq.product_id,
        eq.quality_grade,
        f.torque_nm,
        f.rotational_speed_rpm,
        AVG(f.torque_nm) OVER (
            PARTITION BY eq.product_type 
            ORDER BY f.udi 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS rolling_mean_torque,
        COALESCE(STDDEV(f.torque_nm) OVER (
            PARTITION BY eq.product_type 
            ORDER BY f.udi 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ), 0.01) AS rolling_std_torque,
        f.torque_nm - LAG(f.torque_nm, 1, f.torque_nm) OVER (
            PARTITION BY eq.product_type 
            ORDER BY f.udi
        ) AS torque_step_delta
    FROM warehouse.fact_sensor_telemetry f
    JOIN warehouse.dim_equipment eq ON f.equipment_key = eq.equipment_key
)
SELECT 
    udi,
    product_id,
    quality_grade,
    torque_nm,
    ROUND(rolling_mean_torque::NUMERIC, 2) AS rolling_mean,
    ROUND(torque_step_delta::NUMERIC, 2) AS step_delta,
    ROUND(((torque_nm - rolling_mean_torque) / rolling_std_torque)::NUMERIC, 2) AS z_score
FROM rolling_stats
WHERE ABS((torque_nm - rolling_mean_torque) / rolling_std_torque) > 2.5
ORDER BY ABS(torque_step_delta) DESC
LIMIT 10;

1
-- -----------------------------------------------------------------------------
-- Query 2: Root-Cause Failure Breakdown & Relative Percentage Share per Quality Tier
-- Objective: Calculate failure distribution across product variants with DENSE_RANK.
-- -----------------------------------------------------------------------------
WITH tier_failures AS (
    SELECT 
        eq.product_type,
        eq.quality_grade,
        fm.failure_mode_name,
        COUNT(*) AS failure_count,
        ROUND(AVG(f.temp_differential_c)::NUMERIC, 2) AS avg_thermal_delta_c,
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
    failure_count,
    DENSE_RANK() OVER (PARTITION BY product_type ORDER BY failure_count DESC) AS rank_in_tier,
    ROUND((100.0 * failure_count / SUM(failure_count) OVER (PARTITION BY product_type))::NUMERIC, 2) AS pct_of_tier_failures,
    avg_thermal_delta_c,
    avg_power_kw
FROM tier_failures
ORDER BY product_type, rank_in_tier;


-- -----------------------------------------------------------------------------
-- Query 3: Cumulative Degradation & Overstrain Fatigue Leaderboard
-- Objective: Rank units with highest cumulative mechanical stress.
-- -----------------------------------------------------------------------------
SELECT 
    f.udi,
    eq.product_id,
    eq.quality_grade,
    f.tool_wear_min,
    f.torque_nm,
    f.overstrain_metric,
    fm.failure_mode_name,
    DENSE_RANK() OVER (ORDER BY f.overstrain_metric DESC) AS global_stress_rank
FROM warehouse.fact_sensor_telemetry f
JOIN warehouse.dim_equipment eq ON f.equipment_key = eq.equipment_key
JOIN warehouse.dim_failure_mode fm ON f.failure_mode_key = fm.failure_mode_key
ORDER BY global_stress_rank
LIMIT 10;


-- -----------------------------------------------------------------------------
-- Query 4: Operating Condition Quintiles (NTILE) & Failure Probability
-- Objective: Segment rotational speed into 5 operating buckets and compute failure rates.
-- -----------------------------------------------------------------------------
WITH speed_quintiles AS (
    SELECT 
        f.udi,
        f.rotational_speed_rpm,
        f.is_failure,
        NTILE(5) OVER (ORDER BY f.rotational_speed_rpm) AS speed_quintile
    FROM warehouse.fact_sensor_telemetry f
)
SELECT 
    speed_quintile,
    MIN(rotational_speed_rpm) AS min_rpm,
    MAX(rotational_speed_rpm) AS max_rpm,
    COUNT(*) AS total_readings,
    SUM(CASE WHEN is_failure THEN 1 ELSE 0 END) AS failures_count,
    ROUND((100.0 * SUM(CASE WHEN is_failure THEN 1 ELSE 0 END) / COUNT(*))::NUMERIC, 2) AS failure_rate_pct
FROM speed_quintiles
GROUP BY speed_quintile
ORDER BY speed_quintile;


-- -----------------------------------------------------------------------------
-- Query 5: Pipeline Data Quality & SLA Health Score
-- Objective: Query automated quality check results to compute overall pipeline health.
-- -----------------------------------------------------------------------------
SELECT 
    check_category,
    COUNT(*) AS total_assertions_run,
    SUM(CASE WHEN status = 'PASSED' THEN 1 ELSE 0 END) AS passed_count,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
    ROUND((100.0 * SUM(CASE WHEN status = 'PASSED' THEN 1 ELSE 0 END) / COUNT(*))::NUMERIC, 2) AS pass_rate_pct
FROM quality.check_log
GROUP BY check_category
ORDER BY pass_rate_pct DESC;
