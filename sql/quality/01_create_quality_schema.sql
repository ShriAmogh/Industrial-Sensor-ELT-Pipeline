-- =============================================================================
-- DATA QUALITY SCHEMA & AUDIT TRAIL
-- Objective: Log the execution and results of automated data quality assertions.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS quality;

CREATE TABLE IF NOT EXISTS quality.check_log (
    check_id BIGSERIAL PRIMARY KEY,
    batch_id VARCHAR(100),
    check_name VARCHAR(100) NOT NULL,
    check_category VARCHAR(50) NOT NULL, -- Completeness, Validity, Referential Integrity, Volume
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100),
    status VARCHAR(20) NOT NULL CHECK (status IN ('PASSED', 'FAILED', 'WARNING')),
    observed_value NUMERIC(14, 4),
    threshold_value NUMERIC(14, 4),
    severity VARCHAR(20) NOT NULL CHECK (severity IN ('CRITICAL', 'WARNING', 'INFO')),
    details TEXT,
    checked_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_quality_check_status ON quality.check_log(status);
CREATE INDEX IF NOT EXISTS idx_quality_check_table ON quality.check_log(table_name);
