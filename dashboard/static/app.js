// =============================================================================
// INDUSTRIAL SENSOR ELT PIPELINE DASHBOARD JAVASCRIPT
// =============================================================================

const PRESET_QUERIES = {
    window_anomalies: `-- 1. Window Functions: 10-Reading Rolling Torque & Anomaly Z-Scores
SELECT 
    udi,
    product_id,
    product_type,
    rotational_speed_rpm,
    torque_nm,
    rolling_avg_torque,
    rolling_std_torque,
    torque_step_delta,
    torque_z_score,
    is_torque_anomaly,
    failure_mode_name
FROM analytics.v_sensor_anomaly_features
WHERE is_torque_anomaly = TRUE
ORDER BY ABS(torque_z_score) DESC
LIMIT 20;`,

    failure_ranking: `-- 2. Partition Ranking: Failure Modes by Product Tier (DENSE_RANK)
SELECT 
    product_type,
    quality_grade,
    failure_mode_name,
    occurrence_count,
    avg_temp_diff_c,
    avg_power_kw,
    failure_rank_in_tier,
    failure_pct_share
FROM analytics.v_failure_root_cause_summary
ORDER BY product_type ASC, failure_rank_in_tier ASC;`,

    overstrain_leaderboard: `-- 3. Leaderboard: Top 15 Overstrain Equipment Units
SELECT 
    risk_rank,
    udi,
    product_id,
    quality_grade,
    overstrain_metric,
    tool_wear_min,
    torque_nm,
    temp_differential_c,
    mechanical_power_kw,
    failure_mode_name,
    is_failure
FROM analytics.v_high_risk_equipment_leaderboard
ORDER BY risk_rank ASC
LIMIT 15;`,

    anti_join: `-- 4. Anti-Join: Check for Unmatched Fact-to-Dimension Records (Data Quality)
SELECT 
    f.udi,
    f.equipment_key
FROM warehouse.fact_sensor_telemetry f
LEFT JOIN warehouse.dim_equipment eq 
    ON f.equipment_key = eq.equipment_key
WHERE eq.equipment_key IS NULL
LIMIT 10;`,

    star_schema_join: `-- 5. Star Schema: Fact Join with Equipment & Failure Dimensions
SELECT 
    f.udi,
    eq.product_id,
    eq.product_type,
    eq.quality_grade,
    fm.failure_mode_name,
    fm.severity_level,
    f.air_temp_c,
    f.rotational_speed_rpm,
    f.torque_nm,
    f.mechanical_power_kw,
    f.is_failure
FROM warehouse.fact_sensor_telemetry f
JOIN warehouse.dim_equipment eq ON f.equipment_key = eq.equipment_key
JOIN warehouse.dim_failure_mode fm ON f.failure_mode_key = fm.failure_mode_key
ORDER BY f.udi ASC
LIMIT 25;`,

    quality_sla: `-- 6. Quality SLA: Check Results Breakdown by Target Table
SELECT 
    table_name,
    COUNT(*) as total_checks,
    COUNT(*) FILTER (WHERE status = 'PASSED') as passed,
    COUNT(*) FILTER (WHERE status = 'FAILED') as failed,
    ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'PASSED') / COUNT(*), 1) as pass_rate_pct
FROM quality.check_log
GROUP BY table_name
ORDER BY table_name ASC;`
};

let currentLayerData = null;

// Initialize app on DOM load
document.addEventListener("DOMContentLoaded", () => {
    initTabs();
    initFlowNodes();
    initLayerPills();
    initSQLSandbox();
    initSyncPipeline();

    // Load initial data
    fetchPipelineSummary();
    fetchLayerDetails("raw");
    fetchAnalyticsData();
    fetchQualityData();
});

// =============================================================================
// TAB NAVIGATION
// =============================================================================
function initTabs() {
    const tabButtons = document.querySelectorAll(".tab-btn");
    const tabPanes = document.querySelectorAll(".tab-pane");

    tabButtons.forEach(btn => {
        btn.addEventListener("click", () => {
            const targetId = btn.dataset.tab;

            tabButtons.forEach(b => b.classList.remove("active"));
            tabPanes.forEach(p => p.classList.remove("active"));

            btn.classList.add("active");
            const targetPane = document.getElementById(targetId);
            if (targetPane) targetPane.classList.add("active");

            // Lazy refresh if tab switched
            if (targetId === "tab-analytics") fetchAnalyticsData();
            if (targetId === "tab-quality") fetchQualityData();
        });
    });
}

// =============================================================================
// PIPELINE FLOW NODES
// =============================================================================
function initFlowNodes() {
    const nodes = document.querySelectorAll(".flow-stage-node");
    nodes.forEach(node => {
        node.addEventListener("click", () => {
            nodes.forEach(n => n.classList.remove("active"));
            node.classList.add("active");

            const layerKey = node.dataset.layer;

            // Switch to Layer Inspector Tab
            document.querySelector('.tab-btn[data-tab="tab-layers"]').click();

            // Set active pill
            const pill = document.querySelector(`.pill-btn[data-layer-target="${layerKey}"]`);
            if (pill) pill.click();
        });
    });
}

// =============================================================================
// LAYER PILL SELECTOR
// =============================================================================
function initLayerPills() {
    const pills = document.querySelectorAll(".pill-btn");
    pills.forEach(pill => {
        pill.addEventListener("click", () => {
            pills.forEach(p => p.classList.remove("active"));
            pill.classList.add("active");

            const layerKey = pill.dataset.layerTarget;
            fetchLayerDetails(layerKey);

            // Sync with flow node
            const flowNode = document.querySelector(`.flow-stage-node[data-layer="${layerKey}"]`);
            if (flowNode) {
                document.querySelectorAll(".flow-stage-node").forEach(n => n.classList.remove("active"));
                flowNode.classList.add("active");
            }
        });
    });

    // Search filter for table preview
    const searchInput = document.getElementById("table-search");
    if (searchInput) {
        searchInput.addEventListener("input", (e) => {
            const term = e.target.value.toLowerCase();
            filterTableRows("layer-table-container", term);
        });
    }
}

// =============================================================================
// API DATA FETCHERS
// =============================================================================
async function fetchPipelineSummary() {
    try {
        const res = await fetch("/api/pipeline/summary");
        if (!res.ok) throw new Error("Summary API failed");
        const data = await res.json();

        // Update KPIs
        document.getElementById("kpi-raw-val").textContent = (data.counts.raw || 0).toLocaleString();
        document.getElementById("kpi-staging-val").textContent = (data.counts.staging || 0).toLocaleString();
        document.getElementById("kpi-warehouse-val").textContent = `${(data.counts.fact || 0).toLocaleString()} / ${(data.counts.dim_equipment || 0).toLocaleString()} dims`;
        document.getElementById("kpi-anomalies-val").textContent = (data.counts.anomalies || 0).toLocaleString();

        const qStats = data.quality.latest_run;
        if (qStats && qStats.total > 0) {
            const passRate = Math.round((qStats.passed / qStats.total) * 100);
            document.getElementById("kpi-quality-val").textContent = `${passRate}% PASS`;
            document.getElementById("kpi-quality-sub").textContent = `${qStats.passed}/${qStats.total} Rules Succeeded`;
        }
    } catch (err) {
        console.error("Error fetching summary:", err);
    }
}

async function fetchLayerDetails(layerKey) {
    const tableContainer = document.getElementById("layer-table-container");
    const schemaContainer = document.getElementById("schema-table-container");
    tableContainer.innerHTML = '<div class="loading-spinner">Loading warehouse data...</div>';

    try {
        const res = await fetch(`/api/pipeline/layers/${layerKey}`);
        if (!res.ok) throw new Error(`Failed to load ${layerKey}`);
        const data = await res.json();
        currentLayerData = data;

        // Update callout card
        document.getElementById("layer-title-badge").textContent = data.title;
        document.getElementById("layer-sql-file").textContent = data.sql_file;
        document.getElementById("layer-desc").textContent = data.description;
        document.getElementById("layer-why").textContent = data.why;

        document.getElementById("table-name-display").textContent = data.table;
        document.getElementById("schema-target-name").textContent = data.table;
        document.getElementById("schema-col-count").textContent = `${data.schema_columns.length} Columns`;

        // Render preview table
        renderTable(tableContainer, data.sample_columns, data.sample_rows);

        // Render schema definitions table
        const schemaCols = ["column_name", "data_type", "is_nullable"];
        renderTable(schemaContainer, schemaCols, data.schema_columns);
    } catch (err) {
        tableContainer.innerHTML = `<div class="empty-state" style="color:var(--rose);">Error loading layer: ${err.message}</div>`;
    }
}

async function fetchAnalyticsData() {
    const leaderboardContainer = document.getElementById("leaderboard-table-container");
    const failureContainer = document.getElementById("failure-table-container");
    const anomaliesContainer = document.getElementById("anomalies-table-container");

    try {
        // High risk leaderboard
        const resLeader = await fetch("/api/analytics/high-risk");
        const dataLeader = await resLeader.json();
        renderTable(leaderboardContainer, [
            "risk_rank", "product_id", "quality_grade", "overstrain_metric", "tool_wear_min", "failure_mode_name", "is_failure"
        ], dataLeader.records);

        // Failure root cause summary
        const resFail = await fetch("/api/analytics/failures");
        const dataFail = await resFail.json();
        renderTable(failureContainer, [
            "product_type", "quality_grade", "failure_mode_name", "occurrence_count", "avg_power_kw", "failure_rank_in_tier", "failure_pct_share"
        ], dataFail.records);

        // Anomalies feed
        const onlyAnomalies = document.getElementById("chk-anomalies-only")?.checked ?? true;
        const resAnom = await fetch(`/api/analytics/anomalies?anomaly_only=${onlyAnomalies}&limit=35`);
        const dataAnom = await resAnom.json();
        renderTable(anomaliesContainer, [
            "udi", "product_id", "product_type", "rotational_speed_rpm", "torque_nm", "rolling_avg_torque", "torque_step_delta", "torque_z_score", "is_torque_anomaly", "failure_mode_name"
        ], dataAnom.records, (row) => row.is_torque_anomaly ? "anomaly-row" : "");

    } catch (err) {
        console.error("Error fetching analytics views:", err);
    }
}

async function fetchQualityData() {
    const auditContainer = document.getElementById("quality-audit-container");
    const rulesContainer = document.getElementById("quality-rules-container");

    try {
        const res = await fetch("/api/quality/checks");
        if (!res.ok) throw new Error("Failed to load quality checks");
        const data = await res.json();

        // Render rules cards
        const checks = data.recent_checks.slice(0, 8);
        rulesContainer.innerHTML = checks.map(c => `
            <div class="quality-rule-card">
                <div class="quality-rule-header">
                    <span class="quality-rule-title">${escapeHtml(c.check_name)}</span>
                    <span class="badge-tag ${c.status === 'PASSED' ? 'badge-emerald' : 'badge-rose'}">${c.status}</span>
                </div>
                <div class="quality-rule-target">Table: ${escapeHtml(c.table_name || 'warehouse')} ${c.column_name ? ' &bull; Col: ' + escapeHtml(c.column_name) : ''}</div>
                <div class="quality-rule-details">${escapeHtml(c.details || 'Assertion validated against warehouse')}</div>
                <div class="node-meta" style="margin-top:4px;">Severity: <strong>${c.severity}</strong> &bull; Observed: ${c.observed_value}</div>
            </div>
        `).join("");

        // Render full audit table
        renderTable(auditContainer, [
            "check_id", "check_name", "check_category", "table_name", "column_name", "status", "severity", "observed_value", "threshold_value", "checked_at"
        ], data.recent_checks);

    } catch (err) {
        auditContainer.innerHTML = `<div class="empty-state" style="color:var(--rose);">Error loading quality data: ${err.message}</div>`;
    }
}

// =============================================================================
// INTERACTIVE SQL SANDBOX
// =============================================================================
function initSQLSandbox() {
    const editor = document.getElementById("sql-editor");
    const presetSelect = document.getElementById("preset-query-select");
    const btnRun = document.getElementById("btn-run-query");

    // Set default query
    editor.value = PRESET_QUERIES.window_anomalies;

    // Change query on dropdown
    presetSelect.addEventListener("change", (e) => {
        const key = e.target.value;
        if (PRESET_QUERIES[key]) {
            editor.value = PRESET_QUERIES[key];
        }
    });

    // Run query on button click
    btnRun.addEventListener("click", executeSandboxQuery);

    // Keyboard shortcut (Ctrl+Enter / Cmd+Enter)
    editor.addEventListener("keydown", (e) => {
        if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
            e.preventDefault();
            executeSandboxQuery();
        }
    });
}

async function executeSandboxQuery() {
    const editor = document.getElementById("sql-editor");
    const stats = document.getElementById("query-stats");
    const meta = document.getElementById("query-result-meta");
    const container = document.getElementById("query-results-container");
    const btnRun = document.getElementById("btn-run-query");

    const query = editor.value.trim();
    if (!query) return;

    btnRun.disabled = true;
    stats.innerHTML = `<span style="color:var(--cyan);">Executing query on sensor_dw...</span>`;
    container.innerHTML = '<div class="loading-spinner">Executing query on PostgreSQL warehouse...</div>';

    try {
        const res = await fetch("/api/query/run", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ query: query, limit: 50 })
        });

        const data = await res.json();
        btnRun.disabled = false;

        if (data.status === "error") {
            stats.innerHTML = `<span style="color:var(--rose);">Query Failed (${data.execution_time_ms}ms)</span>`;
            meta.textContent = "Execution Error";
            container.innerHTML = `<div class="empty-state" style="color:var(--rose); text-align:left; font-family:var(--font-mono); white-space:pre-wrap;">${escapeHtml(data.error)}</div>`;
            return;
        }

        stats.innerHTML = `<span style="color:var(--emerald);">✓ Success</span> &bull; Execution Time: <strong>${data.execution_time_ms}ms</strong>`;
        meta.textContent = `${data.row_count} rows returned`;

        if (data.rows.length === 0) {
            container.innerHTML = `<div class="empty-state">Query executed successfully but returned 0 rows.</div>`;
        } else {
            renderTable(container, data.columns, data.rows);
        }
    } catch (err) {
        btnRun.disabled = false;
        stats.innerHTML = `<span style="color:var(--rose);">Network/Server Error</span>`;
        container.innerHTML = `<div class="empty-state" style="color:var(--rose);">${err.message}</div>`;
    }
}

// =============================================================================
// PIPELINE SYNC TRIGGER
// =============================================================================
function initSyncPipeline() {
    const btnTrigger = document.getElementById("btn-trigger-sync");
    const modal = document.getElementById("sync-modal");
    const btnClose = document.getElementById("btn-close-modal");
    const btnDone = document.getElementById("btn-modal-done");
    const progressFill = document.getElementById("sync-progress-fill");

    const steps = [
        document.getElementById("step-extract"),
        document.getElementById("step-staging"),
        document.getElementById("step-star"),
        document.getElementById("step-analytics"),
        document.getElementById("step-quality")
    ];

    btnTrigger.addEventListener("click", async () => {
        modal.classList.add("active");
        btnDone.disabled = true;
        btnDone.textContent = "Pipeline In Progress...";
        progressFill.style.width = "10%";

        // Reset step indicators
        steps.forEach(s => {
            s.className = "sync-step pending";
            s.querySelector(".step-icon").textContent = "⚪";
        });

        // Animate progression while backend executes
        let currentStepIdx = 0;
        const interval = setInterval(() => {
            if (currentStepIdx < steps.length) {
                steps[currentStepIdx].className = "sync-step active";
                steps[currentStepIdx].querySelector(".step-icon").textContent = "⏳";
                progressFill.style.width = `${(currentStepIdx + 1) * 18}%`;
                currentStepIdx++;
            }
        }, 500);

        try {
            const res = await fetch("/api/pipeline/run-sync", { method: "POST" });
            const data = await res.json();
            clearInterval(interval);
            progressFill.style.width = "100%";

            if (data.status === "success") {
                steps.forEach((s, i) => {
                    s.className = "sync-step success";
                    s.querySelector(".step-icon").textContent = "✅";
                    if (data.steps[i]) {
                        s.querySelector("p").textContent = `${data.steps[i].details} (${data.steps[i].duration_s}s)`;
                    }
                });
                btnDone.textContent = `Completed in ${data.total_duration_s}s (Click to Close)`;
            } else {
                const errMsg = data.detail || data.error || "Unknown error";
                steps.forEach(s => {
                    s.className = "sync-step error";
                    s.querySelector(".step-icon").textContent = "❌";
                });
                btnDone.textContent = `Error: ${errMsg.slice(0, 40)}... (Click to Close)`;
            }
        } catch (err) {
            clearInterval(interval);
            steps.forEach(s => {
                s.className = "sync-step error";
                s.querySelector(".step-icon").textContent = "❌";
            });
            btnDone.textContent = `Execution Failed: ${err.message} (Click to Close)`;
        }
        finally {
            btnDone.disabled = false;
            fetchPipelineSummary();
            fetchLayerDetails("raw");
            fetchAnalyticsData();
            fetchQualityData();
        }
    });

    btnClose.addEventListener("click", () => modal.classList.remove("active"));
    btnDone.addEventListener("click", () => modal.classList.remove("active"));
}

// =============================================================================
// TABLE RENDERING & FILTERING HELPERS
// =============================================================================
function renderTable(container, columns, rows, rowClassFn = null) {
    if (!rows || rows.length === 0) {
        container.innerHTML = '<div class="empty-state">No records found.</div>';
        return;
    }

    let html = `<table><thead><tr>`;
    columns.forEach(col => {
        html += `<th>${escapeHtml(col.replace(/_/g, ' '))}</th>`;
    });
    html += `</tr></thead><tbody>`;

    rows.forEach(row => {
        const extraClass = rowClassFn ? rowClassFn(row) : "";
        html += `<tr class="${extraClass}">`;
        columns.forEach(col => {
            let val = row[col];
            if (val === null || val === undefined) {
                html += `<td style="color:var(--text-dim);">NULL</td>`;
            } else if (typeof val === "boolean") {
                html += `<td><span class="badge-tag ${val ? 'badge-rose' : 'badge-emerald'}">${val ? 'TRUE' : 'FALSE'}</span></td>`;
            } else if (col === "status") {
                const badgeClass = val === 'PASSED' || val === 'SUCCESS' ? 'badge-emerald' : 'badge-rose';
                html += `<td><span class="badge-tag ${badgeClass}">${val}</span></td>`;
            } else if (col === "torque_z_score") {
                const zVal = parseFloat(val);
                const isAnom = Math.abs(zVal) > 2.5;
                html += `<td><span class="badge-tag ${isAnom ? 'badge-amber' : 'badge-cyan'}">${val}</span></td>`;
            } else {
                html += `<td>${escapeHtml(String(val))}</td>`;
            }
        });
        html += `</tr>`;
    });

    html += `</tbody></table>`;
    container.innerHTML = html;
}

function filterTableRows(containerId, term) {
    const container = document.getElementById(containerId);
    if (!container) return;
    const rows = container.querySelectorAll("tbody tr");
    rows.forEach(row => {
        const text = row.textContent.toLowerCase();
        row.style.display = text.includes(term) ? "" : "none";
    });
}

function escapeHtml(str) {
    if (typeof str !== "string") return str;
    return str
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}
