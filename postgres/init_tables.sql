-- ============================================================
-- TABLA DE MÉTRICAS AGREGADAS DE LA RED ELÉCTRICA
-- ============================================================
CREATE TABLE IF NOT EXISTS grid_metrics (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    avg_power_kw DOUBLE PRECISION,
    max_fault_num INT,
    total_solar_generated DOUBLE PRECISION,
    avg_price DOUBLE PRECISION,
    total_events BIGINT,
    quality_score DOUBLE PRECISION,
    PRIMARY KEY (window_start, window_end)
);

-- ============================================================
-- TABLA PARA ALERTAS EN TIEMPO REAL
-- ============================================================
CREATE TABLE IF NOT EXISTS grid_alerts (
    alert_id SERIAL PRIMARY KEY,
    alert_time TIMESTAMP(3),
    alert_type VARCHAR(50),
    fault_num INT,
    power_kw DOUBLE PRECISION,
    voltage_v DOUBLE PRECISION,
    current_a DOUBLE PRECISION,
    description TEXT,
    severity VARCHAR(20),
    is_resolved BOOLEAN DEFAULT FALSE
);

-- ============================================================
-- TABLA SCD TYPE 2: HISTORIAL DE PRECIOS DE ENERGÍA
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_price_history (
    price_id SERIAL PRIMARY KEY,
    price_value DOUBLE PRECISION,
    effective_date TIMESTAMP(3),
    expiry_date TIMESTAMP(3),
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- TABLA DE CALIDAD DE DATOS
-- ============================================================
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    metric_id SERIAL PRIMARY KEY,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    total_records INT,
    invalid_power INT,
    invalid_voltage INT,
    null_values INT,
    quality_score DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- ÍNDICES PARA MEJORAR RENDIMIENTO
-- ============================================================
CREATE INDEX idx_window_start ON grid_metrics(window_start);
CREATE INDEX idx_window_end ON grid_metrics(window_end);
CREATE INDEX idx_alert_time ON grid_alerts(alert_time);
CREATE INDEX idx_price_effective ON dim_price_history(effective_date);
CREATE INDEX idx_price_current ON dim_price_history(is_current);
