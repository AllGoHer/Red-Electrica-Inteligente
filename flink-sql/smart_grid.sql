-- ============================================================
-- CONFIGURACIÓN DE FLINK SQL
-- ============================================================
SET 'execution.attached' = 'false';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'AT_LEAST_ONCE';
SET 'execution.checkpointing.timeout' = '60s';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'sql-client.execution.max-table-result-rows' = '10000';

-- ============================================================
-- 1. FUENTE: KAFKA (Datos de la red eléctrica)
-- ============================================================
CREATE TABLE IF NOT EXISTS kafka_source (
    `timestamp` BIGINT,
    voltage_v DOUBLE,
    current_a DOUBLE,
    power_kw DOUBLE,
    solar_kw DOUBLE,
    wind_kw DOUBLE,
    fault_num INT,
    fault_indicator STRING,
    temperature_c DOUBLE,
    humidity_perc DOUBLE,
    electricity_price_gbp_per_kwh DOUBLE,
    event_time AS TO_TIMESTAMP(FROM_UNIXTIME(`timestamp`)),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'smartgrid',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink_sql_group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- ============================================================
-- 2. SUMIDERO: POSTGRESQL (Métricas Agregadas) - CORREGIDO
-- ============================================================
CREATE TABLE IF NOT EXISTS postgres_sink (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    avg_power_kw DOUBLE,
    max_fault_num INT,
    total_solar_generated DOUBLE,
    avg_price DOUBLE,
    total_events BIGINT,      
    quality_score DECIMAL(4, 1)    
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/smartgrid',
    'table-name' = 'grid_metrics',
    'username' = 'admin',
    'password' = 'admin',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '5s',
    'sink.max-retries' = '3'
);

-- ============================================================
-- 3. SUMIDERO: ALERTAS EN POSTGRESQL
-- ============================================================
CREATE TABLE IF NOT EXISTS alert_sink (
    alert_time TIMESTAMP(3),
    alert_type STRING,
    fault_num INT,
    power_kw DOUBLE,
    voltage_v DOUBLE,
    current_a DOUBLE,
    description STRING,
    severity STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/smartgrid',
    'table-name' = 'grid_alerts',
    'username' = 'admin',
    'password' = 'admin',
    'sink.buffer-flush.max-rows' = '1',
    'sink.buffer-flush.interval' = '1s'
);

-- ============================================================
-- 4. CONSULTA PRINCIPAL: Agregaciones por ventana de 1 minuto
-- ============================================================
INSERT INTO postgres_sink
SELECT 
    window_start,
    window_end,
    AVG(power_kw) AS avg_power_kw,
    MAX(fault_num) AS max_fault_num,
    SUM(solar_kw) AS total_solar_generated,
    AVG(electricity_price_gbp_per_kwh) AS avg_price,
    COUNT(*) AS total_events,
    100.0 AS quality_score
FROM TABLE(
    TUMBLE(TABLE kafka_source, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end;

-- ============================================================
-- 5. CONSULTA DE ALERTAS: Alertas en tiempo real
-- ============================================================
INSERT INTO alert_sink
SELECT 
    event_time AS alert_time,
    CASE 
        WHEN fault_num = 1 THEN 'VOLTAGE_DROP'
        WHEN fault_num = 2 THEN 'CURRENT_OVERLOAD'
        WHEN fault_num = 3 THEN 'BLACKOUT_RISK'
        ELSE 'NORMAL'
    END AS alert_type,
    fault_num,
    power_kw,
    voltage_v,
    current_a,
    CONCAT(
        'Fault detected: ', 
        fault_indicator, 
        ' | Power: ', 
        CAST(power_kw AS STRING), 
        ' kW'
    ) AS description,
    CASE 
        WHEN fault_num = 1 THEN 'HIGH'
        WHEN fault_num = 2 THEN 'HIGH'
        WHEN fault_num = 3 THEN 'CRITICAL'
        ELSE 'LOW'
    END AS severity
FROM kafka_source
WHERE fault_num > 0;
