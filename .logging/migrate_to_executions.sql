-- Recria logging.executions com colunas flat (sem JSONB).
-- Executar: psql -U postgres -h localhost -p 5432 -d metro_logging -f migrate_to_executions.sql

\c metro_logging

CREATE SCHEMA IF NOT EXISTS logging;

DROP TABLE IF EXISTS logging.executions;
DROP TABLE IF EXISTS logging.logs;

CREATE TABLE logging.executions (
    id SERIAL PRIMARY KEY,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMP,
    status VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255),
    name VARCHAR(255) NOT NULL,
    target_schema_name VARCHAR(255) NOT NULL,
    target_name VARCHAR(255) NOT NULL,
    mode VARCHAR(255) NOT NULL,
    source_type VARCHAR(255) NOT NULL,
    source_runtime VARCHAR(255) NOT NULL,
    target_type VARCHAR(255) NOT NULL,
    target_runtime VARCHAR(255) NOT NULL,
    strategy_type VARCHAR(255),
    strategy_reference_column VARCHAR(255),
    strategy_lookback_periods INT,
    partition_type VARCHAR(255),
    partition_reference_column VARCHAR(255),
    rows_processed BIGINT,
    duration_seconds NUMERIC(10, 2),
    CONSTRAINT chk_executions_status
        CHECK (status IN ('running', 'success', 'error', 'cancelled'))
);

CREATE INDEX idx_executions_started_at
    ON logging.executions(started_at DESC);

CREATE INDEX idx_executions_status
    ON logging.executions(status);

CREATE INDEX idx_executions_table
    ON logging.executions(schema_name, name);

CREATE INDEX idx_executions_target
    ON logging.executions(target_schema_name, target_name);

CREATE INDEX idx_executions_mode
    ON logging.executions(mode);

\dt logging.*
