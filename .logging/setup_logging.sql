-- Script completo para setup da infraestrutura de logging.
-- Executar: psql -U postgres -h localhost -p 5432 -f setup_logging.sql

-- 1. Criar database (precisa rodar com autocommit, fora de transação)
-- Se já existir, ignora erro.
CREATE DATABASE metro_logging ENCODING 'UTF8';

-- 2. Conectar ao database criado
\c metro_logging

-- 3. Recriar schema e tabela (1 linha por execução do METRO)
CREATE SCHEMA IF NOT EXISTS logging;

DROP TABLE IF EXISTS logging.executions;

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

-- Confirmar
\dt logging.*
