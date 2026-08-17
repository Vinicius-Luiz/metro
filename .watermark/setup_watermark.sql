-- Script completo para setup da infraestrutura de watermark.
-- Executar: psql -U postgres -h localhost -p 5432 -f setup_watermark.sql

-- 1. Criar database (precisa rodar com autocommit, fora de transação)
-- Se já existir, ignora erro.
CREATE DATABASE metro_watermark ENCODING 'UTF8';

-- 2. Conectar ao database criado
\c metro_watermark

-- 3. Criar schema e tabela
CREATE SCHEMA IF NOT EXISTS watermark;

CREATE TABLE IF NOT EXISTS watermark.watermarks (
    id SERIAL PRIMARY KEY,
    task_identifier VARCHAR(255) NOT NULL,
    reference_column VARCHAR(255) NOT NULL,
    watermark_value TEXT NOT NULL,
    watermark_type VARCHAR(50) NOT NULL,
    last_execution TIMESTAMP NOT NULL DEFAULT NOW(),
    last_record_count BIGINT DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_watermark_task_column UNIQUE (task_identifier, reference_column)
);

CREATE INDEX IF NOT EXISTS idx_watermarks_task
    ON watermark.watermarks(task_identifier);

CREATE INDEX IF NOT EXISTS idx_watermarks_last_execution
    ON watermark.watermarks(last_execution DESC);

CREATE OR REPLACE FUNCTION watermark.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_watermarks_updated_at ON watermark.watermarks;

CREATE TRIGGER trg_watermarks_updated_at
    BEFORE UPDATE ON watermark.watermarks
    FOR EACH ROW
    EXECUTE FUNCTION watermark.update_updated_at_column();

-- Confirmar
\dt watermark.*
