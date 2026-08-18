-- Cria tabela dedicada para testes de Incremental Append (MaxValue).
-- Executar no banco stackoverflow (runtime: stackoverflow_postgres_database).
-- psql -U postgres -h localhost -p 5432 -d stackoverflow -f 00_create_table.sql

DROP TABLE IF EXISTS public.test_watermark;

CREATE TABLE public.test_watermark (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    category    TEXT NOT NULL,
    last_update TIMESTAMP NOT NULL
);

COMMENT ON TABLE public.test_watermark IS
    'Tabela de teste METRO — Incremental Append / MaxValue';
