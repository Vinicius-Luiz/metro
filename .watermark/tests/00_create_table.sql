-- PASSO 0: cria a tabela de teste (vazia).
-- Banco: stackoverflow
--
-- psql -U postgres -h localhost -p 5432 -d stackoverflow -f .watermark/tests/00_create_table.sql

DROP TABLE IF EXISTS public.test_watermark CASCADE;

CREATE TABLE public.test_watermark (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    value INTEGER NOT NULL,
    last_update TIMESTAMP NOT NULL DEFAULT NOW()
);

SELECT COUNT(*) AS total_records FROM public.test_watermark;
