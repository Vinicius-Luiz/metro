-- LIMPEZA: remove a tabela de teste.
--
-- psql -U postgres -h localhost -p 5432 -d stackoverflow -f .watermark/tests/99_cleanup.sql
--
-- Opcional (API):
--   DELETE http://localhost:8000/watermarks/raw.test_watermark_append/last_update
--   DELETE http://localhost:8000/watermarks/raw.test_watermark_append_partitioned/last_update
--
-- Opcional (Target local):
--   remover pastas ./local/raw/test_watermark_append*

DROP TABLE IF EXISTS public.test_watermark CASCADE;
