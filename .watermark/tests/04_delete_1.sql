-- PASSO 4: remover 1 registro (id 10).
--
-- psql -U postgres -h localhost -p 5432 -d stackoverflow -f .watermark/tests/04_delete_1.sql
--
-- Depois rode novamente o metro run.
-- Esperado: 0 registros processados; o id 10 permanece nos Parquets
-- já gravados (Append não é CDC e não propaga DELETE).

DELETE FROM public.test_watermark
WHERE id = 10;

SELECT COUNT(*) AS total_records, MAX(last_update) AS max_update
FROM public.test_watermark;

SELECT id, name
FROM public.test_watermark
WHERE id = 10;
