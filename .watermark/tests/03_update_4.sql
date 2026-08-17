-- PASSO 3: atualizar 4 registros (ids 1..4), avançando last_update.
--
-- psql -U postgres -h localhost -p 5432 -d stackoverflow -f .watermark/tests/03_update_4.sql
--
-- Depois rode novamente o metro run.
-- Esperado: processar exatamente 4 registros (Append materializa de novo;
-- não faz upsert — pode gerar duplicidade lógica no Target).

UPDATE public.test_watermark
SET value = value + 1000,
    last_update = TIMESTAMP '2026-01-16 18:00:00'
WHERE id IN (1, 2, 3, 4);

SELECT id, name, value, last_update
FROM public.test_watermark
WHERE id IN (1, 2, 3, 4)
ORDER BY id;

SELECT COUNT(*) AS total_records, MAX(last_update) AS max_update
FROM public.test_watermark;
