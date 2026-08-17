-- PASSO 2: inserir 5 registros novos (devem entrar no 2º Append).
--
-- psql -U postgres -h localhost -p 5432 -d stackoverflow -f .watermark/tests/02_insert_5.sql
--
-- Depois rode novamente o metro run da task de append.
-- Esperado: processar exatamente 5 registros.

INSERT INTO public.test_watermark (name, value, last_update) VALUES
    ('record_11', 1100, '2026-01-11 08:00:00'),
    ('record_12', 1200, '2026-01-12 09:00:00'),
    ('record_13', 1300, '2026-01-13 10:00:00'),
    ('record_14', 1400, '2026-01-14 11:00:00'),
    ('record_15', 1500, '2026-01-15 12:00:00');

SELECT COUNT(*) AS total_records, MAX(last_update) AS max_update
FROM public.test_watermark;

SELECT id, name, value, last_update
FROM public.test_watermark
WHERE last_update > TIMESTAMP '2026-01-10 19:00:00'
ORDER BY last_update;
