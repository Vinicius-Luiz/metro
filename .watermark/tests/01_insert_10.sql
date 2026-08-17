-- PASSO 1: inserir 10 registros iniciais (carga base para a 1ª execução do Append).
--
-- psql -U postgres -h localhost -p 5432 -d stackoverflow -f .watermark/tests/01_insert_10.sql
--
-- Depois rode:
-- metro run tasks/incremental_append/test_watermark_append.yaml --secret-provider local --watermark-api-url http://localhost:8000

TRUNCATE TABLE public.test_watermark RESTART IDENTITY;

INSERT INTO public.test_watermark (name, value, last_update) VALUES
    ('record_01', 100, '2026-01-01 10:00:00'),
    ('record_02', 200, '2026-01-02 11:00:00'),
    ('record_03', 300, '2026-01-03 12:00:00'),
    ('record_04', 400, '2026-01-04 13:00:00'),
    ('record_05', 500, '2026-01-05 14:00:00'),
    ('record_06', 600, '2026-01-06 15:00:00'),
    ('record_07', 700, '2026-01-07 16:00:00'),
    ('record_08', 800, '2026-01-08 17:00:00'),
    ('record_09', 900, '2026-01-09 18:00:00'),
    ('record_10', 1000, '2026-01-10 19:00:00');

SELECT COUNT(*) AS total_records, MAX(last_update) AS max_update
FROM public.test_watermark;
