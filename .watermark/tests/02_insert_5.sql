-- Insere 5 registros novos com last_update acima do watermark atual (2025-05-10 22:00:00).

INSERT INTO public.test_watermark (name, category, last_update) VALUES
    ('registro_11', 'alpha', '2025-05-11 08:00:00'),
    ('registro_12', 'beta',  '2025-05-12 10:30:00'),
    ('registro_13', 'gamma', '2025-05-13 14:15:00'),
    ('registro_14', 'delta', '2025-05-14 09:00:00'),
    ('registro_15', 'omega', '2025-05-15 17:45:00');

-- Verificação
SELECT COUNT(*) AS total, MAX(last_update) AS max_last_update
FROM public.test_watermark;
