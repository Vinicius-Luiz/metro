-- Insere 10 registros iniciais (1ª carga Append = Full Load equivalente).
-- Datas espalhadas em 2023, 2024 e 2025 para exercitar particionamento Hive.

INSERT INTO public.test_watermark (name, category, last_update) VALUES
    ('registro_01', 'alpha', '2023-03-15 10:00:00'),
    ('registro_02', 'alpha', '2023-06-20 11:30:00'),
    ('registro_03', 'beta',  '2023-09-10 08:45:00'),
    ('registro_04', 'beta',  '2024-01-05 14:00:00'),
    ('registro_05', 'gamma', '2024-04-18 09:15:00'),
    ('registro_06', 'gamma', '2024-07-22 16:30:00'),
    ('registro_07', 'delta', '2024-10-30 12:00:00'),
    ('registro_08', 'delta', '2025-01-12 07:45:00'),
    ('registro_09', 'omega', '2025-03-01 18:20:00'),
    ('registro_10', 'omega', '2025-05-10 22:00:00');

-- Verificação
SELECT COUNT(*) AS total, MAX(last_update) AS max_last_update
FROM public.test_watermark;
