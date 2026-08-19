-- Insere 10 registros iniciais (1ª carga Append = Full Load equivalente).
-- Datas espalhadas em 2023, 2024 e 2025 para exercitar particionamento Hive.
-- Formato ISO 8601 (T) evita ambiguidade de DATEFORMAT dmy.

SET DATEFORMAT ymd;

INSERT INTO dbo.test_watermark (name, category, last_update) VALUES
    ('registro_01', 'alpha', '2023-03-15T10:00:00'),
    ('registro_02', 'alpha', '2023-06-20T11:30:00'),
    ('registro_03', 'beta',  '2023-09-10T08:45:00'),
    ('registro_04', 'beta',  '2024-01-05T14:00:00'),
    ('registro_05', 'gamma', '2024-04-18T09:15:00'),
    ('registro_06', 'gamma', '2024-07-22T16:30:00'),
    ('registro_07', 'delta', '2024-10-30T12:00:00'),
    ('registro_08', 'delta', '2025-01-12T07:45:00'),
    ('registro_09', 'omega', '2025-03-01T18:20:00'),
    ('registro_10', 'omega', '2025-05-10T22:00:00');

-- Verificação
SELECT COUNT(*) AS total, MAX(last_update) AS max_last_update
FROM dbo.test_watermark;
