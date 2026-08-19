-- Atualiza last_update de 4 registros existentes para valor acima do watermark.
-- Demonstra semântica Append: registros atualizados reaparecem como novas linhas.

SET DATEFORMAT ymd;

UPDATE dbo.test_watermark
SET last_update = '2025-05-16T12:00:00'
WHERE name IN ('registro_01', 'registro_05', 'registro_08', 'registro_12');

-- Verificação
SELECT name, category, last_update
FROM dbo.test_watermark
WHERE name IN ('registro_01', 'registro_05', 'registro_08', 'registro_12')
ORDER BY name;
