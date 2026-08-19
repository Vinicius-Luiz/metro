-- Remove 1 registro. Append/MaxValue NÃO captura deletes.
-- O registro deletado permanece no Parquet; apenas novos/alterados são adicionados.

DELETE FROM dbo.test_watermark
WHERE name = 'registro_03';

-- Verificação
SELECT COUNT(*) AS total FROM dbo.test_watermark;
