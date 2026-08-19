-- Remove tabela de teste e watermarks associados (via API, se necessário).
-- Watermark task_identifier: raw.sqlserver_test_watermark_append /
-- raw.sqlserver_test_watermark_append_partitioned

IF OBJECT_ID('dbo.test_watermark', 'U') IS NOT NULL
    DROP TABLE dbo.test_watermark;
