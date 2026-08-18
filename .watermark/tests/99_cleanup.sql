-- Remove tabela de teste e watermarks associados (via API, se necessário).
-- Watermark task_identifier: raw.test_watermark_append / raw.test_watermark_append_partitioned

DROP TABLE IF EXISTS public.test_watermark;
