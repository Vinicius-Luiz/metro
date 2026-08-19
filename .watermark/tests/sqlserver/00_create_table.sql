-- Cria tabela dedicada para testes de Incremental Append (MaxValue) no SQL Server.
-- Executar no banco StackOverflow2010 (runtime: stackoverflow_sql_server_database).

IF OBJECT_ID('dbo.test_watermark', 'U') IS NOT NULL
    DROP TABLE dbo.test_watermark;

CREATE TABLE dbo.test_watermark (
    id          INT IDENTITY(1,1) PRIMARY KEY,
    name        NVARCHAR(100) NOT NULL,
    category    NVARCHAR(50) NOT NULL,
    last_update DATETIME NOT NULL
);
