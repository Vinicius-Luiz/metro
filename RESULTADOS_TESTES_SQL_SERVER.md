# Resultados — Super Teste METRO (SQL Server)

**Data da execução:** 2026-08-18  
**Ambiente:** local (SQL Server StackOverflow2010, Watermark API em `http://localhost:8000`)  
**Todos os modos concluíram com exit code 0.**

---

## Performance — Tempo de Processamento

Tempos medidos pelos timestamps do log (primeiro → último evento da execução).

| Tabela | Linhas | Modo | **Tempo** | **Throughput** | Parquet |
|--------|--------|------|-----------|----------------|---------|
| **Votes** | **10.143.364** | Full Load particionado (year) | **44,9 s** | **225.910 rows/s** | 48,70 MB (52 arquivos) |
| **Comments** | **3.875.183** | Full Load plano | **33,9 s** | **114.312 rows/s** | 267,45 MB (20 arquivos) |
| **Comments** | **3.875.183** | Incremental Replace (year, lookback=19) | **36,6 s** | **105.879 rows/s** | 267,57 MB (21 arquivos) |
| **Posts** | **3.729.195** | Full Load plano | **129,3 s** | **28.842 rows/s** | 1.011,70 MB (19 arquivos) |
| **test_watermark** | 10 → 19 | Incremental Append (plano + particionado) | ~4–5 s / etapa | n/a (tabela de teste) | 0,01 MB |

**Destaque:** `Votes` (10,1M linhas, 6 colunas) teve o maior throughput. `Posts` (3,7M com `Body` nvarchar(MAX)) foi a mais lenta em rows/s e gerou ~1 GB de Parquet — mesmo padrão do PostgreSQL.

---

## 1. Full Load plano — `dbo.Comments`

| Campo | Valor |
|-------|-------|
| Task | `tasks/full_load/sqlserver_comments.yaml` |
| Comando | `metro run tasks/full_load/sqlserver_comments.yaml` |
| Tabela origem | `dbo.Comments` (StackOverflow2010, 6 colunas) |
| **Linhas processadas** | **3.875.183** |
| **Tempo** | **33,9 s** (23:33:31 → 23:34:05) |
| **Throughput** | **114.312 rows/s** |
| Parquet | `./local/raw/sqlserver_comments/` — 20 arquivos, **267,45 MB** |
| Log | `logs/full_load/sqlserver_comments_20260818_233330.log` |

---

## 2. Full Load particionado — `dbo.Votes`

| Campo | Valor |
|-------|-------|
| Task | `tasks/full_load/sqlserver_votes_partition.yaml` |
| Comando | `metro run tasks/full_load/sqlserver_votes_partition.yaml` |
| Tabela origem | `dbo.Votes` (StackOverflow2010, 6 colunas) |
| Partição | `year` em `CreationDate` |
| **Linhas processadas** | **10.143.364** |
| **Tempo** | **44,9 s** (23:34:39 → 23:35:23) |
| **Throughput** | **225.910 rows/s** |
| Partições | `year=2008`, `year=2009`, `year=2010` (52 arquivos) |
| Parquet | `./local/raw/sqlserver_votes/` — **48,70 MB** |
| Log | `logs/full_load/sqlserver_votes_partition_20260818_233438.log` |

---

## 3. Full Load plano — `dbo.Posts`

| Campo | Valor |
|-------|-------|
| Task | `tasks/full_load/sqlserver_posts.yaml` |
| Comando | `metro run tasks/full_load/sqlserver_posts.yaml` |
| Tabela origem | `dbo.Posts` (StackOverflow2010, 20 colunas + Body nvarchar(MAX)) |
| **Linhas processadas** | **3.729.195** |
| **Tempo** | **129,3 s** (23:35:59 → 23:38:08) |
| **Throughput** | **28.842 rows/s** |
| Parquet | `./local/raw/sqlserver_posts/` — 19 arquivos, **1.011,70 MB** |
| Log | `logs/full_load/sqlserver_posts_20260818_233559.log` |

---

## 4. Incremental Replace — `dbo.Comments`

`lookback_periods=19` (relativo a 2026) produz cutoff **2008-01-01**, cobrindo o dump 2008–2010.

| Campo | Valor |
|-------|-------|
| Task | `tasks/incremental_replace/sqlserver_comments_replace.yaml` |
| Comando | `metro run tasks/incremental_replace/sqlserver_comments_replace.yaml` |
| Cutoff | 2008-01-01 |
| **Linhas processadas** | **3.875.183** |
| **Tempo** | **36,6 s** (23:38:28 → 23:39:04) |
| **Throughput** | **105.879 rows/s** |
| Partições com dados | year=2008 (136.034), year=2009, year=2010 |
| Partições vazias removidas | 2011–2026 |
| Parquet particionado | `./local/raw/sqlserver_comments/year=XXXX/` — 21 arquivos, **267,57 MB** |
| Log | `logs/incremental_replace/sqlserver_comments_replace_20260818_233827.log` |

**Nota:** o Full Load anterior gravou 20 arquivos planos no mesmo `target_name`. O Replace escreveu as partições Hive, mas os arquivos planos permaneceram. O dataset completo no disco ficou com 41 arquivos / 7.750.366 linhas se ambos forem lidos. Para um Replace isolado, usar um `target_name` distinto ou limpar o diretório antes.

---

## 5. Incremental Append — tabela `dbo.test_watermark`

As tabelas reais do StackOverflow2010 estão estáticas. O Append usa `.watermark/tests/sqlserver/` para exercitar insert / update / delete.

Scripts T-SQL:

| Arquivo | Ação |
|---------|------|
| `00_create_table.sql` | Cria `dbo.test_watermark` |
| `01_insert_10.sql` | 10 registros iniciais |
| `02_insert_5.sql` | 5 inserts acima do watermark |
| `03_update_4.sql` | 4 updates (reaparecem como novas linhas) |
| `04_delete_1.sql` | 1 delete (não capturado) |
| `99_cleanup.sql` | Drop da tabela |

### 5.1 Plano — `sqlserver_test_watermark_append`

| Etapa | SQL | Linhas processadas | Watermark após |
|-------|-----|-------------------|----------------|
| 0 | `00_create_table.sql` + `01_insert_10.sql` | — | — |
| 1 | — | 10 | `2025-05-10 22:00:00` |
| 2 | `02_insert_5.sql` | 5 | `2025-05-15 17:45:00` |
| 3 | `03_update_4.sql` | 4 | `2025-05-16 12:00:00` |
| 4 | `04_delete_1.sql` | 0 (delete não capturado) | inalterado |

| Campo | Valor |
|-------|-------|
| Parquet acumulado | `./local/raw/sqlserver_test_watermark_append/` — 3 arquivos, **19 linhas** |
| Watermark final | `raw.sqlserver_test_watermark_append` / `last_update` = `2025-05-16 12:00:00` |
| Logs | `logs/incremental_append/sqlserver_test_watermark_append_20260818_234126.log` … `234348.log` |

### 5.2 Particionado — `sqlserver_test_watermark_append_partitioned`

| Etapa | Linhas | Partições afetadas | Watermark após |
|-------|--------|-------------------|----------------|
| 1 | 10 | year=2023 (3), 2024 (4), 2025 (3) | `2025-05-10 22:00:00` |
| 2 | 5 | year=2025 | `2025-05-15 17:45:00` |
| 3 | 4 | year=2025 | `2025-05-16 12:00:00` |
| 4 | 0 | — | inalterado |

| Campo | Valor |
|-------|-------|
| Parquet acumulado | `./local/raw/sqlserver_test_watermark_append_partitioned/` — 5 arquivos, **19 linhas** |
| Watermark final | `raw.sqlserver_test_watermark_append_partitioned` / `last_update` = `2025-05-16 12:00:00` |
| Logs | `logs/incremental_append/sqlserver_test_watermark_append_partitioned_20260818_234132.log` … `234352.log` |

**Semântica confirmada:** UPDATE reaparece como nova linha (19 no Parquet vs 14 na origem). DELETE não propaga.

---

## 6. Ajustes feitos durante a validação

- **Connection string:** `mssql-python` exige `UID`/`PWD` e `Encrypt=yes` (não `User Id`/`Password`/`True`). O `SQLServerSource` passou a normalizar aliases ADO.NET.
- **Lower bound datetime:** SQL Server com `DATEFORMAT dmy` interpretava o watermark string `2025-05-10` como outubro. O source agora converte strings ISO para `datetime` antes do bind.

---

## 7. Saídas Parquet (consolidado)

| Dataset | Caminho | Linhas | Arquivos | Tamanho |
|---------|---------|--------|----------|---------|
| sqlserver_comments (full load plano) | `./local/raw/sqlserver_comments/*.parquet` | 3.875.183 | 20 | **267,45 MB** |
| sqlserver_comments (replace) | `./local/raw/sqlserver_comments/year=XXXX/` | 3.875.183 | 21 | **267,57 MB** |
| sqlserver_votes | `./local/raw/sqlserver_votes/` | 10.143.364 | 52 | **48,70 MB** |
| sqlserver_posts | `./local/raw/sqlserver_posts/` | 3.729.195 | 19 | **1.011,70 MB** |
| sqlserver_test_watermark_append | `./local/raw/sqlserver_test_watermark_append/` | 19 | 3 | 0,01 MB |
| sqlserver_test_watermark_append_partitioned | `./local/raw/sqlserver_test_watermark_append_partitioned/` | 19 | 5 | 0,01 MB |

---

## 8. Cleanup (opcional)

```powershell
sqlcmd -S localhost -d StackOverflow2010 -U sa -C -i .watermark\tests\sqlserver\99_cleanup.sql
curl -X DELETE http://localhost:8000/watermarks/raw.sqlserver_test_watermark_append/last_update
curl -X DELETE http://localhost:8000/watermarks/raw.sqlserver_test_watermark_append_partitioned/last_update
```
