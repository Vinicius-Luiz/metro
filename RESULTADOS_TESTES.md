# Resultados — Super Teste METRO (tabelas pesadas)

**Data da execução:** 2026-08-18  
**Ambiente:** local (PostgreSQL stackoverflow, Watermark API em `http://localhost:8000`)  
**Todos os modos concluíram com exit code 0.**

---

## Performance — Tempo de Processamento

Tempos medidos pelos timestamps do log (primeiro → último evento da execução).

| Tabela | Linhas | Modo | **Tempo** | **Throughput** | Parquet |
|--------|--------|------|-----------|----------------|---------|
| **votes** | **9.463.619** | Full Load particionado (year) | **14,2 s** | **668.241 rows/s** | 59,05 MB (49 arquivos) |
| **posts** | **3.680.688** | Full Load plano | **59,7 s** | **61.607 rows/s** | 1.012,33 MB (19 arquivos) |
| **comments** | **3.353.493** | Incremental Replace (year, lookback=19) | **15,9 s** | **210.739 rows/s** | 259,06 MB (18 arquivos) |
| **test_watermark** | 10 → 19 | Incremental Append (plano + particionado) | ~4–5 s / etapa | n/a (tabela de teste) | 0,01 MB |

**Destaque:** `votes` (9,5M linhas, 6 colunas) foi a mais rápida em throughput. `posts` (3,7M com `body` TEXT) foi a mais lenta em rows/s e gerou ~1 GB de Parquet.

---

## 1. Full Load plano — `public.posts`

| Campo | Valor |
|-------|-------|
| Task | `tasks/full_load/stackoverflow_posts.yaml` |
| Comando | `metro run tasks/full_load/stackoverflow_posts.yaml --secret-provider local` |
| Tabela origem | `public.posts` (stackoverflow, 23 colunas + body TEXT) |
| **Linhas processadas** | **3.680.688** |
| **Tempo** | **59,7 s** (00:54:28 → 00:55:28) |
| **Throughput** | **61.607 rows/s** |
| Parquet | `./local/raw/stackoverflow_posts/` — 19 arquivos, **1.012,33 MB** |
| Log | `logs/full_load/stackoverflow_posts_20260818_005428.log` |

---

## 2. Full Load particionado — `public.votes`

| Campo | Valor |
|-------|-------|
| Task | `tasks/full_load/stackoverflow_votes_partition.yaml` |
| Comando | `metro run tasks/full_load/stackoverflow_votes_partition.yaml --secret-provider local` |
| Tabela origem | `public.votes` (stackoverflow, 6 colunas) |
| Partição | `year` em `creationdate` |
| **Linhas processadas** | **9.463.619** |
| **Tempo** | **14,2 s** (00:55:40 → 00:55:54) |
| **Throughput** | **668.241 rows/s** |
| Partições | `year=2008`, `year=2009`, `year=2010` (49 arquivos) |
| Parquet | `./local/raw/stackoverflow_votes/` — **59,05 MB** |
| Log | `logs/full_load/stackoverflow_votes_partition_20260818_005540.log` |

---

## 3. Incremental Replace — `public.comments`

`lookback_periods` é relativo à data atual (2026). Com lookback=3 o cutoff caiu em 2024-01-01 e **não leu nenhum comentário** (dataset 2008–2010). Ajustado para **lookback=19** (cutoff 2008-01-01) para cobrir a tabela inteira.

### 3.1 Tentativa lookback=3 (0 linhas)

| Campo | Valor |
|-------|-------|
| Cutoff | 2024-01-01 |
| Partições | year=2024, 2025, 2026 (todas vazias) |
| Linhas | 0 |
| Tempo | 0,3 s |
| Log | `logs/incremental_replace/stackoverflow_comments_replace_20260818_005606.log` |

### 3.2 Execução lookback=19 (carga pesada)

| Campo | Valor |
|-------|-------|
| Task | `tasks/incremental_replace/stackoverflow_comments_replace.yaml` |
| Comando | `metro run tasks/incremental_replace/stackoverflow_comments_replace.yaml --secret-provider local` |
| Cutoff | 2008-01-01 |
| **Linhas processadas** | **3.353.493** |
| **Tempo** | **15,9 s** (00:56:26 → 00:56:42) |
| **Throughput** | **210.739 rows/s** |
| Partições com dados | year=2008 (117.657), year=2009, year=2010 |
| Partições vazias removidas | 2011–2026 |
| Parquet | `./local/raw/stackoverflow_comments/` — 18 arquivos, **259,06 MB** |
| Log | `logs/incremental_replace/stackoverflow_comments_replace_20260818_005626.log` |

---

## 4. Incremental Append — tabela personalizada `public.test_watermark`

As tabelas reais do stackoverflow estão frias (sem mutações). O Append usa `.watermark/tests/` para exercitar insert / update / delete.

### 4.1 Plano — `test_watermark_append`

| Etapa | SQL | Linhas processadas | Watermark após |
|-------|-----|-------------------|----------------|
| 0 | `00_create_table.sql` + `01_insert_10.sql` | — | — |
| 1 | — | 10 | `2025-05-10 22:00:00` |
| 2 | `02_insert_5.sql` | 5 | `2025-05-15 17:45:00` |
| 3 | `03_update_4.sql` | 4 | `2025-05-16 12:00:00` |
| 4 | `04_delete_1.sql` | 0 (delete não capturado) | inalterado |

| Campo | Valor |
|-------|-------|
| Parquet acumulado | `./local/raw/test_watermark_append/` — 3 arquivos, **19 linhas** |
| Watermark final | `raw.test_watermark_append` / `last_update` = `2025-05-16 12:00:00` |
| Logs | `logs/incremental_append/test_watermark_append_20260818_005715.log` … `005821.log` |

### 4.2 Particionado — `test_watermark_append_partitioned`

| Etapa | Linhas | Partições afetadas | Watermark após |
|-------|--------|-------------------|----------------|
| 1 | 10 | year=2023 (3), 2024 (4), 2025 (3) | `2025-05-10 22:00:00` |
| 2 | 5 | year=2025 | `2025-05-15 17:45:00` |
| 3 | 4 | year=2025 | `2025-05-16 12:00:00` |
| 4 | 0 | — | inalterado |

| Campo | Valor |
|-------|-------|
| Parquet acumulado | `./local/raw/test_watermark_append_partitioned/` — 5 arquivos, **19 linhas** |
| Watermark final | `raw.test_watermark_append_partitioned` / `last_update` = `2025-05-16 12:00:00` |
| Logs | `logs/incremental_append/test_watermark_append_partitioned_20260818_005720.log` … `005824.log` |

**Semântica confirmada:** UPDATE reaparece como nova linha (19 no Parquet vs 14 na origem). DELETE não propaga.

---

## 5. Saídas Parquet (consolidado)

| Dataset | Caminho | Linhas | Arquivos | Tamanho |
|---------|---------|--------|----------|---------|
| stackoverflow_posts | `./local/raw/stackoverflow_posts/` | 3.680.688 | 19 | **1.012,33 MB** |
| stackoverflow_votes | `./local/raw/stackoverflow_votes/` | 9.463.619 | 49 | **59,05 MB** |
| stackoverflow_comments | `./local/raw/stackoverflow_comments/` | 3.353.493 | 18 | **259,06 MB** |
| test_watermark_append | `./local/raw/test_watermark_append/` | 19 | 3 | 0,01 MB |
| test_watermark_append_partitioned | `./local/raw/test_watermark_append_partitioned/` | 19 | 5 | 0,01 MB |

---

## 6. Cleanup (opcional)

```powershell
psql -U postgres -h localhost -p 5432 -d stackoverflow -f .watermark/tests/99_cleanup.sql
curl -X DELETE http://localhost:8000/watermarks/raw.test_watermark_append/last_update
curl -X DELETE http://localhost:8000/watermarks/raw.test_watermark_append_partitioned/last_update
```
