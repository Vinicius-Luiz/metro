# Logging API (infraestrutura externa)

API HTTP minimalista para registro de **execuções** do METRO (1 linha por `metro run`).

## Setup

### 1. Criar database e schema

Instalação nova:

```powershell
psql -U postgres -h localhost -p 5432 -f setup_logging.sql
```

Recriar a tabela `logging.executions` (apaga dados existentes):

```powershell
psql -U postgres -h localhost -p 5432 -d metro_logging -f migrate_to_executions.sql
```

### 2. Configurar credencial no `.env` (na raiz do METRO)

Somente a connection string (credencial do serviço).

```env
METRO_LOGGING_DATABASE="postgresql://postgres:senha@localhost:5432/metro_logging"
```

### 3. Instalar dependências e iniciar API

```powershell
cd .logging
..\venv\Scripts\pip.exe install -r requirements.txt
..\venv\Scripts\uvicorn.exe app:app --port 8001
```

API disponível em `http://localhost:8001`

### 4. Habilitar no METRO

Em [`metro/settings.py`](../metro/settings.py):

```python
logging_enabled: bool = True
logging_api_url: str | None = "http://localhost:8001"
```

Se `logging_enabled` for `False` (ou `logging_api_url` for `None`), o METRO não registra execuções (sem erro). Console e arquivo de log continuam ativos.

## Endpoints

- `GET /health` — Health check
- `POST /executions` — Criar execução (`status=running`)
- `PATCH /executions/{id}` — Atualizar execução (status, rows, duration)
- `GET /executions/{id}` — Obter execução
- `GET /executions?skip=0&limit=100&status=success&mode=incremental&strategy_type=append` — Listar

## Tabela

Schema `logging.executions` (sem JSONB — todos os parâmetros em colunas):

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | SERIAL | PK |
| `started_at` | TIMESTAMP | Início da execução |
| `finished_at` | TIMESTAMP | Fim da execução |
| `status` | VARCHAR(255) | `running`, `success`, `error`, `cancelled` |
| `schema_name` | VARCHAR(255) | Schema da tabela na origem (opcional) |
| `name` | VARCHAR(255) | Nome da tabela/collection na origem |
| `target_schema_name` | VARCHAR(255) | Schema/pasta no target |
| `target_name` | VARCHAR(255) | Nome do dataset no target |
| `mode` | VARCHAR(255) | `full_load` ou `incremental` |
| `source_type` | VARCHAR(255) | Tipo do source |
| `source_runtime` | VARCHAR(255) | Runtime do source |
| `target_type` | VARCHAR(255) | Tipo do target |
| `target_runtime` | VARCHAR(255) | Runtime do target |
| `strategy_type` | VARCHAR(255) | `append` / `replace` (NULL em full_load) |
| `strategy_reference_column` | VARCHAR(255) | Coluna da strategy (NULL em full_load) |
| `strategy_lookback_periods` | INT | Lookback (NULL fora de replace) |
| `partition_type` | VARCHAR(255) | `year` / `month` / `day` (NULL sem partition) |
| `partition_reference_column` | VARCHAR(255) | Coluna da partition (NULL sem partition) |
| `rows_processed` | BIGINT | Total de registros processados |
| `duration_seconds` | NUMERIC | Duração em segundos |

## Exemplo de consulta

```powershell
Invoke-RestMethod "http://localhost:8001/executions?status=success&limit=10"
Invoke-RestMethod "http://localhost:8001/executions?schema_name=public&name=actor"
Invoke-RestMethod "http://localhost:8001/executions?mode=incremental&strategy_type=append"
```

```sql
SELECT id, status, schema_name, name, mode, strategy_type,
       partition_type, rows_processed, duration_seconds
FROM logging.executions
ORDER BY started_at DESC
LIMIT 20;
```
