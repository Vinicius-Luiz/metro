# METRO

## Motor de Extração, Transferência e Replicação de Objetos

Este README explica como instalar, configurar e executar o METRO. Arquitetura, domínio e roadmap estão em [MANIFEST.md](MANIFEST.md).

O **METRO** é um motor de replicação em Python para **Full Load** e **Incremental Load** de fontes **SQL/NoSQL** para storages (**Local**, futuramente **S3**), com **Parquet** como formato de persistência.

Não utiliza CDC, replication slots ou mensageria. Cada execução processa **uma tabela**.

---

## O que já funciona

PostgreSQL e SQL Server → Local (Full Load, Incremental Replace/Partition, Incremental Append/MaxValue). Escrita atômica via `_tmp`. CLI `metro run`.

---

## Pré-requisitos

- Python **3.10+**

---

## Instalação

```powershell
# Na raiz do repositório
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -e .
```

Ou: `pip install -r requirements.txt`

---

## Configuração

Há duas camadas distintas:

- **Settings** (`metro/settings.py`): infraestrutura do motor (logs, watermark API, query repository, provider de secrets). Não lê o `.env`.
- **Secrets** (`.env` / Secret Provider): somente credenciais — connection strings e secrets de runtime.

### Settings do METRO

Valores padrão vivem em `metro/settings.py` e devem ser alterados **editando esse arquivo**. Não são sobrescritos por variáveis de ambiente.

| Parâmetro | Descrição | Default |
| --- | --- | --- |
| `secret_provider` | Provider de secrets (`local` por enquanto) | `local` |
| `log_level` | Nível de log | `INFO` |
| `log_dir` | Diretório de logs | `logs` |
| `log_file` | Arquivo de log (se omitido, gera `logs/<modo>/<task>_<timestamp>.log`) | — |
| `logging_enabled` | Habilitar envio de logs para a Logging API | `True` |
| `logging_api_url` | URL da API de logging | `http://localhost:8001` |
| `logging_api_timeout` | Timeout HTTP da API de logging (segundos) | `10` |
| `watermark_enabled` | Habilitar watermark (necessário para incremental append) | `True` |
| `watermark_api_url` | URL da API de watermark | `http://localhost:8000` |
| `watermark_api_timeout` | Timeout HTTP da API de watermark (segundos) | `10` |
| `query_repository_base_dir` | Diretório das queries | `.metro/queries` |
| `local_storage_base_path` | Fallback do Target Local quando o secret não informa `base_path` | `./local` |

### Secrets (`.env`)

O `.env` guarda **somente credenciais**. Configuração do motor (nível de log, URL da API, diretórios) não entra nesse arquivo.

Crie um `.env` na raiz (não versionado). Modelo em `example.env`:

```env
METRO_PAGILA_POSTGRES_DATABASE="postgresql://user:password@localhost:5432/pagila"
METRO_DEVELOPMENT_STORAGE_BASE_PATH="./local"
METRO_WATERMARK_DATABASE="postgresql://user:password@localhost:5432/metro_watermark"
METRO_LOGGING_DATABASE="postgresql://user:password@localhost:5432/metro_logging"
```

O `runtime` do YAML vira `METRO_<RUNTIME_EM_UPPER_SNAKE>`. Sources de banco usam o padrão `<nome>_<database_type>_database`.

| YAML `runtime` | Variável no `.env` |
| --- | --- |
| `pagila_postgres_database` | `METRO_PAGILA_POSTGRES_DATABASE` |
| `stackoverflow_postgres_database` | `METRO_STACKOVERFLOW_POSTGRES_DATABASE` |
| `stackoverflow_sql_server_database` | `METRO_STACKOVERFLOW_SQL_SERVER_DATABASE` |
| `development_storage` | `METRO_DEVELOPMENT_STORAGE_BASE_PATH` |
| *(API watermark)* | `METRO_WATERMARK_DATABASE` |
| *(API logging)* | `METRO_LOGGING_DATABASE` |

`METRO_WATERMARK_DATABASE` e `METRO_LOGGING_DATABASE` são conexões dos **serviços** (`.watermark/` e `.logging/`), não secrets de task do motor.

---

## Como executar

Uma task por comando. O contrato vem de um YAML:

```powershell
metro run tasks/full_load/postgresql_pagila_actor.yaml
metro run tasks/full_load/sqlserver_comments.yaml
```

YAMLs de exemplo:

```text
tasks/
├── full_load/
├── incremental_replace/
└── incremental_append/
```

| Task | Modo |
| --- | --- |
| `tasks/full_load/postgresql_pagila_actor.yaml` | Full Load |
| `tasks/full_load/postgresql_stackoverflow_votes_partition.yaml` | Full Load particionado |
| `tasks/full_load/sqlserver_comments.yaml` | Full Load (SQL Server) |
| `tasks/incremental_replace/sqlserver_comments_replace.yaml` | Incremental Replace |
| `tasks/incremental_append/postgresql_stackoverflow_posts_append.yaml` | Incremental Append |

**Append** precisa da Watermark API no ar e `watermark_enabled=True` em `metro/settings.py` — setup em [`.watermark/README.md`](.watermark/README.md). Logging PostgreSQL exige `logging_enabled=True` e a Logging API — setup em [`.logging/README.md`](.logging/README.md).

---

## Contrato YAML

```yaml
table:
  schema_name: public
  name: actor
  target_schema_name: raw
  target_name: pagila_actor

source:
  type: postgresql
  runtime: pagila_postgres_database
  # query_path: film.sql   # opcional; sem isso, SELECT * FROM schema.table

target:
  type: local
  runtime: development_storage

replication:
  mode: full_load          # full_load | incremental
```

Incremental **Replace** exige `strategy` (`type: replace`, `reference_column`, `lookback_periods`) e `replication.partition`. Incremental **Append** exige `strategy` com `type: append` e `reference_column`; `replication.partition` é opcional. Contratos completos estão em `tasks/`.

---

## Execução sem YAML

As flags espelham o YAML: ponto (`.`) separa níveis (`--replication.strategy.type`) e hífen (`-`) separa palavras (`--reference-column`). Lista completa: `metro run --help`.

```powershell
metro run `
  --table.name actor `
  --table.target-schema raw `
  --table.target-name pagila_actor `
  --source.type postgresql `
  --source.runtime pagila_postgres_database `
  --target.type local `
  --target.runtime development_storage `
  --replication.mode full_load
```

Se um YAML for informado, as flags de contrato da CLI são ignoradas.

---

## Saídas

| Destino | Conteúdo |
| --- | --- |
| `./local/` | Parquets gerados |
| `./logs/` | Logs da execução |
| `.metro/queries/` | Queries referenciadas por `query_path` |
