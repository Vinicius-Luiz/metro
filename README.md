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
- PostgreSQL acessível (para os exemplos Pagila)

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

- **Settings** (`metro/settings.py`): infraestrutura do motor (logs, watermark API, query repository, provider de secrets).
- **Secrets** (`.env` / Secret Provider): connection strings e runtimes de cada task.

### Settings do METRO

Valores padrão vivem em `metro/settings.py`. Qualquer um pode ser sobrescrito por variável de ambiente com prefixo `METRO_`:

| Variável | Descrição | Default |
| --- | --- | --- |
| `METRO_SECRET_PROVIDER` | Provider de secrets (`local` por enquanto) | `local` |
| `METRO_LOG_LEVEL` | Nível de log | `INFO` |
| `METRO_LOG_DIR` | Diretório de logs | `logs` |
| `METRO_LOG_FILE` | Arquivo de log (se omitido, gera `logs/<modo>/<task>_<timestamp>.log`) | — |
| `METRO_WATERMARK_API_URL` | URL da API de watermark | `http://localhost:8000` |
| `METRO_WATERMARK_API_TIMEOUT` | Timeout HTTP da API de watermark (segundos) | `10` |
| `METRO_QUERY_REPOSITORY_BASE_DIR` | Diretório das queries | `.metro/queries` |
| `METRO_LOCAL_STORAGE_BASE_PATH` | Fallback do Target Local quando o secret não informa `base_path` | `./local` |

### Secrets (`.env`)

Crie um `.env` na raiz (não versionado). Modelo em `example.env`:

```env
METRO_PAGILA_POSTGRES_DATABASE="postgresql://user:password@localhost:5432/pagila"
METRO_DEVELOPMENT_STORAGE_BASE_PATH="./local"
METRO_WATERMARK_POSTGRES_DATABASE="postgresql://user:password@localhost:5432/metro_watermark"
```

O `runtime` do YAML vira `METRO_<RUNTIME_EM_UPPER_SNAKE>`. Sources de banco usam o padrão `<nome>_<database_type>_database`.

| YAML `runtime` | Variável no `.env` |
| --- | --- |
| `pagila_postgres_database` | `METRO_PAGILA_POSTGRES_DATABASE` |
| `stackoverflow_postgres_database` | `METRO_STACKOVERFLOW_POSTGRES_DATABASE` |
| `stackoverflow_sql_server_database` | `METRO_STACKOVERFLOW_SQL_SERVER_DATABASE` |
| `development_storage` | `METRO_DEVELOPMENT_STORAGE_BASE_PATH` |
| *(API watermark)* | `METRO_WATERMARK_POSTGRES_DATABASE` |

`METRO_WATERMARK_POSTGRES_DATABASE` é conexão do **serviço** da Watermark API (`.watermark/`), não um secret de task do motor.

### Connection String SQL Server

O SQL Server usa a biblioteca oficial `mssql-python`, que requer connection strings no formato:

```env
METRO_STACKOVERFLOW_SQL_SERVER_DATABASE="Server=localhost;Database=StackOverflow2010;UID=sa;PWD=senha;TrustServerCertificate=yes;Encrypt=yes;"
```

**Importante**: Não inclua `Driver={...}` na connection string — o `mssql-python` gerencia drivers automaticamente. Aliases ADO.NET (`User Id`, `Password`, `True`/`False`) também são aceitos.

---

## Como executar

Uma task por comando. O contrato vem de um YAML:

```powershell
metro run tasks/full_load/pagila_actor.yaml
metro run tasks/full_load/pagila_film_full_partition.yaml
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
| `tasks/full_load/pagila_actor.yaml` | Full Load |
| `tasks/full_load/pagila_film_full_partition.yaml` | Full Load particionado |
| `tasks/full_load/sqlserver_comments.yaml` | Full Load (SQL Server) |
| `tasks/incremental_replace/pagila_film_replace.yaml` | Incremental Replace |
| `tasks/incremental_append/stackoverflow_posts_append.yaml` | Incremental Append |

**Append** precisa da Watermark API no ar — setup em [`.watermark/README.md`](.watermark/README.md). Logging e URL da API vêm de `metro/settings.py` (ou das variáveis `METRO_*` correspondentes).

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

Incremental **Replace** exige `strategy` com `type: replace`, `reference_column`, `lookback_periods` e `partition`. Incremental **Append** exige `strategy` com `type: append` e `reference_column`. Contratos completos estão em `tasks/`.

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
