# METRO

## Motor de Extração, Transferência e Replicação de Objetos

Este README explica como instalar, configurar e executar o METRO. Arquitetura, domínio e roadmap estão em [MANIFEST.md](MANIFEST.md).

O **METRO** é um motor de replicação em Python para **Full Load** e **Incremental Load** de fontes **SQL/NoSQL** para storages (**Local**, futuramente **S3**), com **Parquet** como formato de persistência.

Não utiliza CDC, replication slots ou mensageria. Cada execução processa **uma tabela**.

---

## O que já funciona

- Full Load: PostgreSQL → Local (plano ou particionado Hive)
- Incremental Replace / Partition: PostgreSQL → Local
- Incremental Append / MaxValue: PostgreSQL → Local (via Watermark API)
- Escrita atômica via pasta `_tmp`
- CLI `metro run` (YAML ou flags)
- Secrets locais via `.env`
- Queries em `.metro/queries/` — ou query padrão (`SELECT *`) se `query_path` não for informado
- Logs no console e em `logs/`
- Watermark API local em `.watermark/`

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

## Configuração (`.env`)

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
| `development_storage` | `METRO_DEVELOPMENT_STORAGE_BASE_PATH` |
| *(API watermark)* | `METRO_WATERMARK_POSTGRES_DATABASE` |

---

## Como executar

Uma task por comando. O contrato vem de um YAML:

```powershell
metro run tasks/full_load/pagila_actor.yaml --secret-provider local
metro run tasks/full_load/pagila_film_full_partition.yaml --secret-provider local
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
| `tasks/incremental_replace/pagila_film_replace.yaml` | Incremental Replace |
| `tasks/incremental_append/stackoverflow_posts_append.yaml` | Incremental Append |

**Append** precisa da Watermark API no ar — setup em [`.watermark/README.md`](.watermark/README.md).

Opções úteis:

```powershell
metro run tasks/full_load/pagila_actor.yaml --secret-provider local --log-level DEBUG
metro run tasks/full_load/pagila_actor.yaml --secret-provider local --log-file logs/meu_run.log
```

| Argumento | Descrição | Default |
| --- | --- | --- |
| `task` | Caminho do YAML | — |
| `--secret-provider` | Provider de secrets (`local` por enquanto) | `local` |
| `--watermark-api-url` | URL da API de watermark | `http://localhost:8000` |
| `--log-level` | Nível de log | `INFO` |
| `--log-file` | Arquivo de log | `logs/<task>_<timestamp>.log` |

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
  --replication.mode full_load `
  --secret-provider local
```

Se um YAML for informado, as flags de contrato da CLI são ignoradas.

---

## Saídas

| Destino | Conteúdo |
| --- | --- |
| `./local/` | Parquets gerados |
| `./logs/` | Logs da execução |
| `.metro/queries/` | Queries referenciadas por `query_path` |
