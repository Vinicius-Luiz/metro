# METRO

## Motor de Extração, Transferência e Replicação de Objetos

O **METRO** é um motor de replicação de dados em Python para **Full Load** e **Incremental Load** de fontes **SQL/NoSQL** para storages (**Local**, futuramente **S3**), com **Parquet** como formato de persistência.

Não utiliza CDC, replication slots ou mensageria. Cada execução processa **uma tabela** (modelo alinhado a container/instância única).

---

## O que já funciona

- Full Load: **PostgreSQL → Local**
- CLI: `metro run`
- Secrets locais via `.env`
- Queries externas em `.metro/queries/`
- Query padrão automática quando `query_path` não é informado
- Logs no console e em `logs/`

Detalhes de arquitetura e roadmap: [MANIFEST.md](MANIFEST.md).

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

Ou:

```powershell
pip install -r requirements.txt
```

---

## Configuração (`.env`)

Crie um `.env` na raiz (não versionado). Exemplo:

```env
# Connection string PostgreSQL (runtime = pagila_database)
METRO_PAGILA_DATABASE="postgresql://user:password@localhost:5432/pagila"

# Target Local (runtime = development_storage)
METRO_DEVELOPMENT_STORAGE_BASE_PATH="./local"
```

Convenção: `runtime` do YAML vira `METRO_<RUNTIME_EM_UPPER_SNAKE>`.

| YAML `runtime` | Variável no `.env` |
|---|---|
| `pagila_database` | `METRO_PAGILA_DATABASE` |
| `development_storage` | `METRO_DEVELOPMENT_STORAGE_BASE_PATH` |

---

## Como executar

Ative o venv e rode **uma task por comando** (1 tabela = 1 execução):

```powershell
metro run tasks/pagila_film.yaml --secret-provider local
metro run tasks/pagila_actor.yaml --secret-provider local
```

### Opções úteis

```powershell
metro run tasks/pagila_film.yaml --secret-provider local --log-level DEBUG
metro run tasks/pagila_film.yaml --secret-provider local --log-file logs/meu_run.log
```

| Argumento | Descrição |
|---|---|
| `task` | Caminho do YAML (obrigatório; exatamente um) |
| `--secret-provider` | Provider de secrets (`local` por enquanto) |
| `--log-level` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `--log-file` | Caminho customizado do log (padrão: `logs/<task>_<timestamp>.log`) |

---

## Exemplos de task

### Com `query_path` — `tasks/pagila_film.yaml`

Usa a query em `.metro/queries/film.sql`.

```yaml
table:
  schema_name: public
  name: film

source:
  type: postgresql
  runtime: pagila_database
  query_path: film.sql

target:
  type: local
  runtime: development_storage

replication:
  mode: full_load
```

### Sem `query_path` — `tasks/pagila_actor.yaml`

O Source monta automaticamente:

`SELECT "col1", "col2", ... FROM "schema"."table"`

```yaml
table:
  schema_name: public
  name: actor

source:
  type: postgresql
  runtime: pagila_database

target:
  type: local
  runtime: development_storage

replication:
  mode: full_load
```

---

## Saídas

| Destino | Conteúdo |
|---|---|
| `./local/` | Parquets gerados (ex.: `public.film.parquet`, `public.actor.parquet`) |
| `./logs/` | Logs da execução (console + arquivo) |
| `.metro/queries/` | Arquivos de query referenciados por `query_path` |

---

## Fluxo resumido

```text
YAML (1 tabela)
  → SecretProvider (.env)
  → Source (query_path ou query padrão)
  → Polars DataFrame
  → Full Load
  → Parquet
  → Target Local (./local)
```

---

## Conceitos fundamentais

- **Source Endpoint** — obtém os dados
- **Target Endpoint** — materializa os dados
- **Table / Column** — identidade e metadados do dataset
- **Replication Strategy** — Full Load / Incremental
- **Query Repository** — resolve `query_path`
- **Secret Provider** — resolve `runtime`
- **Polars / Parquet** — processamento e persistência
