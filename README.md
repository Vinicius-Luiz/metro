# METRO

## Motor de Extração, Transferência e Replicação de Objetos

O **METRO** é um motor de replicação de dados em Python para **Full Load** e **Incremental Load** de fontes **SQL/NoSQL** para storages (**Local**, futuramente **S3**), com **Parquet** como formato de persistência.

Não utiliza CDC, replication slots ou mensageria. Cada execução processa **uma tabela** (modelo alinhado a container/instância única).

---

## O que já funciona

- Full Load: **PostgreSQL → Local** (plano ou particionado Hive)
- Incremental Replace / Partition: **PostgreSQL → Local**
- Escrita atômica via pasta `_tmp` (promove só ao final)
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
# Connection string PostgreSQL (runtime = pagila_postgres_database)
METRO_PAGILA_POSTGRES_DATABASE="postgresql://user:password@localhost:5432/pagila"

# Target Local (runtime = development_storage)
METRO_DEVELOPMENT_STORAGE_BASE_PATH="./local"
```

Convenção: `runtime` do YAML vira `METRO_<RUNTIME_EM_UPPER_SNAKE>`.

Padrão de nome para Sources de banco: `<nome>_<database_type>_database`.


| YAML `runtime`                   | Variável no `.env`                           |
| -------------------------------- | -------------------------------------------- |
| `pagila_postgres_database`       | `METRO_PAGILA_POSTGRES_DATABASE`             |
| `stackoverflow_postgres_database`| `METRO_STACKOVERFLOW_POSTGRES_DATABASE`      |
| `stackoverflow_sql_server_database` | `METRO_STACKOVERFLOW_SQL_SERVER_DATABASE` |
| `development_storage`            | `METRO_DEVELOPMENT_STORAGE_BASE_PATH`        |


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


| Argumento           | Descrição                                                          |
| ------------------- | ------------------------------------------------------------------ |
| `task`              | Caminho do YAML (obrigatório; exatamente um)                       |
| `--secret-provider` | Provider de secrets (`local` por enquanto)                         |
| `--log-level`       | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`                    |
| `--log-file`        | Caminho customizado do log (padrão: `logs/<task>_<timestamp>.log`) |


---



## Exemplos de task



### Com `query_path` — `tasks/pagila_film.yaml`

Usa a query em `.metro/queries/film.sql`.

```yaml
table:
  schema_name: public
  name: film
  target_schema_name: raw
  target_name: pagila_film

source:
  type: postgresql
  runtime: pagila_postgres_database
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
  target_schema_name: raw
  target_name: pagila_actor

source:
  type: postgresql
  runtime: pagila_postgres_database

target:
  type: local
  runtime: development_storage

replication:
  mode: full_load
```



### Full Load particionado — `tasks/pagila_film_full_partition.yaml`

Grava o dataset inteiro no layout Hive, compatível com um Replace posterior
na mesma tabela. Use a **mesma** `reference_column` e granularidade no Replace.

```yaml
table:
  schema_name: public
  name: film
  target_schema_name: raw
  target_name: pagila_film

source:
  type: postgresql
  runtime: pagila_postgres_database
  query_path: film.sql

target:
  type: local
  runtime: development_storage

replication:
  mode: full_load
  partition:
    type: year
    reference_column: last_update
```



### Incremental Replace / Partition — `tasks/pagila_film_replace.yaml`

Reconstrói as últimas N partições (Hive) pela coluna de data. A escrita
ocorre em `dataset/_tmp/` e só é promovida ao final (atomicidade).

Fluxo típico: Full Load particionado → Replace periódico (mesma coluna/granularidade).

```yaml
table:
  schema_name: public
  name: film
  target_schema_name: raw
  target_name: pagila_film

source:
  type: postgresql
  runtime: pagila_postgres_database
  query_path: film.sql

target:
  type: local
  runtime: development_storage

replication:
  mode: incremental
  strategy:
    type: replace
    reference_column: last_update
    lookback_periods: 5
    partition:
      type: year
```

Layout de saída (exemplo com `partition.type: year`):

```text
local/raw/pagila_film/
├── year=2022/
│   └── part_0001.parquet
├── year=2023/
├── year=2024/
├── year=2025/
└── year=2026/
```

---



## Saídas


| Destino           | Conteúdo                                                                          |
| ----------------- | --------------------------------------------------------------------------------- |
| `./local/`        | Parquets gerados (ex.: `raw/pagila_film/year=YYYY/*.parquet`)                     |
| `./logs/`         | Logs da execução (console + arquivo)                                              |
| `.metro/queries/` | Arquivos de query referenciados por `query_path`                                  |


---



## Fluxo resumido

```text
YAML (1 tabela)
  → SecretProvider (.env)
  → Source (query_path ou query padrão)
  → Polars DataFrame
  → Full Load / Incremental Replace
  → Parquet (via _tmp, depois promove)
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

