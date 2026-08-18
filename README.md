# METRO

## Motor de Extração, Transferência e Replicação de Objetos

O **METRO** é um motor de replicação de dados em Python para **Full Load** e **Incremental Load** de fontes **SQL/NoSQL** para storages (**Local**, futuramente **S3**), com **Parquet** como formato de persistência.

Não utiliza CDC, replication slots ou mensageria. Cada execução processa **uma tabela** (modelo alinhado a container/instância única).

---

## O que já funciona

- Full Load: **PostgreSQL → Local** (plano ou particionado Hive)
- Incremental Replace / Partition: **PostgreSQL → Local**
- Incremental Append / MaxValue: **PostgreSQL → Local** (via Watermark API)
- Escrita atômica via pasta `_tmp` (promove só ao final)
- CLI: `metro run`
- Secrets locais via `.env`
- Queries externas em `.metro/queries/`
- Query padrão automática quando `query_path` não é informado
- Logs no console e em `logs/`
- Watermark API local (`.watermark/`) sobre PostgreSQL externo

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

# Watermark API (infraestrutura externa)
METRO_WATERMARK_POSTGRES_DATABASE="postgresql://user:password@localhost:5432/metro_watermark"
```

Convenção: `runtime` do YAML vira `METRO_<RUNTIME_EM_UPPER_SNAKE>`.

Padrão de nome para Sources de banco: `<nome>_<database_type>_database`.


| YAML `runtime`                   | Variável no `.env`                           |
| -------------------------------- | -------------------------------------------- |
| `pagila_postgres_database`       | `METRO_PAGILA_POSTGRES_DATABASE`             |
| `stackoverflow_postgres_database`| `METRO_STACKOVERFLOW_POSTGRES_DATABASE`      |
| `stackoverflow_sql_server_database` | `METRO_STACKOVERFLOW_SQL_SERVER_DATABASE` |
| `development_storage`            | `METRO_DEVELOPMENT_STORAGE_BASE_PATH`        |
| *(API watermark)*                | `METRO_WATERMARK_POSTGRES_DATABASE`          |


---



## Como executar

Ative o venv e rode **uma task por comando** (1 tabela = 1 execução):

```powershell
metro run tasks/full_load/pagila_actor.yaml --secret-provider local
metro run tasks/full_load/pagila_film_full_partition.yaml --secret-provider local
```

Organização das tasks:

```text
tasks/
├── full_load/
├── incremental_replace/
└── incremental_append/
```



### Opções úteis

```powershell
metro run tasks/full_load/pagila_actor.yaml --secret-provider local --log-level DEBUG
metro run tasks/full_load/pagila_actor.yaml --secret-provider local --log-file logs/meu_run.log
```


| Argumento              | Descrição                                                          |
| ---------------------- | ------------------------------------------------------------------ |
| `task`                 | Caminho do YAML (obrigatório; exatamente um)                       |
| `--secret-provider`    | Provider de secrets (`local` por enquanto)                         |
| `--watermark-api-url`  | URL da API de watermark (padrão: `http://localhost:8000`)          |
| `--log-level`          | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`                    |
| `--log-file`           | Caminho customizado do log (padrão: `logs/<task>_<timestamp>.log`) |


---



## Tasks de exemplo

YAMLs prontos em `tasks/`. Abra o arquivo correspondente para ver o contrato completo.


| Task | Modo |
| ---- | ---- |
| `tasks/full_load/pagila_actor.yaml` | Full Load (query automática) |
| `tasks/full_load/pagila_film_full_partition.yaml` | Full Load particionado |
| `tasks/incremental_replace/pagila_film_replace.yaml` | Incremental Replace |
| `tasks/incremental_append/stackoverflow_posts_append.yaml` | Incremental Append |

Estrutura mínima de um contrato:

```yaml
table:
  schema_name: public
  name: actor
  target_schema_name: raw
  target_name: pagila_actor

source:
  type: postgresql
  runtime: pagila_postgres_database
  # query_path: film.sql   # opcional; sem isso, o Source monta SELECT * FROM schema.table

target:
  type: local
  runtime: development_storage

replication:
  mode: full_load          # full_load | incremental
  # partition:             # opcional (Full Load e Append)
  #   type: year
  #   reference_column: last_update
  # strategy:              # obrigatório em incremental
  #   type: replace       # replace | append
  #   reference_column: last_update
  #   lookback_periods: 5 # replace
  #   partition:           # obrigatório em replace
  #     type: year
```

**Append** exige a Watermark API no ar — setup em [`.watermark/README.md`](.watermark/README.md).

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
  → Full Load / Incremental Replace / Incremental Append
  → Parquet (plano ou particionado Hive, via _tmp)
  → Commit atômico no Target Local (./local)
  → Watermark API (somente Append)
```

## Particionamento Hive

Todos os 3 modos suportam particionamento Hive (`year`/`month`/`day`):

- **Full Load**: `replication.partition` (opcional)
- **Incremental Replace**: `strategy.partition` (obrigatório) + `lookback_periods`
- **Incremental Append**: `strategy.partition` (opcional)

A lógica de layout Hive e de materialização é compartilhada entre as strategies
(`metro/replication/partitioning.py` e `metro/replication/writer.py`):

- escrita plana em batches → `write_batched`
- escrita particionada Hive → `write_partitioned`

---



## Conceitos fundamentais

- **Source Endpoint** — obtém os dados
- **Target Endpoint** — materializa os dados
- **Table** — identidade e metadados do dataset
- **Replication Strategy** — Full Load / Incremental Append / Incremental Replace
- **Query Repository** — resolve `query_path`
- **Secret Provider** — resolve `runtime`
- **Watermark Client** — consome a API externa de watermarks (Append)
- **Polars / Parquet** — processamento e persistência

