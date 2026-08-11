# METRO

## Motor de Extração, Transferência e Replicação de Objetos

O **METRO** é um motor de replicação de dados desenvolvido em Python, projetado para realizar **Full Load** e **Incremental Load** de dados provenientes de fontes **SQL e NoSQL** para **storages**, utilizando **Parquet** como formato de persistência.

O projeto é concebido para execução local durante o desenvolvimento e, posteriormente, em ambientes containerizados e orquestrados, como **AWS ECS**.

O METRO não utiliza CDC, leitura de logs, replication slots ou mensageria para realizar a replicação. O processo incremental é baseado em estratégias definidas na própria tarefa de replicação.

---

## Conceitos Fundamentais

O METRO possui uma separação clara entre:

- **Source Endpoint** — responsável por obter os dados.
- **Target Endpoint** — responsável por materializar os dados.
- **Table** — representa o dataset lógico que será replicado.
- **Column** — representa metadados das colunas do dataset.
- **Replication Strategy** — determina como os dados serão replicados.
- **Checkpoint** — representa o estado de uma execução incremental.
- **Query Repository** — resolve arquivos de consulta utilizados pelo Source.
- **Polars** — representa o núcleo de processamento dos datasets dentro do METRO.
- **Parquet** — formato utilizado para persistência dos dados no Target.

O METRO não tenta transformar internamente estruturas específicas de cada fonte. O Source é responsável por entregar os dados já na forma desejada, enquanto o METRO se responsabiliza pela ingestão e materialização.

---

# Arquitetura Base

```text
                         ┌─────────────────────┐
                         │        METRO        │
                         │                     │
                         │  Replication Engine │
                         └──────────┬──────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    │               │               │
                    ▼               ▼               ▼
                 Source           Table          Target
                 Endpoint                         Endpoint
                    │                               │
             ┌──────┴──────┐                 ┌──────┴──────┐
             │             │                 │             │
            SQL          NoSQL              S3           Local
             │             │                 │             │
             └──────┬──────┘                 └──────┬──────┘
                    │                               │
                    ▼                               │
             Query Repository                       │
                    │                               │
                    ▼                               │
                 Source                             │
                    │                               │
                    ▼                               │
                 POLARS                             │
              DataFrame                             │
                    │                               │
                    ▼                               │
          Replication Strategy                      │
                    │                               │
              ┌─────┴─────┐                         │
              │           │                         │
           Full Load  Incremental                   │
                         │                          │
                   ┌─────┴─────┐                    │
                   │           │                    │
                Append       Replace                │
                   │           │                    │
               MaxValue    Partition                │
                   │           │                    │
                   └─────┬─────┘                    │
                         │                          │
                         ▼                          ▼
                       Parquet ──────────────────► Target
```