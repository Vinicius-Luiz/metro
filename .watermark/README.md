# Watermark API (infraestrutura externa)

API HTTP minimalista para gerenciamento de watermarks do METRO (estratégia Append/MaxValue).

## Setup

### 1. Criar database e schema

```powershell
psql -U postgres -h localhost -p 5432 -f setup_watermark.sql
```

### 2. Configurar `.env` (na raiz do METRO)

```env
METRO_WATERMARK_POSTGRES_DATABASE="postgresql://postgres:senha@localhost:5432/metro_watermark"
```

### 3. Instalar dependências e iniciar API

```powershell
cd .watermark
..\venv\Scripts\pip.exe install -r requirements.txt
..\venv\Scripts\uvicorn.exe app:app --port 8000
```

API disponível em `http://localhost:8000`

## Endpoints

- `GET /health` — Health check
- `POST /watermarks` — Criar watermark
- `GET /watermarks/{task_identifier}/{reference_column}` — Obter watermark
- `PUT /watermarks/{task_identifier}/{reference_column}` — Atualizar watermark
- `DELETE /watermarks/{task_identifier}/{reference_column}` — Remover watermark
- `GET /watermarks?skip=0&limit=100` — Listar watermarks

## Testes manuais (Incremental Append)

Roteiro completo: [`tests/passo_a_passo.txt`](tests/passo_a_passo.txt)

Scripts SQL em ordem:

| Arquivo | O que faz |
|---------|-----------|
| `tests/00_create_table.sql` | Cria `public.test_watermark` |
| `tests/01_insert_10.sql` | Insere 10 registros (1ª carga) |
| `tests/02_insert_5.sql` | Insere 5 novos |
| `tests/03_update_4.sql` | Atualiza 4 registros |
| `tests/04_delete_1.sql` | Remove 1 registro |
| `tests/99_cleanup.sql` | Remove a tabela de teste |

Tasks correspondentes:

```powershell
metro run tasks/incremental_append/test_watermark_append.yaml --secret-provider local --watermark-api-url http://localhost:8000
metro run tasks/incremental_append/test_watermark_append_partitioned.yaml --secret-provider local --watermark-api-url http://localhost:8000
```
