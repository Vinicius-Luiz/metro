"""Entrypoint da CLI do METRO."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from metro.core.task import Task
from metro.queries.local import LocalQueryRepository
from metro.replication.full_load.strategy import FullLoadStrategy
from metro.replication.incremental.append.max_value import AppendMaxValueStrategy
from metro.replication.incremental.replace.partition import ReplacePartitionStrategy
from metro.secrets.base import SecretProvider
from metro.secrets.local import LocalSecretProvider
from metro.sources.base import SourceEndpoint
from metro.sources.sql.postgresql import PostgreSQLSource
from metro.targets.base import TargetEndpoint
from metro.targets.local import LocalTarget
from metro.watermark.client import WatermarkClient

logger = logging.getLogger(__name__)

DEFAULT_LOG_DIR = Path("logs")
LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def main(argv: list[str] | None = None) -> int:
    """Ponto de entrada da CLI; retorna código de saída do processo."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command != "run":
        parser.print_help()
        return 1

    task_path = Path(args.task)
    log_file = _configure_logging(
        level=args.log_level,
        task_path=task_path,
        log_file=Path(args.log_file) if args.log_file else None,
    )
    logger.info("Arquivo de log: %s", log_file)

    try:
        run_task(
            task_path=task_path,
            secret_provider_name=args.secret_provider,
            watermark_api_url=args.watermark_api_url,
        )
    except Exception:
        logger.exception("Falha ao executar a task")
        return 1
    return 0


def run_task(
    task_path: Path,
    secret_provider_name: str,
    watermark_api_url: str | None = None,
) -> None:
    """Executa exatamente uma task (uma tabela) por invocação."""
    logger.info("Carregando task: %s", task_path)
    task = Task.from_yaml(task_path)
    _log_task_parameters(task, secret_provider_name)

    secret_provider = _build_secret_provider(secret_provider_name)
    query_repository = LocalQueryRepository()
    logger.debug("QueryRepository base_dir=%s", query_repository.base_dir)

    watermark_client = None
    needs_watermark = (
        task.replication.mode == "incremental"
        and task.replication.strategy is not None
        and task.replication.strategy.type == "append"
    )
    if needs_watermark:
        api_url = watermark_api_url or "http://localhost:8000"
        watermark_client = WatermarkClient(api_base_url=api_url)
        logger.debug("WatermarkClient configurado com api_base_url=%s", api_url)

    source = _build_source(task, secret_provider, query_repository)
    target = _build_target(task, secret_provider)
    strategy = _build_strategy(task, watermark_client)

    logger.info(
        "Iniciando replicação (table=%s, mode=%s, source=%s, target=%s)",
        task.table.qualified_name,
        task.replication.mode,
        task.source.type,
        task.target.type,
    )

    with source, target:
        strategy.execute(source, target, task.table)

    logger.info("Replicação concluída (table=%s)", task.table.qualified_name)


def _log_task_parameters(task: Task, secret_provider_name: str) -> None:
    """Registra parâmetros efetivos da task em nível debug."""
    strategy = task.replication.strategy
    logger.debug(
        "Parâmetros METRO | secret_provider=%s | table.schema_name=%s | "
        "table.name=%s | table.qualified_name=%s | table.target_schema_name=%s | "
        "table.target_name=%s | table.target_dataset_path=%s | "
        "table.columns_declared=%s",
        secret_provider_name,
        task.table.schema_name,
        task.table.name,
        task.table.qualified_name,
        task.table.target_schema_name,
        task.table.target_name,
        task.table.target_dataset_path,
        len(task.table.columns),
    )
    if task.table.columns:
        declared = [
            f"{col.name}:{col.data_type}:nullable={col.nullable}"
            for col in task.table.columns
        ]
        logger.debug("Colunas declaradas no YAML: %s", declared)

    logger.debug(
        "Parâmetros Source | type=%s | runtime=%s | query_path=%s | chunk_size=%s",
        task.source.type,
        task.source.runtime,
        task.source.query_path,
        task.source.chunk_size,
    )
    logger.debug(
        "Parâmetros Target | type=%s | runtime=%s | chunk_size=%s",
        task.target.type,
        task.target.runtime,
        task.target.chunk_size,
    )
    logger.debug(
        "Parâmetros Replication | mode=%s | strategy=%s | partition=%s",
        task.replication.mode,
        None
        if strategy is None
        else {
            "type": strategy.type,
            "reference_column": strategy.reference_column,
            "aggregation": strategy.aggregation,
            "lookback_periods": strategy.lookback_periods,
            "partition": None
            if strategy.partition is None
            else strategy.partition.model_dump(),
        },
        None
        if task.replication.partition is None
        else task.replication.partition.model_dump(),
    )


def _build_parser() -> argparse.ArgumentParser:
    """Monta o ArgumentParser da CLI (`metro run …`)."""
    parser = argparse.ArgumentParser(
        prog="metro",
        description="METRO — Motor de Extração, Transferência e Replicação de Objetos",
    )
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser(
        "run",
        help="Executa uma task de replicação (uma tabela por invocação)",
    )
    run_parser.add_argument(
        "task",
        help="Caminho do YAML da task (exatamente um arquivo)",
    )
    run_parser.add_argument(
        "--secret-provider",
        default="local",
        choices=["local"],
        help="Provider de secrets (padrão: local)",
    )
    run_parser.add_argument(
        "--watermark-api-url",
        default="http://localhost:8000",
        help="URL base da API de watermark (padrão: http://localhost:8000)",
    )
    run_parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Nível de logging (padrão: INFO)",
    )
    run_parser.add_argument(
        "--log-file",
        default=None,
        help=(
            "Caminho do arquivo de log. "
            "Padrão: logs/<task>_<timestamp>.log"
        ),
    )
    return parser


def _configure_logging(
    level: str,
    task_path: Path,
    log_file: Path | None = None,
) -> Path:
    """Configura logging para console e arquivo simultaneamente."""
    log_level = getattr(logging, level)
    destination = log_file or _default_log_path(task_path)
    destination.parent.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(log_level)

    formatter = logging.Formatter(LOG_FORMAT)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    file_handler = logging.FileHandler(destination, encoding="utf-8")
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    return destination.resolve()


def _default_log_path(task_path: Path) -> Path:
    """Gera o path padrão `logs/<task>_<timestamp>.log`."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return DEFAULT_LOG_DIR / f"{task_path.stem}_{timestamp}.log"


def _build_secret_provider(name: str) -> SecretProvider:
    """Instancia o Secret Provider pedido pela CLI."""
    if name == "local":
        return LocalSecretProvider()
    raise ValueError(f"Secret provider não suportado: {name}")


def _build_source(
    task: Task,
    secret_provider: SecretProvider,
    query_repository: LocalQueryRepository,
) -> SourceEndpoint:
    """Instancia o Source Endpoint conforme `source.type` da task."""
    source_type = task.source.type
    if source_type == "postgresql":
        return PostgreSQLSource(
            runtime=task.source.runtime,
            secret_provider=secret_provider,
            query_repository=query_repository,
            query_path=task.source.query_path,
            chunk_size=task.source.chunk_size,
            table=task.table,
        )
    raise ValueError(f"Source type não suportado: {source_type}")


def _build_target(task: Task, secret_provider: SecretProvider) -> TargetEndpoint:
    """Instancia o Target Endpoint conforme `target.type` da task."""
    target_type = task.target.type
    if target_type == "local":
        return LocalTarget(
            runtime=task.target.runtime,
            secret_provider=secret_provider,
            chunk_size=task.target.chunk_size,
        )
    raise ValueError(f"Target type não suportado: {target_type}")


def _build_strategy(
    task: Task,
    watermark_client: WatermarkClient | None = None,
):
    """Instancia a Replication Strategy conforme `replication` da task."""
    if task.replication.mode == "full_load":
        partition = task.replication.partition
        if partition is not None:
            if partition.reference_column is None:
                raise ValueError(
                    "replication.partition.reference_column é obrigatório "
                    "no full_load particionado"
                )
            return FullLoadStrategy(
                reference_column=partition.reference_column,
                granularity=partition.type,
            )
        return FullLoadStrategy()
    if task.replication.mode == "incremental":
        strategy = task.replication.strategy
        if strategy is None:
            raise ValueError("replication.strategy é obrigatório para mode=incremental")
        if strategy.type == "append":
            if watermark_client is None:
                raise ValueError(
                    "watermark_client é obrigatório para Append/MaxValue"
                )
            partition_type = None
            partition_column = None
            if strategy.partition is not None:
                partition_type = strategy.partition.type
                partition_column = strategy.partition.reference_column
            return AppendMaxValueStrategy(
                reference_column=strategy.reference_column,
                watermark_client=watermark_client,
                aggregation=strategy.aggregation or "MAX",
                partition_type=partition_type,
                partition_column=partition_column,
            )
        if strategy.type == "replace":
            if strategy.partition is None or strategy.lookback_periods is None:
                raise ValueError(
                    "replace/partition exige strategy.partition e "
                    "strategy.lookback_periods"
                )
            return ReplacePartitionStrategy(
                reference_column=strategy.reference_column,
                granularity=strategy.partition.type,
                lookback_periods=strategy.lookback_periods,
            )
        raise ValueError(
            f"Replication strategy type não suportado ainda: {strategy.type}"
        )
    raise ValueError(
        f"Replication mode não suportado ainda: {task.replication.mode}"
    )


if __name__ == "__main__":
    sys.exit(main())
