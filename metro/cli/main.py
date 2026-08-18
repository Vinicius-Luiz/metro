"""Entrypoint da CLI do METRO."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from metro.core.metadata import MetadataContext
from metro.core.task import Task, TaskValidationError
from metro.queries.local import LocalQueryRepository
from metro.replication.full_load.strategy import FullLoadStrategy
from metro.replication.incremental.append.max_value import AppendMaxValueStrategy
from metro.replication.incremental.replace.partition import ReplacePartitionStrategy
from metro.secrets.base import SecretProvider
from metro.secrets.local import LocalSecretProvider
from metro.settings import settings
from metro.sources.base import SourceEndpoint
from metro.sources.sql.postgresql import PostgreSQLSource
from metro.targets.base import TargetEndpoint
from metro.targets.local import LocalTarget
from metro.watermark.client import WatermarkClient

logger = logging.getLogger(__name__)

LOG_SUBDIRS = {"full_load", "incremental_replace", "incremental_append"}
LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"

REQUIRED_CLI_FIELDS = {
    "table_name": "--table.name",
    "table_target_schema": "--table.target-schema",
    "table_target_name": "--table.target-name",
    "source_type": "--source.type",
    "source_runtime": "--source.runtime",
    "target_type": "--target.type",
    "target_runtime": "--target.runtime",
    "replication_mode": "--replication.mode",
}


def main(argv: list[str] | None = None) -> int:
    """Ponto de entrada da CLI; retorna código de saída do processo."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command != "run":
        parser.print_help()
        return 1

    task_path = Path(args.task) if args.task else None
    log_file = _configure_logging(
        task_path=task_path,
        table_name=getattr(args, "table_name", None),
    )
    logger.info("Arquivo de log: %s", log_file)

    try:
        if task_path is not None:
            logger.info("Carregando task do YAML: %s", task_path)
            task = Task.from_yaml(task_path)
        else:
            logger.info("Construindo task via argumentos CLI")
            task = _build_task_from_cli(args)
        run_task(task=task)
    except Exception:
        logger.exception("Falha ao executar a task")
        return 1
    return 0


def run_task(task: Task) -> None:
    """Executa exatamente uma task (uma tabela) por invocação."""
    execution_timestamp = datetime.now()
    _log_task_parameters(task)

    metadata_context = _build_metadata_context(task, execution_timestamp)
    if metadata_context is not None:
        logger.info(
            "Metadados habilitados (source=%s, timestamp=%s)",
            task.table.qualified_name,
            execution_timestamp.replace(microsecond=0).isoformat(timespec="seconds"),
        )

    secret_provider = _build_secret_provider()
    query_repository = LocalQueryRepository()
    logger.debug("QueryRepository base_dir=%s", query_repository.base_dir)

    watermark_client = None
    needs_watermark = (
        task.replication.mode == "incremental"
        and task.replication.strategy is not None
        and task.replication.strategy.type == "append"
    )
    if needs_watermark:
        watermark_client = WatermarkClient(
            api_base_url=settings.watermark_api_url
        )
        logger.debug(
            "WatermarkClient configurado com api_base_url=%s",
            settings.watermark_api_url,
        )

    source = _build_source(task, secret_provider, query_repository)
    target = _build_target(task, secret_provider)
    strategy = _build_strategy(task, watermark_client, metadata_context)

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


def _log_task_parameters(task: Task) -> None:
    """Registra parâmetros efetivos da task em nível debug."""
    strategy = task.replication.strategy
    logger.debug(
        "Parâmetros METRO | secret_provider=%s | table.schema_name=%s | "
        "table.name=%s | table.qualified_name=%s | table.target_schema_name=%s | "
        "table.target_name=%s | table.target_dataset_path=%s",
        settings.secret_provider,
        task.table.schema_name,
        task.table.name,
        task.table.qualified_name,
        task.table.target_schema_name,
        task.table.target_name,
        task.table.target_dataset_path,
    )

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
    logger.debug(
        "Parâmetros Metadata | enabled=%s | config=%s",
        task.metadata.enabled,
        task.metadata.model_dump(),
    )


def _cli_dest(flag: str) -> str:
    """Converte `--a.b.foo-bar` em dest argparse `a_b_foo_bar`."""
    return flag.lstrip("-").replace("-", "_").replace(".", "_")


def _parse_bool(value: str) -> bool:
    """Interpreta valores booleanos passados via CLI."""
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    raise argparse.ArgumentTypeError(
        f"valor booleano inválido: '{value}' (use true/false)"
    )


def _add_task_flag(
    parser: argparse.ArgumentParser,
    flag: str,
    **kwargs,
) -> None:
    """Registra uma flag hierárquica com dest sem pontos."""
    parser.add_argument(flag, dest=_cli_dest(flag), **kwargs)


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
        nargs="?",
        help="Caminho do YAML da task (opcional se as flags CLI forem fornecidas)",
    )

    table_group = run_parser.add_argument_group("table")
    _add_task_flag(table_group, "--table.schema", help="Schema da tabela no source")
    _add_task_flag(table_group, "--table.name", help="Nome da tabela no source")
    _add_task_flag(table_group, "--table.target-schema", help="Schema no target")
    _add_task_flag(table_group, "--table.target-name", help="Nome da tabela no target")

    source_group = run_parser.add_argument_group("source")
    _add_task_flag(
        source_group,
        "--source.type",
        help="Tipo do source (postgresql, mongodb, etc)",
    )
    _add_task_flag(source_group, "--source.runtime", help="Runtime do source")
    _add_task_flag(
        source_group,
        "--source.query-path",
        help="Caminho da query SQL (opcional)",
    )
    _add_task_flag(
        source_group,
        "--source.chunk-size",
        type=int,
        help="Chunk size do source (opcional)",
    )

    target_group = run_parser.add_argument_group("target")
    _add_task_flag(target_group, "--target.type", help="Tipo do target (local, s3)")
    _add_task_flag(target_group, "--target.runtime", help="Runtime do target")
    _add_task_flag(
        target_group,
        "--target.chunk-size",
        type=int,
        help="Chunk size do target (opcional)",
    )

    replication_group = run_parser.add_argument_group("replication")
    _add_task_flag(
        replication_group,
        "--replication.mode",
        choices=["full_load", "incremental"],
        help="Modo de replicação",
    )
    _add_task_flag(
        replication_group,
        "--replication.partition.type",
        choices=["year", "month", "day"],
        help="Tipo de partição para full_load",
    )
    _add_task_flag(
        replication_group,
        "--replication.partition.reference-column",
        help="Coluna de referência para partição",
    )
    _add_task_flag(
        replication_group,
        "--replication.strategy.type",
        choices=["replace", "append"],
        help="Tipo de estratégia incremental",
    )
    _add_task_flag(
        replication_group,
        "--replication.strategy.reference-column",
        help="Coluna de referência da estratégia incremental",
    )
    _add_task_flag(
        replication_group,
        "--replication.strategy.lookback-periods",
        type=int,
        help="Número de períodos de lookback (apenas replace)",
    )
    _add_task_flag(
        replication_group,
        "--replication.strategy.aggregation",
        help="Função de agregação (apenas append, default: MAX)",
    )
    _add_task_flag(
        replication_group,
        "--replication.strategy.partition.type",
        choices=["year", "month", "day"],
        help="Tipo de partição da estratégia",
    )
    _add_task_flag(
        replication_group,
        "--replication.strategy.partition.reference-column",
        help="Coluna de referência da partição da estratégia",
    )

    metadata_group = run_parser.add_argument_group("metadata")
    _add_task_flag(
        metadata_group,
        "--metadata.enabled",
        type=_parse_bool,
        help="Habilitar metadata (default: True)",
    )
    _add_task_flag(
        metadata_group,
        "--metadata.columns.data-ingestao.enabled",
        type=_parse_bool,
        help="Habilitar coluna data_ingestao",
    )
    _add_task_flag(
        metadata_group,
        "--metadata.columns.data-ingestao.column-name",
        help="Nome customizado para coluna data_ingestao",
    )
    _add_task_flag(
        metadata_group,
        "--metadata.columns.banco-origem.enabled",
        type=_parse_bool,
        help="Habilitar coluna banco_origem",
    )
    _add_task_flag(
        metadata_group,
        "--metadata.columns.banco-origem.column-name",
        help="Nome customizado para coluna banco_origem",
    )
    return parser


def _omit_none(payload: dict) -> dict:
    """Remove chaves com valor None do dicionário."""
    return {key: value for key, value in payload.items() if value is not None}


def _build_nested_dict_from_args(args: argparse.Namespace) -> dict:
    """Constrói dicionário aninhado a partir de flags hierárquicas."""
    task_dict: dict = {
        "table": _omit_none(
            {
                "schema_name": args.table_schema,
                "name": args.table_name,
                "target_schema_name": args.table_target_schema,
                "target_name": args.table_target_name,
            }
        ),
        "source": _omit_none(
            {
                "type": args.source_type,
                "runtime": args.source_runtime,
                "query_path": args.source_query_path,
                "chunk_size": args.source_chunk_size,
            }
        ),
        "target": _omit_none(
            {
                "type": args.target_type,
                "runtime": args.target_runtime,
                "chunk_size": args.target_chunk_size,
            }
        ),
    }

    replication: dict = {"mode": args.replication_mode}
    if (
        args.replication_partition_type
        or args.replication_partition_reference_column
    ):
        replication["partition"] = _omit_none(
            {
                "type": args.replication_partition_type,
                "reference_column": args.replication_partition_reference_column,
            }
        )

    if args.replication_strategy_type:
        strategy: dict = {
            "type": args.replication_strategy_type,
            "reference_column": args.replication_strategy_reference_column,
        }
        if args.replication_strategy_lookback_periods is not None:
            strategy["lookback_periods"] = args.replication_strategy_lookback_periods
        if args.replication_strategy_aggregation:
            strategy["aggregation"] = args.replication_strategy_aggregation
        if (
            args.replication_strategy_partition_type
            or args.replication_strategy_partition_reference_column
        ):
            strategy["partition"] = _omit_none(
                {
                    "type": args.replication_strategy_partition_type,
                    "reference_column": (
                        args.replication_strategy_partition_reference_column
                    ),
                }
            )
        replication["strategy"] = strategy

    task_dict["replication"] = replication

    metadata_dict: dict = {}
    if args.metadata_enabled is not None:
        metadata_dict["enabled"] = args.metadata_enabled

    columns_dict: dict = {}
    data_ingestao = _omit_none(
        {
            "enabled": args.metadata_columns_data_ingestao_enabled,
            "column_name": args.metadata_columns_data_ingestao_column_name,
        }
    )
    if data_ingestao:
        columns_dict["data_ingestao"] = data_ingestao

    banco_origem = _omit_none(
        {
            "enabled": args.metadata_columns_banco_origem_enabled,
            "column_name": args.metadata_columns_banco_origem_column_name,
        }
    )
    if banco_origem:
        columns_dict["banco_origem"] = banco_origem

    if columns_dict:
        metadata_dict["columns"] = columns_dict
    if metadata_dict:
        task_dict["metadata"] = metadata_dict

    return task_dict


def _build_task_from_cli(args: argparse.Namespace) -> Task:
    """Constrói uma Task a partir de argumentos CLI com flags hierárquicas."""
    missing = [
        flag
        for field, flag in REQUIRED_CLI_FIELDS.items()
        if not getattr(args, field, None)
    ]
    if missing:
        raise TaskValidationError(
            "Campos obrigatórios faltando: " + ", ".join(missing)
        )

    task_dict = _build_nested_dict_from_args(args)
    try:
        return Task.model_validate(task_dict)
    except TaskValidationError:
        raise
    except Exception as exc:
        raise TaskValidationError(f"Contrato inválido via CLI: {exc}") from exc


def _configure_logging(
    task_path: Path | None = None,
    table_name: str | None = None,
) -> Path:
    """Configura logging para console e arquivo simultaneamente."""
    log_level = getattr(logging, settings.log_level)
    if settings.log_file:
        destination = settings.log_file
    elif task_path:
        destination = _default_log_path(task_path)
    elif table_name:
        destination = _default_log_path_from_table(table_name)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        destination = settings.log_dir / f"metro_{timestamp}.log"
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
    """Gera o path padrão `logs/<modo>/<task>_<timestamp>.log`."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    subdir = task_path.parent.name
    base = (
        settings.log_dir / subdir
        if subdir in LOG_SUBDIRS
        else settings.log_dir
    )
    return base / f"{task_path.stem}_{timestamp}.log"


def _default_log_path_from_table(table_name: str) -> Path:
    """Gera path de log para task via CLI (sem YAML)."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return settings.log_dir / f"{table_name}_{timestamp}.log"


def _build_secret_provider() -> SecretProvider:
    """Instancia o Secret Provider configurado em settings."""
    if settings.secret_provider == "local":
        return LocalSecretProvider()
    raise ValueError(
        f"Secret provider não suportado: {settings.secret_provider}"
    )


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


def _build_metadata_context(
    task: Task,
    execution_timestamp: datetime,
) -> MetadataContext | None:
    """Monta o contexto de metadados quando habilitado na task."""
    if not task.metadata.is_active():
        return None
    return MetadataContext(
        config=task.metadata,
        source_table_qualified_name=task.table.qualified_name,
        execution_timestamp=execution_timestamp,
    )


def _build_strategy(
    task: Task,
    watermark_client: WatermarkClient | None = None,
    metadata_context: MetadataContext | None = None,
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
                metadata_context=metadata_context,
            )
        return FullLoadStrategy(metadata_context=metadata_context)
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
                metadata_context=metadata_context,
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
                metadata_context=metadata_context,
            )
        raise ValueError(
            f"Replication strategy type não suportado ainda: {strategy.type}"
        )
    raise ValueError(
        f"Replication mode não suportado ainda: {task.replication.mode}"
    )


if __name__ == "__main__":
    sys.exit(main())
