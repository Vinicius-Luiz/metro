"""Cliente HTTP para comunicação com a API de watermarks do METRO."""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import quote

import requests

from metro.settings import settings

logger = logging.getLogger(__name__)


class WatermarkAPIError(RuntimeError):
    """Erro de comunicação com a API de watermark."""


class WatermarkClient:
    """Cliente HTTP simples para consumir a API de watermarks.

    A API é a única abstração necessária — não há providers locais/remotos,
    apenas um cliente que consome HTTP.
    """

    def __init__(self, api_base_url: str) -> None:
        self._api_base_url = api_base_url.rstrip("/")
        logger.debug(
            "WatermarkClient configurado com api_base_url=%s",
            self._api_base_url,
        )

    def get_watermark(
        self,
        task_identifier: str,
        reference_column: str,
    ) -> dict[str, Any] | None:
        """Retorna o watermark atual ou None se não existir."""
        url = (
            f"{self._api_base_url}/watermarks/"
            f"{quote(task_identifier, safe='')}/"
            f"{quote(reference_column, safe='')}"
        )

        try:
            response = requests.get(url, timeout=settings.watermark_api_timeout)
            if response.status_code == 404:
                logger.debug(
                    "Watermark não encontrado (task_identifier=%s, "
                    "reference_column=%s)",
                    task_identifier,
                    reference_column,
                )
                return None
            response.raise_for_status()
            data = response.json()
            logger.info(
                "Watermark obtido (task_identifier=%s, reference_column=%s, "
                "watermark_value=%s)",
                task_identifier,
                reference_column,
                data["watermark_value"],
            )
            return data
        except requests.RequestException as exc:
            raise WatermarkAPIError(
                f"Falha ao obter watermark de {url}: {exc}"
            ) from exc

    def create_watermark(
        self,
        task_identifier: str,
        reference_column: str,
        watermark_value: Any,
        watermark_type: str,
        record_count: int = 0,
    ) -> None:
        """Cria um novo watermark (primeira execução)."""
        url = f"{self._api_base_url}/watermarks"
        payload = {
            "task_identifier": task_identifier,
            "reference_column": reference_column,
            "watermark_value": str(watermark_value),
            "watermark_type": watermark_type,
            "last_record_count": record_count,
        }

        try:
            response = requests.post(url, json=payload, timeout=settings.watermark_api_timeout)
            if response.status_code == 409:
                logger.warning(
                    "Watermark já existe (task_identifier=%s, "
                    "reference_column=%s), usando update ao invés de create",
                    task_identifier,
                    reference_column,
                )
                self.update_watermark(
                    task_identifier,
                    reference_column,
                    watermark_value,
                    watermark_type,
                    record_count,
                )
                return
            response.raise_for_status()
            logger.info(
                "Watermark criado (task_identifier=%s, reference_column=%s, "
                "watermark_value=%s)",
                task_identifier,
                reference_column,
                watermark_value,
            )
        except requests.RequestException as exc:
            raise WatermarkAPIError(
                f"Falha ao criar watermark em {url}: {exc}"
            ) from exc

    def update_watermark(
        self,
        task_identifier: str,
        reference_column: str,
        watermark_value: Any,
        watermark_type: str,
        record_count: int,
    ) -> None:
        """Atualiza o watermark após commit bem-sucedido."""
        url = (
            f"{self._api_base_url}/watermarks/"
            f"{quote(task_identifier, safe='')}/"
            f"{quote(reference_column, safe='')}"
        )
        payload = {
            "watermark_value": str(watermark_value),
            "last_record_count": record_count,
        }

        try:
            response = requests.put(url, json=payload, timeout=settings.watermark_api_timeout)
            response.raise_for_status()
            logger.info(
                "Watermark atualizado (task_identifier=%s, "
                "reference_column=%s, watermark_value=%s, record_count=%s)",
                task_identifier,
                reference_column,
                watermark_value,
                record_count,
            )
        except requests.RequestException as exc:
            raise WatermarkAPIError(
                f"Falha ao atualizar watermark em {url}: {exc}"
            ) from exc
