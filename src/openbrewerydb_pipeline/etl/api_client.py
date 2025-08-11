from __future__ import annotations

from typing import Any, Dict, List, Optional

import requests
from .exceptions import ExtractionError, ApiResponseError


class ApiClient:
    def __init__(
        self,
        endpoint: str,
        *,
        page_param: str = "page",
        per_page_param: str = "per_page",
        request_timeout_seconds: int = 20,
        default_params: Optional[Dict[str, Any]] = None,
        default_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.endpoint = endpoint.rstrip("?")
        self.page_param = page_param
        self.per_page_param = per_page_param
        self.request_timeout_seconds = request_timeout_seconds
        self.default_params = default_params or {}
        self.default_headers = default_headers or {}
        self._session = requests.Session()

    def fetch_page(self, *, page_number: int, per_page: int) -> List[Dict[str, Any]]:
        params = dict(self.default_params)
        params[self.page_param] = page_number
        params[self.per_page_param] = per_page

        try:
            response = self._session.get(
                self.endpoint,
                params=params,
                headers=self.default_headers,
                timeout=self.request_timeout_seconds,
            )
            response.raise_for_status()
        except requests.Timeout as exc:
            raise ExtractionError(
                f"Timeout ao chamar API em {self.endpoint} com params={params}"
            ) from exc
        except requests.RequestException as exc:
            raise ExtractionError(
                f"Falha na requisição HTTP para {self.endpoint}: {exc}"
            ) from exc

        try:
            data = response.json()
        except ValueError as exc:
            raise ApiResponseError(
                "A resposta da API não é um JSON válido."
            ) from exc

        if not isinstance(data, list):
            raise ApiResponseError(
                "A resposta da API não é uma lista JSON como esperado."
            )

        return data  # type: ignore[return-value]

    def fetch_all(self, *, per_page: int, max_pages: Optional[int] = None) -> List[Dict[str, Any]]:
        page_number = 1
        results: List[Dict[str, Any]] = []

        while True:
            if max_pages is not None and page_number > max_pages:
                break

            page_data = self.fetch_page(page_number=page_number, per_page=per_page)
            if len(page_data) == 0:
                break

            results.extend(page_data)
            page_number += 1

        return results 