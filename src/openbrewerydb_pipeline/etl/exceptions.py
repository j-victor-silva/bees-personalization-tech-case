from __future__ import annotations

class EtlError(Exception):
    """Erro genérico da pipeline ETL."""


class ExtractionError(EtlError):
    """Falha ao extrair dados da fonte (API, arquivos, etc.)."""


class ApiResponseError(ExtractionError):
    """A resposta da API está fora do formato esperado ou inválida."""


class TransformError(EtlError):
    """Falha na transformação/parsing/validação de dados."""


class LoadError(EtlError):
    """Falha ao gravar dados em destino (Datalake, DW, etc.).""" 