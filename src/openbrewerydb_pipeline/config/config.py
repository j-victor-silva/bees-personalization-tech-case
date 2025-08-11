from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv


@dataclass
class AppConfig:
    endpoint: str
    per_page: int = 50
    max_pages: Optional[int] = 5
    output_path: str = "data/output/breweries_parquet"
    page_param: str = "page"
    per_page_param: str = "per_page"
    request_timeout_seconds: int = 20
    bronze_output_path: str = "data/datalake/bronze/openbrewerydb"
    silver_output_path: str = "data/datalake/silver/openbrewerydb"
    gold_output_path: str = "data/datalake/gold/openbrewerydb_aggregated"

    @staticmethod
    def from_env_and_args() -> "AppConfig":
        import argparse

        load_dotenv(override=False)

        parser = argparse.ArgumentParser(description="PySpark API Extractor")
        parser.add_argument(
            "--endpoint",
            type=str,
            default=os.getenv("ENDPOINT"),
            required=os.getenv("ENDPOINT") is None,
            help="URL base da API (ex: https://api.openbrewerydb.org/v1/breweries)",
        )
        parser.add_argument(
            "--per-page",
            dest="per_page",
            type=int,
            default=int(os.getenv("PER_PAGE", "50")),
            help="Quantidade de registros por página (default: 50)",
        )
        parser.add_argument(
            "--max-pages",
            dest="max_pages",
            type=int,
            default=os.getenv("MAX_PAGES"),
            help="Número máximo de páginas a serem buscadas (default: 5; vazio para buscar até acabar)",
        )
        parser.add_argument(
            "--output-path",
            dest="output_path",
            type=str,
            default=os.getenv("OUTPUT_PATH", "data/output/breweries_parquet"),
            help="Caminho de saída (Parquet)",
        )
        parser.add_argument(
            "--bronze-output-path",
            dest="bronze_output_path",
            type=str,
            default=os.getenv("BRONZE_OUTPUT_PATH", "data/datalake/bronze/openbrewerydb"),
            help="Caminho de saída da camada Bronze",
        )
        parser.add_argument(
            "--silver-output-path",
            dest="silver_output_path",
            type=str,
            default=os.getenv("SILVER_OUTPUT_PATH", "data/datalake/silver/openbrewerydb"),
            help="Caminho de saída da camada Silver",
        )
        parser.add_argument(
            "--gold-output-path",
            dest="gold_output_path",
            type=str,
            default=os.getenv("GOLD_OUTPUT_PATH", "data/datalake/gold/openbrewerydb_aggregated"),
            help="Caminho de saída da camada Gold",
        )
        parser.add_argument(
            "--page-param",
            dest="page_param",
            type=str,
            default=os.getenv("PAGE_PARAM", "page"),
            help="Nome do parâmetro de página (default: page)",
        )
        parser.add_argument(
            "--per-page-param",
            dest="per_page_param",
            type=str,
            default=os.getenv("PER_PAGE_PARAM", "per_page"),
            help="Nome do parâmetro de tamanho de página (default: per_page)",
        )
        parser.add_argument(
            "--request-timeout-seconds",
            dest="request_timeout_seconds",
            type=int,
            default=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20")),
            help="Timeout de requisições HTTP em segundos (default: 20)",
        )

        args = parser.parse_args()

        max_pages_value: Optional[int]
        if args.max_pages is None:
            max_pages_value = 5
        else:
            max_pages_value = int(args.max_pages)

        return AppConfig(
            endpoint=args.endpoint,
            per_page=int(args.per_page),
            max_pages=max_pages_value,
            output_path=args.output_path,
            page_param=args.page_param,
            per_page_param=args.per_page_param,
            request_timeout_seconds=int(args.request_timeout_seconds),
            bronze_output_path=args.bronze_output_path,
            silver_output_path=args.silver_output_path,
            gold_output_path=args.gold_output_path,
        ) 