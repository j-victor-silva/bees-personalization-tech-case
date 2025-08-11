from __future__ import annotations

import json
import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

# Garante que o Airflow encontre o pacote src/
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

# Imports do projeto
from src.openbrewerydb_pipeline.etl.api_client import ApiClient
from src.openbrewerydb_pipeline.etl.bronze import to_bronze_dataframe, write_bronze
from src.openbrewerydb_pipeline.etl.silver import to_silver_dataframe, write_silver
from src.openbrewerydb_pipeline.etl.gold import to_gold_aggregated, write_gold
from pyspark.sql import SparkSession

# Configurações básicas (pode mover para Variáveis do Airflow se preferir)
ENDPOINT = os.getenv("ENDPOINT", "https://api.openbrewerydb.org/v1/breweries")
PER_PAGE = int(os.getenv("PER_PAGE", "50"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "5"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", str(PROJECT_ROOT / "airflow"))
DATA_DIR = Path(os.getenv("DATA_DIR", str(Path(AIRFLOW_HOME) / "data")))
RAW_DIR = DATA_DIR / "raw" / "openbrewerydb"
BRONZE_PATH = os.getenv("BRONZE_OUTPUT_PATH") or str(DATA_DIR / "datalake" / "bronze" / "openbrewerydb")
SILVER_PATH = os.getenv("SILVER_OUTPUT_PATH") or str(DATA_DIR / "datalake" / "silver" / "openbrewerydb")
GOLD_PATH = os.getenv("GOLD_OUTPUT_PATH") or str(DATA_DIR / "datalake" / "gold" / "openbrewerydb_aggregated")


def build_spark_for_airflow() -> SparkSession:
    os.environ.setdefault("SPARK_LOCAL_IP", "<IP_DO_SEU_COMPUTADOR>")
    builder: SparkSession.Builder = SparkSession.builder  # type: ignore[attr-defined]
    return (
        builder.master("local[*]")
        .appName("BreweriesMedallionPipeline")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def task_test_api_connection(**_):
    logger = logging.getLogger("breweries_medallion.test_api")
    try:
        client = ApiClient(
            ENDPOINT,
            page_param=os.getenv("PAGE_PARAM", "page"),
            per_page_param=os.getenv("PER_PAGE_PARAM", "per_page"),
            request_timeout_seconds=REQUEST_TIMEOUT_SECONDS,
        )
        sample = client.fetch_page(page_number=1, per_page=1)
        if not isinstance(sample, list) or len(sample) == 0:
            raise AirflowException("API respondeu vazio no teste de conexão.")
        logger.info("Teste de conexão com API OK: 1 registro de amostra retornado.")
    except Exception as exc:
        logger.exception("Falha no teste de conexão com API: %s", exc)
        raise AirflowException(f"Teste de conexão com API falhou: {exc}")


def task_extract(**context: Dict[str, Any]) -> str:
    logger = logging.getLogger("breweries_medallion.extract")
    try:
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        (DATA_DIR / "datalake" / "bronze" / "openbrewerydb").mkdir(parents=True, exist_ok=True)
        (DATA_DIR / "datalake" / "silver" / "openbrewerydb").mkdir(parents=True, exist_ok=True)
        (DATA_DIR / "datalake" / "gold" / "openbrewerydb_aggregated").mkdir(parents=True, exist_ok=True)

        logger.info("Iniciando extração: endpoint=%s, per_page=%s, max_pages=%s", ENDPOINT, PER_PAGE, MAX_PAGES)

        client = ApiClient(
            ENDPOINT,
            page_param=os.getenv("PAGE_PARAM", "page"),
            per_page_param=os.getenv("PER_PAGE_PARAM", "per_page"),
            request_timeout_seconds=REQUEST_TIMEOUT_SECONDS,
        )
        records: List[Dict[str, Any]] = client.fetch_all(per_page=PER_PAGE, max_pages=MAX_PAGES)
        logger.info("Extração concluída: %d registros", len(records))

        ts_nodash = context["ts_nodash"]  # ex: 20250101T010203
        raw_file = RAW_DIR / f"breweries_{ts_nodash}.json"

        with raw_file.open("w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False)

        logger.info("RAW salvo em: %s", raw_file)
        return str(raw_file)
    except Exception as exc:
        logger.exception("Falha na task extract: %s", exc)
        raise AirflowException(f"Extract falhou: {exc}")


def task_test_bronze(ti, **_):
    logger = logging.getLogger("breweries_medallion.test_bronze")
    try:
        raw_path: str = ti.xcom_pull(task_ids="extract")
        if not raw_path:
            raise AirflowException("Caminho do RAW não encontrado no XCom")
        with open(raw_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, list) or len(data) == 0:
                raise AirflowException("RAW JSON inválido ou vazio para Bronze.")

        spark = build_spark_for_airflow()
        try:
            bronze_df = to_bronze_dataframe(spark, data, source_system="openbrewerydb")
            count = bronze_df.count()
            if count <= 0:
                raise AirflowException("Bronze DF vazio no teste.")
            expected_cols = {"ingestion_timestamp", "ingestion_date", "source_system", "payload"}
            if not expected_cols.issubset(set(bronze_df.columns)):
                raise AirflowException(f"Colunas esperadas ausentes na Bronze: {expected_cols - set(bronze_df.columns)}")
            logger.info("Teste Bronze OK: count=%d, colunas=%s", count, bronze_df.columns)
        finally:
            spark.stop()
    except Exception as exc:
        logger.exception("Falha no teste da Bronze: %s", exc)
        raise AirflowException(f"Teste Bronze falhou: {exc}")


def task_bronze(ti, **_):
    logger = logging.getLogger("breweries_medallion.bronze")
    try:
        raw_path: str = ti.xcom_pull(task_ids="extract")
        if not raw_path:
            raise RuntimeError("Caminho do RAW não encontrado no XCom")
        logger.info("Iniciando Bronze a partir de: %s", raw_path)

        with open(raw_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, list):
                raise ValueError("RAW JSON esperado como lista de objetos.")

        spark = build_spark_for_airflow()
        try:
            bronze_df = to_bronze_dataframe(spark, data, source_system="openbrewerydb")
            count = bronze_df.count()
            logger.info("Bronze DataFrame count: %d", count)
            write_bronze(bronze_df, BRONZE_PATH)
            logger.info("Bronze gravado em: %s", BRONZE_PATH)
        finally:
            spark.stop()
    except Exception as exc:
        logger.exception("Falha na task bronze: %s", exc)
        raise AirflowException(f"Bronze falhou: {exc}")


def task_test_silver(**_):
    logger = logging.getLogger("breweries_medallion.test_silver")
    try:
        spark = build_spark_for_airflow()
        try:
            bronze_df = spark.read.parquet(BRONZE_PATH)
            if bronze_df.count() <= 0:
                raise AirflowException("Bronze em disco está vazia para teste da Silver.")
            silver_df = to_silver_dataframe(bronze_df)
            count = silver_df.count()
            if count <= 0:
                raise AirflowException("Silver DF vazio no teste.")
            required_cols = {
                "id",
                "name",
                "brewery_type",
                "address_1",
                "city",
                "state_province",
                "country",
                "postal_code",
                "longitude",
                "latitude",
                "phone",
                "website_url",
                "state",
                "street",
            }
            if not required_cols.issubset(set(silver_df.columns)):
                raise AirflowException(f"Colunas esperadas ausentes na Silver: {required_cols - set(silver_df.columns)}")
            logger.info("Teste Silver OK: count=%d, colunas=%s", count, silver_df.columns)
        finally:
            spark.stop()
    except Exception as exc:
        logger.exception("Falha no teste da Silver: %s", exc)
        raise AirflowException(f"Teste Silver falhou: {exc}")


def task_silver(**_):
    logger = logging.getLogger("breweries_medallion.silver")
    try:
        spark = build_spark_for_airflow()
        try:
            bronze_df = spark.read.parquet(BRONZE_PATH)
            bronze_count = bronze_df.count()
            logger.info("Lido Bronze de %s, count=%d", BRONZE_PATH, bronze_count)

            silver_df = to_silver_dataframe(bronze_df)
            silver_count = silver_df.count()
            logger.info("Silver DataFrame count: %d", silver_count)

            write_silver(silver_df, SILVER_PATH)
            logger.info("Silver gravado em: %s", SILVER_PATH)
        finally:
            spark.stop()
    except Exception as exc:
        logger.exception("Falha na task silver: %s", exc)
        raise AirflowException(f"Silver falhou: {exc}")


def task_test_gold(**_):
    logger = logging.getLogger("breweries_medallion.test_gold")
    try:
        spark = build_spark_for_airflow()
        try:
            silver_df = spark.read.parquet(SILVER_PATH)
            if silver_df.count() <= 0:
                raise AirflowException("Silver em disco está vazia para teste da Gold.")
            gold_df = to_gold_aggregated(silver_df)
            count = gold_df.count()
            if count <= 0:
                raise AirflowException("Gold agregada vazia no teste.")
            expected_cols = {"brewery_type", "country", "state_province", "city", "num_breweries"}
            if not expected_cols.issubset(set(gold_df.columns)):
                raise AirflowException(f"Colunas esperadas ausentes na Gold: {expected_cols - set(gold_df.columns)}")
            logger.info("Teste Gold OK: count=%d, colunas=%s", count, gold_df.columns)
        finally:
            spark.stop()
    except Exception as exc:
        logger.exception("Falha no teste da Gold: %s", exc)
        raise AirflowException(f"Teste Gold falhou: {exc}")


def task_gold(**_):
    logger = logging.getLogger("breweries_medallion.gold")
    try:
        spark = build_spark_for_airflow()
        try:
            silver_df = spark.read.parquet(SILVER_PATH)
            silver_count = silver_df.count()
            logger.info("Lido Silver de %s, count=%d", SILVER_PATH, silver_count)

            gold_df = to_gold_aggregated(silver_df)
            gold_count = gold_df.count()
            logger.info("Gold agregada count: %d", gold_count)

            write_gold(gold_df, GOLD_PATH)
            logger.info("Gold gravado em: %s", GOLD_PATH)
        finally:
            spark.stop()
    except Exception as exc:
        logger.exception("Falha na task gold: %s", exc)
        raise AirflowException(f"Gold falhou: {exc}")


default_args = {
    "owner": "airflow",
    "retries": int(os.getenv("RETRIES", "3")),
    "retry_delay": timedelta(seconds=int(os.getenv("RETRY_DELAY_SECONDS", "60"))),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=int(os.getenv("MAX_RETRY_DELAY_MINUTES", "10"))),
}

with DAG(
    dag_id="breweries_medallion_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=os.getenv("DAG_CRON", "0 0 * * *"),
    is_paused_upon_creation=False,
    catchup=False,
    tags=["pyspark", "medallion", "openbrewerydb"],
) as dag:

    test_api = PythonOperator(
        task_id="test_api_connection",
        python_callable=task_test_api_connection,
    )

    extract = PythonOperator(
        task_id="extract",
        python_callable=task_extract,
        provide_context=True,
    )

    test_bronze = PythonOperator(
        task_id="test_bronze_layer",
        python_callable=task_test_bronze,
        provide_context=True,
    )

    bronze = PythonOperator(
        task_id="bronze",
        python_callable=task_bronze,
        provide_context=True,
    )

    test_silver = PythonOperator(
        task_id="test_silver_layer",
        python_callable=task_test_silver,
    )

    silver = PythonOperator(
        task_id="silver",
        python_callable=task_silver,
    )

    test_gold = PythonOperator(
        task_id="test_gold_layer",
        python_callable=task_test_gold,
    )

    gold = PythonOperator(
        task_id="gold",
        python_callable=task_gold,
    )

    # Fluxo: teste_de_conexao -> extração -> teste_bronze -> bronze -> teste_silver -> silver -> teste_gold -> gold
    test_api >> extract >> test_bronze >> bronze >> test_silver >> silver >> test_gold >> gold 