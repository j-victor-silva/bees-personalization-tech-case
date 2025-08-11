from __future__ import annotations

from pyspark.sql import SparkSession

from .etl.api_client import ApiClient
from .config.config import AppConfig
from .etl.bronze import to_bronze_dataframe, write_bronze
from .etl.silver import to_silver_dataframe, write_silver
from .etl.gold import to_gold_aggregated, write_gold

import os
os.environ["SPARK_LOCAL_IP"] = "<IP_DO_SEU_COMPUTADOR>"


def build_spark(app_name: str = "ApiExtractor") -> SparkSession:
    builder: SparkSession.Builder = SparkSession.builder  # type: ignore[attr-defined]
    spark = (
        builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "<IP_DO_SEU_COMPUTADOR>")
        .config("spark.driver.bindAddress", "<IP_DO_SEU_COMPUTADOR>")
        .getOrCreate()
    )
    return spark


def main() -> None:
    config = AppConfig.from_env_and_args()

    spark = build_spark()

    client = ApiClient(
        config.endpoint,
        page_param=config.page_param,
        per_page_param=config.per_page_param,
        request_timeout_seconds=config.request_timeout_seconds,
    )

    records = client.fetch_all(per_page=config.per_page, max_pages=config.max_pages)

    if len(records) == 0:
        print("Nenhum registro retornado pela API.")
        spark.stop()
        return

    # Camada Bronze
    bronze_df = to_bronze_dataframe(spark, records, source_system="openbrewerydb")
    write_bronze(bronze_df, config.bronze_output_path)
    print(f"Bronze gravado em: {config.bronze_output_path}")

    # Camada Silver a partir da Bronze em disco
    bronze_on_disk_df = spark.read.parquet(config.bronze_output_path)
    silver_df = to_silver_dataframe(bronze_on_disk_df)
    write_silver(silver_df, config.silver_output_path)
    print(f"Silver gravado em: {config.silver_output_path}")

    # Camada Gold agregada a partir da Silver
    silver_on_disk_df = spark.read.parquet(config.silver_output_path)
    gold_df = to_gold_aggregated(silver_on_disk_df)
    write_gold(gold_df, config.gold_output_path)
    print(f"Gold gravado em: {config.gold_output_path}")

    spark.stop()


if __name__ == "__main__":
    main() 