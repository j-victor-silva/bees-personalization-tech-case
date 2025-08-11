from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json

from ..utils.transform import get_breweries_schema
from .exceptions import TransformError, LoadError


def to_silver_dataframe(bronze_df: DataFrame) -> DataFrame:
    if bronze_df is None:
        raise TransformError("'bronze_df' não pode ser None.")

    schema = get_breweries_schema()
    try:
        parsed = bronze_df.select(
            col("ingestion_timestamp"),
            from_json(col("payload"), schema).alias("data"),
        )
    except Exception as exc:  # noqa: BLE001 - capturar erro de parsing
        raise TransformError("Falha ao converter payload JSON na Silver.") from exc

    return parsed.select(
        col("ingestion_timestamp"),
        col("data.id").alias("id"),
        col("data.name").alias("name"),
        col("data.brewery_type").alias("brewery_type"),
        col("data.address_1").alias("address_1"),
        col("data.address_2").alias("address_2"),
        col("data.address_3").alias("address_3"),
        col("data.street").alias("street"),
        col("data.city").alias("city"),
        col("data.state").alias("state"),
        col("data.state_province").alias("state_province"),
        col("data.country").alias("country"),
        col("data.postal_code").alias("postal_code"),
        col("data.longitude").alias("longitude"),
        col("data.latitude").alias("latitude"),
        col("data.phone").alias("phone"),
        col("data.website_url").alias("website_url"),
    )


def write_silver(df: DataFrame, output_path: str) -> None:
    try:
        (
            df.write.mode("append")
            .format("parquet")
            .partitionBy("country", "state_province")
            .save(output_path)
        ) 
    except Exception as exc:  # noqa: BLE001 - capturar erro de I/O do Spark
        raise LoadError(f"Falha ao gravar Silver em {output_path}.") from exc 