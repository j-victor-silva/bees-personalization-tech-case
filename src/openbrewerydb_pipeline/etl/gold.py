from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, lit, desc

from .exceptions import TransformError, LoadError


def to_gold_aggregated(silver_df: DataFrame) -> DataFrame:
    if silver_df is None:
        raise TransformError("'silver_df' não pode ser None.")

    required_cols = {"brewery_type", "country", "state_province", "city"}
    missing = [c for c in required_cols if c not in silver_df.columns]
    if missing:
        raise TransformError(f"Colunas ausentes na Silver para agregação: {missing}")

    # Agrega quantidade de cervejarias por tipo e localização
    try:
        return (
            silver_df.groupBy(
                col("brewery_type"),
                col("country"),
                col("state_province"),
                col("city"),
            )
            .agg(count(lit(1)).alias("num_breweries"))
            .orderBy(desc("num_breweries"))
        )
    except Exception as exc:  # noqa: BLE001 - capturar erro de agregação
        raise TransformError("Falha ao agregar dados na Gold.") from exc


def write_gold(df: DataFrame, output_path: str) -> None:
    try:
        (
            df.write.mode("overwrite")
            .format("parquet")
            .save(output_path)
        ) 
    except Exception as exc:  # noqa: BLE001 - capturar erro de I/O do Spark
        raise LoadError(f"Falha ao gravar Gold em {output_path}.") from exc 