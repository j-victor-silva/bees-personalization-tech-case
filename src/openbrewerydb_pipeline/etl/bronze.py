from __future__ import annotations

import json
from datetime import datetime, timezone, date
from typing import Any, Dict, List

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

from .exceptions import TransformError, LoadError


def _bronze_schema() -> StructType:
    return StructType(
        [
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("ingestion_date", DateType(), False),
            StructField("source_system", StringType(), True),
            StructField("payload", StringType(), True),
        ]
    )


def to_bronze_dataframe(
    spark: SparkSession,
    records: List[Dict[str, Any]],
    *,
    source_system: str = "openbrewerydb",
) -> DataFrame:
    if records is None:
        raise TransformError("'records' não pode ser None.")
    if not isinstance(records, list):
        raise TransformError("'records' deve ser uma lista de dicionários.")

    utc_now = datetime.now(timezone.utc).replace(tzinfo=None)
    today = date.today()

    rows: List[Row] = []
    for record in records:
        try:
            payload = json.dumps(record, ensure_ascii=False)
        except (TypeError, ValueError) as exc:
            raise TransformError("Falha ao serializar registro para JSON na Bronze.") from exc
        rows.append(
            Row(
                ingestion_timestamp=utc_now,
                ingestion_date=today,
                source_system=source_system,
                payload=payload,
            )
        )

    try:
        return spark.createDataFrame(rows, schema=_bronze_schema())
    except Exception as exc:  # noqa: BLE001 - capturar erro do Spark DataFrame creation
        raise TransformError("Falha ao criar DataFrame Bronze.") from exc


def write_bronze(df: DataFrame, output_path: str) -> None:
    try:
        (
            df.write.mode("append")
            .format("parquet")
            .partitionBy("ingestion_date")
            .save(output_path)
        ) 
    except Exception as exc:  # noqa: BLE001 - capturar erro de I/O do Spark
        raise LoadError(f"Falha ao gravar Bronze em {output_path}.") from exc 