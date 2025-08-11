from __future__ import annotations

from typing import Any, Dict, List

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


def get_breweries_schema() -> StructType:
    return StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("brewery_type", StringType(), True),
            StructField("address_1", StringType(), True),
            StructField("address_2", StringType(), True),
            StructField("address_3", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state_province", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("country", StringType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("phone", StringType(), True),
            StructField("website_url", StringType(), True),
            StructField("state", StringType(), True),
            StructField("street", StringType(), True),
        ]
    )


def to_spark_dataframe(
    spark: SparkSession, records: List[Dict[str, Any]]
) -> DataFrame:
    schema = get_breweries_schema()
    field_names = [f.name for f in schema.fields]
    rows: List[Row] = []
    for record in records:
        # Garante ordem e chaves compatíveis com o schema
        ordered = {name: record.get(name) for name in field_names}
        rows.append(Row(**ordered))

    return spark.createDataFrame(rows, schema=schema)