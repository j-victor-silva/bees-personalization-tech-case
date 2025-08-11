import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

import pytest
from pyspark.sql import DataFrame

from src.openbrewerydb_pipeline.etl.bronze import to_bronze_dataframe, write_bronze


def test_to_bronze_dataframe_creates_df(spark, sample_api_records):
    df: DataFrame = to_bronze_dataframe(spark, sample_api_records, source_system="test")
    assert df.count() == len(sample_api_records)
    assert set(df.columns) == {"ingestion_timestamp", "ingestion_date", "source_system", "payload"}


def test_write_bronze(tmp_path: Path, spark, sample_api_records):
    out = tmp_path / "bronze"
    df = to_bronze_dataframe(spark, sample_api_records, source_system="test")

    write_bronze(df, str(out))

    read_back = spark.read.parquet(str(out))
    assert read_back.count() == len(sample_api_records) 