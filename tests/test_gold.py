import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from pyspark.sql import DataFrame

from src.openbrewerydb_pipeline.etl.bronze import to_bronze_dataframe
from src.openbrewerydb_pipeline.etl.silver import to_silver_dataframe
from src.openbrewerydb_pipeline.etl.gold import to_gold_aggregated


def test_to_gold_aggregated_counts(spark, sample_api_records):
    bronze_df = to_bronze_dataframe(spark, sample_api_records, source_system="test")
    silver_df: DataFrame = to_silver_dataframe(bronze_df)
    gold_df: DataFrame = to_gold_aggregated(silver_df)

    # Deve existir pelo menos 1 linha agregada
    assert gold_df.count() >= 1
    assert set(["brewery_type", "country", "state_province", "city", "num_breweries"]).issubset(set(gold_df.columns)) 