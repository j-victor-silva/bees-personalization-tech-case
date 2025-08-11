import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from pyspark.sql import DataFrame

from src.openbrewerydb_pipeline.etl.bronze import to_bronze_dataframe
from src.openbrewerydb_pipeline.etl.silver import to_silver_dataframe


def test_to_silver_dataframe_parses_payload(spark, sample_api_records):
    bronze_df = to_bronze_dataframe(spark, sample_api_records, source_system="test")
    silver_df: DataFrame = to_silver_dataframe(bronze_df)

    assert silver_df.count() == len(sample_api_records)
    for col_name in [
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
    ]:
        assert col_name in silver_df.columns 