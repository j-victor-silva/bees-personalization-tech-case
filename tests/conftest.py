import os
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    os.environ.setdefault("SPARK_LOCAL_IP", "192.168.0.9")
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("pytest-openbrewerydb")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "192.168.0.9")
        .config("spark.driver.bindAddress", "192.168.0.9")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture()
def sample_api_records():
    return [
        {
            "id": "1",
            "name": "Brew A",
            "brewery_type": "micro",
            "address_1": "A",
            "address_2": None,
            "address_3": None,
            "city": "Austin",
            "state_province": "Texas",
            "postal_code": "00001",
            "country": "United States",
            "longitude": -97.0,
            "latitude": 30.0,
            "phone": "1111",
            "website_url": "http://a",
            "state": "Texas",
            "street": "A",
        },
        {
            "id": "2",
            "name": "Brew B",
            "brewery_type": "nano",
            "address_1": "B",
            "address_2": None,
            "address_3": None,
            "city": "Austin",
            "state_province": "Texas",
            "postal_code": "00002",
            "country": "United States",
            "longitude": -97.1,
            "latitude": 30.2,
            "phone": "2222",
            "website_url": "http://b",
            "state": "Texas",
            "street": "B",
        },
    ] 