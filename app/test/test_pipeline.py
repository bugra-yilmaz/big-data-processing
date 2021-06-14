import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def spark_session():
    spark = SparkSession.builder.appName('adidas-bigdata-processing').master('local[*]').getOrCreate()
    return spark


def test_example(spark_session):
    assert True
