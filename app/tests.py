# Created by bugra-yilmaz on 13.06.2021.
#
# Unit testing module for data transformation operations used in the Spark processing job.

# Imports
from typing import Tuple, List, Any

import pytest
import pyspark.sql
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as F

from main import (
    parse_argument,
    get_spark_session,
    calculate_rf_metrics,
)

AnyTuple = Tuple[Any, Any]
StringList = List[str]
IntList = List[int]


# Provide Spark session fixture for unit tests
@pytest.fixture
def spark_session() -> pyspark.sql.SparkSession:
    spark = (
        SparkSession.builder.appName("adidas-bigdata-processing")
        .master("local[*]")
        .getOrCreate()
    )
    return spark


# Provide sample data fixture for unit tests
@pytest.fixture
def sample_data(spark_session: pyspark.sql.SparkSession) -> AnyTuple:
    fact = [
        (710, "2016-08-18 09:42", 102),
        (710, "2016-02-29 14:24", 155),
        (619, "2016-03-01 15:27", 102),
        (619, "2016-03-25 00:51", 188),
        (180, "2016-04-28 00:24", 196),
        (623, "2016-01-31 21:45", 126),
        (710, "2016-04-27 22:24", 196),
        (87, "2018-11-26 22:04", 112),
        (479, "2016-04-27 22:24", 196),
        (320, "2017-01-17 01:20", 149),
        (540, "2016-04-28 00:24", 196),
        (644, "2016-02-20 13:12", 190),
        (970, "2016-04-28 00:24", 196),
        (970, "2016-06-30 14:02", 102),
        (701, "2016-01-17 11:40", 200),
        (9, "2018-01-12 13:48", 111),
        (507, "2016-07-21 16:51", 157),
        (701, "2016-02-13 07:32", 102),
        (651, "2016-04-28 00:24", 196),
        (160, "2016-04-28 00:24", 196),
        (710, "2016-04-27 22:24", 196),
        (189, "2016-02-28 15:58", 102),
        (658, "2016-08-18 20:53", 166),
        (420, "2016-04-27 22:24", 196),
        (710, "2016-04-27 22:24", 196),
        (47, "2018-11-26 22:04", 112),
        (285, "2016-02-03 08:49", 187),
    ]

    lookup = [
        (102, "news"),
        (155, "news"),
        (188, "movies"),
        (196, "news"),
        (126, "movies"),
        (112, "news"),
        (149, "news"),
        (190, "news"),
        (200, "news"),
        (111, "news"),
        (157, "news"),
        (166, "news"),
        (187, "movies"),
    ]
    fact_schema = StructType(
        [
            StructField("USER_ID", IntegerType(), False),
            StructField("EVENT_DATE", StringType(), False),
            StructField("WEB_PAGEID", IntegerType(), False),
        ]
    )
    lookup_schema = StructType(
        [
            StructField("WEB_PAGEID", IntegerType(), False),
            StructField("WEBPAGE_TYPE", StringType(), False),
        ]
    )
    fact_df = spark_session.createDataFrame(fact, schema=fact_schema)
    fact_df = fact_df.withColumn("EVENT_DATE", F.to_timestamp("EVENT_DATE"))
    lookup_df = spark_session.createDataFrame(lookup, schema=lookup_schema)

    return fact_df, lookup_df


@pytest.mark.parametrize(
    "argument, parsed",
    [
        ("news, movies", ["news", "movies"]),
        ("fre,dur  ", ["fre", "dur"]),
        ("news,", ["news"]),
        ("365,730, 1460,2920", ["365", "730", "1460", "2920"]),
    ],
)
def test_parse_values(argument, parsed):
    assert parsed == parse_argument(argument)


def test_get_spark_session():
    spark_session = get_spark_session()
    assert type(spark_session) == SparkSession

    data = [
        (710, "23/01/2017 15:43", 157),
        (446, "12/04/2016 09:09", 138),
        (538, "28/04/2016 00:24", 196),
        (368, "27/04/2016 22:24", 196),
        (9, "12/01/2018 13:48", 111),
    ]
    columns = ["USER_ID", "EVENT_DATE", "WEB_PAGEID"]

    df = spark_session.createDataFrame(data, schema=columns)
    assert type(df) == DataFrame


@pytest.mark.parametrize(
    "metric_types, page_types, reference_date, time_windows, count, names",
    [
        (
            ["fre"],
            ["news"],
            "2017-10-12",
            [365, 730],
            15,
            ["USER_ID", "pageview_news_fre_365", "pageview_news_fre_730"],
        ),
        (
            ["fre", "dur"],
            ["news"],
            "2017-10-12",
            [365, 730],
            15,
            [
                "USER_ID",
                "pageview_news_dur",
                "pageview_news_fre_365",
                "pageview_news_fre_730",
            ],
        ),
        (
            ["dur"],
            ["news"],
            "2019-01-01",
            [365, 730],
            18,
            ["USER_ID", "pageview_news_dur"],
        ),
        (
            ["fre"],
            ["news"],
            "2017-10-12",
            [365, 730],
            15,
            ["USER_ID", "pageview_news_fre_365", "pageview_news_fre_730"],
        ),
        (
            ["fre", "dur"],
            ["news", "movies"],
            "2019-10-12",
            [365, 730, 1460, 2920],
            20,
            [
                "USER_ID",
                "pageview_news_dur",
                "pageview_news_fre_365",
                "pageview_news_fre_730",
                "pageview_news_fre_1460",
                "pageview_news_fre_2920",
                "pageview_movies_dur",
                "pageview_movies_fre_365",
                "pageview_movies_fre_730",
                "pageview_movies_fre_1460",
                "pageview_movies_fre_2920",
            ],
        ),
        (
            ["fre", "dur"],
            ["news"],
            "2019-10-12",
            [365, 730, 1460, 2920],
            18,
            [
                "USER_ID",
                "pageview_news_dur",
                "pageview_news_fre_365",
                "pageview_news_fre_730",
                "pageview_news_fre_1460",
                "pageview_news_fre_2920",
            ],
        ),
    ],
)
def test_calculate_rf_metrics_for_page_type(
    sample_data: AnyTuple,
    metric_types: StringList,
    page_types: StringList,
    reference_date: str,
    time_windows: IntList,
    count: int,
    names: StringList,
):
    fact_df, lookup_df = sample_data

    fact_df = fact_df.filter(fact_df.EVENT_DATE < F.lit(reference_date))
    lookup_df = lookup_df.filter(lookup_df.WEBPAGE_TYPE.isin(page_types))
    fact_df = fact_df.join(lookup_df, "WEB_PAGEID").drop("WEB_PAGEID")

    assert (
        calculate_rf_metrics(
            fact_df, metric_types, page_types, reference_date, time_windows
        ).count()
        == count
    )
    assert (
        calculate_rf_metrics(
            fact_df, metric_types, page_types, reference_date, time_windows
        ).schema.names
        == names
    )
