import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as F

from main import parse_argument, get_spark_session, filter_and_join_data, get_intermediate_dataframe


# Provide Spark session fixture for unit tests
@pytest.fixture
def spark_session():
    spark = SparkSession.builder.appName('adidas-bigdata-processing').master('local[*]').getOrCreate()
    return spark


# Test argument parsing for command line arguments
def test_parse_values():
    argument, parsed = 'news, movies', ['news', 'movies']
    assert parsed == parse_argument(argument)

    argument, parsed = 'fre,dur  ', ['fre', 'dur']
    assert parsed == parse_argument(argument)

    argument, parsed = 'news,', ['news']
    assert parsed == parse_argument(argument)

    argument, parsed = '365,730, 1460,2920', ['365', '730', '1460', '2920']
    assert parsed == parse_argument(argument)


# Test spark session generation
def test_get_spark_session():
    spark_session = get_spark_session()
    assert type(spark_session) == SparkSession

    data = [(710, '23/01/2017 15:43', 157), (446, '12/04/2016 09:09', 138),
            (538, '28/04/2016 00:24', 196), (368, '27/04/2016 22:24', 196),
            (9, '12/01/2018 13:48', 111)]
    columns = ['USER_ID', 'EVENT_DATE', 'WEB_PAGEID']

    df = spark_session.createDataFrame(data, schema=columns)
    assert type(df) == DataFrame


# Test initial filtering with reference date and page types, and join with lookup dataframe
def test_filter_and_join_data(spark_session):
    fact = [(710, '2016-08-18 09:42', 102), (710, '2016-02-29 14:24', 155), (619, '2016-03-01 15:27', 102),
            (619, '2016-03-25 00:51', 188), (180, '2016-04-28 00:24', 196), (623, '2016-01-31 21:45', 126),
            (710, '2016-04-27 22:24', 196), (87, '2018-11-26 22:04', 112), (479, '2016-04-27 22:24', 196),
            (320, '2017-01-17 01:20', 149), (540, '2016-04-28 00:24', 196), (644, '2016-02-20 13:12', 190),
            (970, '2016-04-28 00:24', 196), (970, '2016-06-30 14:02', 102), (701, '2016-01-17 11:40', 200),
            (9, '2018-01-12 13:48', 111), (507, '2016-07-21 16:51', 157), (701, '2016-02-13 07:32', 102),
            (651, '2016-04-28 00:24', 196), (160, '2016-04-28 00:24', 196), (710, '2016-04-27 22:24', 196),
            (189, '2016-02-28 15:58', 102), (658, '2016-08-18 20:53', 166), (420, '2016-04-27 22:24', 196),
            (710, '2016-04-27 22:24', 196), (47, '2018-11-26 22:04', 112), (285, '2016-02-03 08:49', 187)]

    lookup = [(102, 'news'), (155, 'news'), (188, 'movies'), (196, 'news'), (126, 'movies'), (112, 'news'),
              (149, 'news'), (190, 'news'), (200, 'news'), (111, 'news'), (157, 'news'), (166, 'news'),
              (187, 'movies')]
    fact_schema = StructType([
        StructField('USER_ID', IntegerType(), False),
        StructField('EVENT_DATE', StringType(), False),
        StructField('WEB_PAGEID', IntegerType(), False)
    ])
    lookup_schema = StructType([
        StructField('WEB_PAGEID', IntegerType(), False),
        StructField('WEBPAGE_TYPE', StringType(), False),
    ])

    fact_df = spark_session.createDataFrame(fact, schema=fact_schema)
    fact_df = fact_df.withColumn('EVENT_DATE', F.to_timestamp('EVENT_DATE'))
    lookup_df = spark_session.createDataFrame(lookup, schema=lookup_schema)

    reference_date, page_types = '2019-10-12', ['sports']
    joined_filtered_df = filter_and_join_data(fact_df, lookup_df, reference_date, page_types)
    assert joined_filtered_df.count() == 0

    reference_date, page_types = '2017-01-01', ['news']
    joined_filtered_df = filter_and_join_data(fact_df, lookup_df, reference_date, page_types)
    assert joined_filtered_df.count() == 20


# Test main computation stage where temporary dataframes are generated per page type with all metrics and time windows
def test_get_intermediate_dataframe(spark_session):
    fact = [(710, '2016-08-18 09:42', 102), (710, '2016-02-29 14:24', 155), (619, '2016-03-01 15:27', 102),
            (619, '2016-03-25 00:51', 188), (180, '2016-04-28 00:24', 196), (623, '2016-01-31 21:45', 126),
            (710, '2016-04-27 22:24', 196), (87, '2018-11-26 22:04', 112), (479, '2016-04-27 22:24', 196),
            (320, '2017-01-17 01:20', 149), (540, '2016-04-28 00:24', 196), (644, '2016-02-20 13:12', 190),
            (970, '2016-04-28 00:24', 196), (970, '2016-06-30 14:02', 102), (701, '2016-01-17 11:40', 200),
            (9, '2018-01-12 13:48', 111), (507, '2016-07-21 16:51', 157), (701, '2016-02-13 07:32', 102),
            (651, '2016-04-28 00:24', 196), (160, '2016-04-28 00:24', 196), (710, '2016-04-27 22:24', 196),
            (189, '2016-02-28 15:58', 102), (658, '2016-08-18 20:53', 166), (420, '2016-04-27 22:24', 196),
            (710, '2016-04-27 22:24', 196), (47, '2018-11-26 22:04', 112), (285, '2016-02-03 08:49', 187)]

    lookup = [(102, 'news'), (155, 'news'), (188, 'movies'), (196, 'news'), (126, 'movies'), (112, 'news'),
              (149, 'news'), (190, 'news'), (200, 'news'), (111, 'news'), (157, 'news'), (166, 'news'),
              (187, 'movies')]
    fact_schema = StructType([
        StructField('USER_ID', IntegerType(), False),
        StructField('EVENT_DATE', StringType(), False),
        StructField('WEB_PAGEID', IntegerType(), False)
    ])
    lookup_schema = StructType([
        StructField('WEB_PAGEID', IntegerType(), False),
        StructField('WEBPAGE_TYPE', StringType(), False),
    ])
    fact_df = spark_session.createDataFrame(fact, schema=fact_schema)
    fact_df = fact_df.withColumn('EVENT_DATE', F.to_timestamp('EVENT_DATE'))
    lookup_df = spark_session.createDataFrame(lookup, schema=lookup_schema)

    reference_date, page_types = '2017-10-12', ['news', 'movies']
    fact_df = filter_and_join_data(fact_df, lookup_df, reference_date, page_types)

    metric_types, page_type, time_windows = ['fre'], 'news', [365, 730]
    temp_df = get_intermediate_dataframe(fact_df, metric_types, page_type, reference_date, time_windows, [])
    assert temp_df.select(F.sum('pageview_news_fre_365')).collect()[0][0] == 1
    assert temp_df.select(F.sum('pageview_news_fre_730')).collect()[0][0] == 21

    metric_types, page_type, time_windows = ['fre', 'dur'], 'movies', [365, 730]
    temp_df = get_intermediate_dataframe(fact_df, metric_types, page_type, reference_date, time_windows, [])
    assert [val.pageview_movies_dur for val in temp_df.select('pageview_movies_dur').collect()] == [620, 617, 566]
    assert temp_df.select(F.sum('pageview_movies_fre_730')).collect()[0][0] == 3

    metric_types, page_type, time_windows = ['dur'], 'news', [365, 730]
    temp_df = get_intermediate_dataframe(fact_df, metric_types, page_type, reference_date, time_windows, [])
    assert temp_df.schema.names == ['USER_ID', 'pageview_news_dur']
    assert [val.pageview_news_dur for val in temp_df.select('pageview_news_dur').collect()] == \
           [532, 469, 533, 448, 600, 592, 590, 420, 268, 532, 532, 420, 533, 532, 607]
