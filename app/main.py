import os
import argparse
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
import pyspark.sql.functions as F


def parse_values(argument):
    # Split argument strings on commas
    return [x.strip() for x in argument.split(',') if x.strip()]


def get_spark_session():
    # Create a Spark session with all the available cores in the local machine
    spark = SparkSession.builder.appName('adidas-bigdata-processing').master('local[*]').getOrCreate()

    return spark


def filter_and_join_data(fact_df, lookup_df, reference_date, page_types):
    # Remove data with dates later than the reference date
    fact_df = fact_df.filter(fact_df.EVENT_DATE < F.lit(reference_date))

    # Join dataframes - broadcast the small lookup dataframe
    fact_df = fact_df.join(F.broadcast(lookup_df), 'WEB_PAGEID').drop('WEB_PAGEID')

    # Remove data with other page types
    fact_df = fact_df.filter(fact_df.WEBPAGE_TYPE.isin(page_types))

    return fact_df


def get_intermediate_dataframe(fact_df, metric_types, page_type, reference_date, time_windows, frequency_column_names):
    aggs = []

    # Keep data with a single page type
    temp_df = fact_df.filter(fact_df.WEBPAGE_TYPE == F.lit(page_type))

    if 'dur' in metric_types:
        recency_column_name = f'pageview_{page_type}_dur'

        # Calculate recency metric as the difference between latest event date and the reference date
        agg = F.datediff(F.lit(reference_date), F.max(temp_df.EVENT_DATE)). \
            alias(recency_column_name)
        aggs.append(agg)

    if 'fre' in metric_types:
        for time_window in time_windows:
            frequency_column_name = f'pageview_{page_type}_fre_{time_window}'

            # Calculate frequency metric as a count of event dates within the time window
            agg = F.count(F.when(F.date_add(temp_df.EVENT_DATE, time_window) > F.lit(reference_date), 1)). \
                alias(frequency_column_name)
            aggs.append(agg)

            # Keep frequency column names to replace null values later
            frequency_column_names.append(frequency_column_name)

    # Apply all the aggregates on the intermediate dataframe
    temp_df = temp_df.groupby(['USER_ID']).agg(*aggs)

    return temp_df


if __name__ == '__main__':
    argument_parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    argument_parser.add_argument('-p', '--page-type', help='Page types - separated by comma.', dest='p', metavar='')
    argument_parser.add_argument('-m', '--metric-type', help='Metric types - separated by comma.', dest='m', metavar='')
    argument_parser.add_argument('-t', '--time-window', help='Time windows - separated by comma.', dest='t', metavar='')
    argument_parser.add_argument('-d', '--date-ref', help='Reference date.',
                                 dest='d', metavar='')
    args = argument_parser.parse_args()

    # Define input file paths
    dir_path = os.path.dirname(os.path.realpath(__file__))
    fact_file_path = os.path.join(dir_path, 'data', 'fact.csv')
    lookup_file_path = os.path.join(dir_path, 'data', 'lookup.csv')

    # Parse command line arguments
    page_types = parse_values(args.p)
    metric_types = parse_values(args.m)
    time_windows = list(map(int, parse_values(args.t)))
    reference_date = datetime.datetime.strptime(args.d, '%d/%m/%Y').strftime('%Y-%m-%d')

    spark = SparkSession.builder.appName('adidas-bigdata-processing').master('local[*]').getOrCreate()

    fact_schema = StructType([
        StructField('USER_ID', IntegerType(), False),
        StructField('EVENT_DATE', DateType(), False),
        StructField('WEB_PAGEID', IntegerType(), False)
    ])

    lookup_schema = StructType([
        StructField('WEB_PAGEID', IntegerType(), False),
        StructField('WEBPAGE_TYPE', StringType(), False),
    ])

    fact_df = spark.read.csv(fact_file_path, schema=fact_schema, header=True, dateFormat='dd/MM/yyyy HH:mm')

    # Partition the dataframe by user id
    fact_df = fact_df.repartition(8, 'USER_ID')

    lookup_df = spark.read.csv(lookup_file_path, schema=lookup_schema, header=True)

    fact_df = filter_and_join_data(fact_df, lookup_df, reference_date, page_types)

    result_df = None
    frequency_column_names = []
    for page_type in page_types:
        # Calculate intermediate dataframe - metrics for a single page type
        temp_df = get_intermediate_dataframe(fact_df, metric_types, page_type, reference_date,
                                             time_windows, frequency_column_names)

        # Join temporary (single page type) dataframe to the resulting dataframe by user id
        if result_df:
            # Full outer join to calculate null frequency and recency values
            result_df = result_df.join(temp_df, 'USER_ID', how='full')
        # If not initialized, start with the first intermediate dataframe
        else:
            result_df = temp_df

    # Replace null frequency values with 0
    if 'fre' in metric_types:
        result_df = result_df.fillna(0, frequency_column_names)

    # Coalesce resulting dataframe to a single partition to output a single .csv file
    result_df = result_df.coalesce(1)
    result_df.show(1000)
    result_df.write.option('header', 'true').mode('overwrite').csv('output')
