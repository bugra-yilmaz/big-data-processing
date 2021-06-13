import os
import argparse
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import date_add, datediff, col, sum, max, lit


def parse_values(argument):
    return [x.strip() for x in argument.split(',') if x.strip()]


if __name__ == '__main__':
    argument_parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    argument_parser.add_argument('-p', '--page-type', help='Page types - separated by comma.', dest='p', metavar='')
    argument_parser.add_argument('-m', '--metric-type', help='Metric types - separated by comma.', dest='m', metavar='')
    argument_parser.add_argument('-t', '--time-window', help='Time windows - separated by comma.', dest='t', metavar='')
    argument_parser.add_argument('-d', '--date-ref', help='Reference date.',
                                 dest='d', metavar='')
    args = argument_parser.parse_args()

    page_types = parse_values(args.p)
    metric_types = parse_values(args.m)
    time_windows = list(map(int, parse_values(args.t)))
    reference_date = datetime.datetime.strptime(args.d, '%d/%m/%Y').strftime('%Y-%m-%d')

    dir_path = os.path.dirname(os.path.realpath(__file__))
    fact_file_path = os.path.join(dir_path, 'fact.csv')
    lookup_file_path = os.path.join(dir_path, 'lookup.csv')

    spark = SparkSession.builder.appName('adidas-bigdata-processing').getOrCreate()

    schema_fact = StructType([
        StructField('USER_ID', IntegerType(), False),
        StructField('EVENT_DATE', DateType(), False),
        StructField('WEB_PAGEID', IntegerType(), False)
    ])

    schema_lookup = StructType([
        StructField('WEB_PAGEID', IntegerType(), False),
        StructField('WEBPAGE_TYPE', StringType(), False),
    ])

    fact = spark.read.csv(fact_file_path, schema=schema_fact, header=True, dateFormat='dd/MM/yyyy HH:mm')
    lookup = spark.read.csv(lookup_file_path, schema=schema_lookup, header=True)

    # Remove data with dates later than the reference date
    fact = fact.filter(fact.EVENT_DATE < reference_date)

    # Join dataframes
    joined = fact.join(lookup, 'WEB_PAGEID').select(col('USER_ID'), col('EVENT_DATE'), col('WEBPAGE_TYPE'),
                                                    lit(reference_date).alias('REFERENCE_DATE'))

    # Remove data with other page types
    joined = joined.filter(joined.WEBPAGE_TYPE.isin(page_types))

    # Calculate end dates and time window matches for given time windows
    result = None
    fre_columns = []
    for page_type in page_types:
        aggs = []
        df = joined.filter(joined.WEBPAGE_TYPE == page_type)

        if 'dur' in metric_types:
            aggs.append(datediff(max(df.REFERENCE_DATE), max(df.EVENT_DATE)).alias(f'pageview_{page_type}_dur'))

        if 'fre' in metric_types:
            for time_window in time_windows:
                df = df. \
                    withColumn(f'{time_window}_match',
                               ((date_add(df.EVENT_DATE, time_window) > df.REFERENCE_DATE).cast('integer')))

                fre_column = f'pageview_{page_type}_fre_{time_window}'
                aggs.append(sum(col(f'{time_window}_match')).alias(fre_column))
                fre_columns.append(fre_column)

        df = df.groupby(['USER_ID']).agg(*aggs)

        if result:
            result = result.join(df, 'USER_ID', how='full')
        else:
            result = df

    if 'fre' in metric_types:
        result = result.fillna(0, fre_columns)

    result.show(100)
