import os
import argparse
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType


def parse_values(argument):
    return [x.strip() for x in argument.split(',')]


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
