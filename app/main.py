# Created by bugra-yilmaz on 13.06.2021.
#
# Main Spark processing job.

# Imports
import os
import argparse
import datetime
from typing import List

import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
import pyspark.sql.functions as F

StringList = List[str]
IntList = List[int]


def parse_argument(argument: str) -> StringList:
    """
    Parses given command line argument by splitting it on commas and cleaning whitespaces.

    Args:
        argument (str): Command line argument.

    Returns:
        list[str]: List of argument values.

    """

    return [x.strip() for x in argument.split(",") if x.strip()]


def get_spark_session() -> pyspark.sql.SparkSession:
    """
    Generates a Spark session with all the available cores in the local machine.

    Returns:
        pyspark.sql.SparkSession: Spark session.

    """

    spark = (
        SparkSession.builder.appName("adidas-bigdata-processing")
        .master("local[*]")
        .getOrCreate()
    )
    return spark


def filter_by_date_and_join(
    fact_df: pyspark.sql.DataFrame,
    lookup_df: pyspark.sql.DataFrame,
    reference_date: str,
    page_types: StringList,
) -> pyspark.sql.DataFrame:
    """
    Filters the given 'fact' dataframe by reference date and page types.
    After removing all events later than the reference date, joins the resulting dataframe with the 'lookup' dataframe.
    Broadcasts the 'lookup' dataframe to the worker nodes.
    Finally, removes events that belong to other page types.

    Args:
        fact_df (pyspark.sql.DataFrame): 'fact' dataframe containing user events.
        lookup_df (pyspark.sql.DataFrame): 'lookup' dataframe containing page id - page type pairs.
        reference_date (str): Reference date in 'yyyy-MM-dd' format.
        page_types (list[str]): List of requested page types.

    Returns:
        pyspark.sql.DataFrame: Filtered and joined dataframe.

    """

    # Remove events with dates later than the reference date as we don't need them in the analysis
    fact_df = fact_df.filter(fact_df.EVENT_DATE < F.lit(reference_date))

    # Join dataframes, broadcast the lookup dataframe as it is a small, static file that does not need to be partitioned
    fact_df = fact_df.join(F.broadcast(lookup_df), "WEB_PAGEID").drop("WEB_PAGEID")

    # Remove data with other page types as we don't need them in the analysis
    fact_df = fact_df.filter(fact_df.WEBPAGE_TYPE.isin(page_types))

    return fact_df


def get_intermediate_dataframe(
    fact_df: pyspark.sql.DataFrame,
    metric_types: StringList,
    page_type: str,
    reference_date: str,
    time_windows: IntList,
    frequency_column_names: StringList,
) -> pyspark.sql.DataFrame:
    """
    Generates an intermediate dataframe from the given dataframe, containing all the metrics for a single page type.
    Resulting dataframes will later be joined to produce the final result.

    Args:
        fact_df (pyspark.sql.DataFrame): 'fact' dataframe containing user events matched to the page types.
        metric_types (list[str]): List of requested metrics e.g. ['fre', 'dur'].
        page_type (str): A single page type assigned to the intermediate dataframe.
        reference_date (str): Reference date in 'yyyy-MM-dd' format.
        time_windows (list[int]): Time windows to be used for frequency metric calculation, e.g [365, 730, 1460].
        frequency_column_names (list[str]): List of frequency column names, to be used for replacing null values
        with zeros.

    Returns:
        pyspark.sql.DataFrame: Intermediate dataframe containing calculated metrics for a single page type.

    """

    aggs = []

    # Keep data with a single page type to calculate metrics per page type
    temp_df = fact_df.filter(fact_df.WEBPAGE_TYPE == F.lit(page_type))

    if "dur" in metric_types:
        recency_column_name = f"pageview_{page_type}_dur"

        # Calculate recency metric as the difference between latest event date and the reference date
        agg = F.datediff(F.lit(reference_date), F.max(temp_df.EVENT_DATE)).alias(
            recency_column_name
        )
        aggs.append(agg)

    if "fre" in metric_types:
        for time_window in time_windows:
            frequency_column_name = f"pageview_{page_type}_fre_{time_window}"

            # Calculate frequency metric as a count of event dates within the time window
            agg = F.count(
                F.when(
                    F.date_add(temp_df.EVENT_DATE, time_window) > F.lit(reference_date),
                    1,
                )
            ).alias(frequency_column_name)
            aggs.append(agg)

            # Keep frequency column names to replace null values later
            frequency_column_names.append(frequency_column_name)

    # Apply all the aggregates on the intermediate dataframe to obtain requested metrics
    temp_df = temp_df.groupby(["USER_ID"]).agg(*aggs)

    return temp_df


def merge_csv_files(output_directory: str, output_file_path: str):
    """
    A helper function that merges .csv files generated by the worker nodes.

    Args:
        output_directory (str): Output directory that contains partitioned .csv files.
        output_file_path (str): Output file path that will store the merged .csv file.

    """

    # Collect partitioned .csv files
    file_paths = [
        os.path.join(output_directory, f)
        for f in os.listdir(output_directory)
        if f.endswith(".csv")
    ]

    # Accumulate all the content in the .csv files
    header = ""
    content = ""
    for file_path in file_paths:
        with open(file_path, "r") as f:
            header = next(f)
            content += f.read()
    content = header + content

    with open(output_file_path, "w") as f:
        f.write(content)


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    argument_parser.add_argument(
        "-p",
        "--page-type",
        help="Page types - separated by comma.",
        dest="p",
        metavar="",
    )
    argument_parser.add_argument(
        "-m",
        "--metric-type",
        help="Metric types - separated by comma.",
        dest="m",
        metavar="",
    )
    argument_parser.add_argument(
        "-t",
        "--time-window",
        help="Time windows - separated by comma.",
        dest="t",
        metavar="",
    )
    argument_parser.add_argument(
        "-d", "--date-ref", help="Reference date.", dest="d", metavar=""
    )
    argument_parser.add_argument(
        "-np",
        "--n-partitions",
        help="Number of partitions for the main dataframe.",
        dest="np",
        default=16,
        metavar="",
    )
    argument_parser.add_argument(
        "-o",
        "--output",
        help="Output file path.",
        dest="o",
        default="output",
        metavar="",
    )
    args = argument_parser.parse_args()

    # Define input file paths
    dir_path = os.path.dirname(os.path.realpath(__file__))
    fact_file_path = os.path.join(dir_path, "data", "fact.csv")
    lookup_file_path = os.path.join(dir_path, "data", "lookup.csv")

    # Parse command line arguments
    page_types = parse_argument(args.p)
    metric_types = parse_argument(args.m)
    time_windows = list(map(int, parse_argument(args.t)))
    reference_date = datetime.datetime.strptime(args.d, "%d/%m/%Y").strftime("%Y-%m-%d")

    spark = (
        SparkSession.builder.appName("adidas-bigdata-processing")
        .master("local[*]")
        .getOrCreate()
    )

    fact_schema = StructType(
        [
            StructField("USER_ID", IntegerType(), False),
            StructField("EVENT_DATE", DateType(), False),
            StructField("WEB_PAGEID", IntegerType(), False),
        ]
    )

    lookup_schema = StructType(
        [
            StructField("WEB_PAGEID", IntegerType(), False),
            StructField("WEBPAGE_TYPE", StringType(), False),
        ]
    )

    fact_df = spark.read.csv(
        fact_file_path, schema=fact_schema, header=True, dateFormat="dd/MM/yyyy HH:mm"
    )
    fact_df = fact_df.repartition(args.np, "USER_ID")

    lookup_df = spark.read.csv(lookup_file_path, schema=lookup_schema, header=True)

    fact_df = filter_by_date_and_join(fact_df, lookup_df, reference_date, page_types)

    result_df = None
    frequency_column_names = []
    for page_type in page_types:
        # Calculate intermediate dataframe - metrics for a single page type
        temp_df = get_intermediate_dataframe(
            fact_df,
            metric_types,
            page_type,
            reference_date,
            time_windows,
            frequency_column_names,
        )

        if result_df:
            # Full outer join to calculate null frequency and recency values
            result_df = result_df.join(temp_df, "USER_ID", how="full")
        # If not initialized, start with the first intermediate dataframe
        else:
            result_df = temp_df

    if "fre" in metric_types:
        result_df = result_df.fillna(0, frequency_column_names)

    result_df.write.option("header", "true").mode("overwrite").csv(args.o)
    merge_csv_files(args.o, args.o + ".csv")
