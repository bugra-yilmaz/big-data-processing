from typing import Callable
from typing import List

import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from schemas import page_type_schema
from schemas import visit_schema

PAGE_TYPES_FILE_PATH = "app/data/lookup.csv"
VISITS_FILE_PATH = "app/data/fact.csv"


class PageViewDataPreprocessor:
    DATE_COL = "EVENT_DATE"
    PAGE_TYPE_COL = "WEBPAGE_TYPE"
    PAGE_ID_COL = "WEB_PAGEID"

    def __init__(self, page_type_df: DataFrame):
        self.page_type_df = page_type_df

    @classmethod
    def filter_by_reference_date(cls, reference_date: str) -> Callable[[DataFrame], DataFrame]:
        return lambda visit_df: visit_df.filter(visit_df[cls.DATE_COL] < F.lit(reference_date))

    @classmethod
    def filter_by_page_types(cls, page_types: List[str]) -> Callable[[DataFrame], DataFrame]:
        return lambda page_type_df: page_type_df.filter(page_type_df[cls.PAGE_TYPE_COL].isin(page_types))

    def add_page_type_to_visits(self, filtered_page_type_df: DataFrame) -> Callable[[DataFrame], DataFrame]:
        return lambda visit_df: (
            visit_df
            .join(F.broadcast(filtered_page_type_df), [self.PAGE_ID_COL])
            .drop(self.PAGE_ID_COL)
        )

    def create_dataframe_for(self, visit_df: DataFrame, reference_date: str, page_types: List[str]) -> DataFrame:
        df = (
            visit_df
            .transform(self.filter_by_reference_date(reference_date))
            .transform(self.add_page_type_to_visits(self.page_type_df.transform(self.filter_by_page_types(page_types))))
        )
        return df


class UnknownMetricTypeError(Exception):
    pass


class MetricCalculator:
    DATE_COL = "EVENT_DATE"
    PAGE_TYPE_COL = "WEBPAGE_TYPE"
    USER_ID_COL = "USER_ID"

    def __init__(self, page_view_df: DataFrame, reference_date: str, page_types: List[str], time_windows: List[int],
                 metric_types: List[str]):
        self.page_view_df = page_view_df
        self.reference_date = reference_date
        self.page_types = page_types
        self.time_windows = time_windows
        self.metric_types = metric_types
        self.metric_column_names = self.get_metric_column_names()

    def get_metric_column_names(self) -> List[str]:
        metric_column_names = []
        for metric_type in self.metric_types:
            if metric_type == "fre":
                metric_column_names.extend(self.get_frequency_column_names())
            elif metric_type == "dur":
                metric_column_names.extend(self.get_recency_column_names())

        return metric_column_names

    def get_frequency_column_names(self) -> List[str]:
        frequency_column_names = []
        for page_type in self.page_types:
            for time_window in self.time_windows:
                frequency_column_names.append(f"pageview_{page_type}_fre_{time_window}")

        return frequency_column_names

    def get_recency_column_names(self) -> List[str]:
        recency_column_names = []
        for page_type in self.page_types:
            recency_column_names.append(f"pageview_{page_type}_dur")

        return recency_column_names

    def get_frequency_aggregations(self) -> List[Column]:
        frequency_aggregations = []
        for page_type in self.page_types:
            for time_window in self.time_windows:
                column_name = f"pageview_{page_type}_fre_{time_window}"

                page_type_aggregation = F.count(
                    F.when(
                        (
                            (self.page_view_df[self.PAGE_TYPE_COL] == F.lit(page_type)) &
                            (F.date_add(self.page_view_df[self.DATE_COL], time_window) > F.lit(self.reference_date))
                        ),
                        1
                    )
                ).alias(column_name)
                frequency_aggregations.append(page_type_aggregation)

        return frequency_aggregations

    def get_recency_aggregations(self) -> List[Column]:
        recency_aggregations = []
        for page_type in self.page_types:
            column_name = f"pageview_{page_type}_dur"

            page_type_aggregation = F.when(
                self.page_view_df[self.PAGE_TYPE_COL] == F.lit(page_type),
                F.datediff(
                    F.lit(self.reference_date), F.max(self.page_view_df[self.PAGE_TYPE_COL])
                ),
            ).alias(column_name)
            recency_aggregations.append(page_type_aggregation)

        return recency_aggregations

    def get_metric_aggregations(self) -> List[Column]:
        aggregations = []
        for metric_type in self.metric_types:
            if metric_type == "fre":
                aggregations.extend(self.get_frequency_aggregations())
            elif metric_type == "dur":
                aggregations.extend(self.get_recency_aggregations())
            else:
                raise UnknownMetricTypeError(f"Metric type: {metric_type}")

        return aggregations

    def clean_zero_recency_values(self, metrics_df: DataFrame) -> DataFrame:
        for recency_column_name in self.get_recency_column_names():
            metrics_df = metrics_df.withColumn(
                recency_column_name,
                F.when(metrics_df[recency_column_name] == 0, None).otherwise(metrics_df[recency_column_name])
            )

        return metrics_df

    def create_metrics_dataframe(self) -> DataFrame:
        metric_aggregations = self.get_metric_aggregations()
        sum_aggregations = [F.sum(column).alias(column) for column in self.metric_column_names]

        metrics_df = (
            self.page_view_df
            .groupby(self.USER_ID_COL, self.PAGE_TYPE_COL)
            .agg(*metric_aggregations)
            .fillna(0, self.metric_column_names)
            .groupby(self.USER_ID_COL)
            .agg(*sum_aggregations)
        )

        if "dur" in self.metric_types:
            metrics_df = self.clean_zero_recency_values(metrics_df)

        return metrics_df


if __name__ == '__main__':
    spark = SparkSession.builder.appName('ddd').master('local[*]').getOrCreate()

    reference_date = "2019-01-01"
    page_types = ["news", "movies"]
    time_windows = [365]
    metric_types = ["fre", "dur"]

    visit_df = spark.read.csv(VISITS_FILE_PATH, schema=visit_schema, header=True, dateFormat="dd/MM/yyyy HH:mm")
    page_type_df = spark.read.csv(PAGE_TYPES_FILE_PATH, schema=page_type_schema, header=True)

    preprocessor = PageViewDataPreprocessor(page_type_df)

    page_view_df = preprocessor.create_dataframe_for(visit_df, reference_date, page_types)

    metric_calculator = MetricCalculator(page_view_df, reference_date, page_types, time_windows, metric_types)

    metrics_df = metric_calculator.create_metrics_dataframe()
    metrics_df.show(1000)
