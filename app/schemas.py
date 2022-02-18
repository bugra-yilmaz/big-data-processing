import pyspark.sql.types as T

visit_schema = T.StructType([
        T.StructField('USER_ID', T.IntegerType(), False),
        T.StructField('EVENT_DATE', T.DateType(), False),
        T.StructField('WEB_PAGEID', T.IntegerType(), False)
    ])

page_type_schema = T.StructType([
    T.StructField('WEB_PAGEID', T.IntegerType(), False),
    T.StructField('WEBPAGE_TYPE', T.StringType(), False),
])
