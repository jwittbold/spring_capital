from pyspark.sql.types import StringType, IntegerType, DecimalType, DateType, TimestampType, StructType, StructField


# PySpark schema 
common_event_schema = StructType([StructField('trade_dt', DateType(), True),
                        StructField('arrival_tm', TimestampType(), True),
                        StructField('rec_type', StringType(), True),
                        StructField('symbol', StringType(), True),
                        StructField('event_tm', TimestampType(), True),
                        StructField('event_seq_nb', IntegerType(), True),
                        StructField('exchange', StringType(), True),
                        StructField('trade_pr', DecimalType(precision=17, scale=14), True),
                        StructField('trade_size', IntegerType(), True),
                        StructField('bid_pr', DecimalType(precision=17, scale=14), True),
                        StructField('bid_size', IntegerType(), True),
                        StructField('ask_pr', DecimalType(precision=17, scale=14), True),
                        StructField('ask_size', IntegerType(), True),
                        StructField('partition', StringType(), True),
                        StructField('bad_rec', StringType(), True)
])