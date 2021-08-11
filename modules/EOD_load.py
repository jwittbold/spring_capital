from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from datetime import date
from pyspark.sql.types import StringType


########################################################################
#####################         BUILD SPARK          #####################       
########################################################################

spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName('SpringCapital_EOD_Load') \
    .getOrCreate()

# set number of partitions to match RDD partitions on blob storage
spark.conf.set('spark.sql.shuffle.partitions', 4)


########################################################################
####################        AZURE BLOB PATHS        ####################       
########################################################################

# base path to stock data on Azure blob storage
blob_container = 'wasbs://stockdata@springcapitalstorage.blob.core.windows.net'

# full paths to temp staged 'trade', 'quote' and 'bad' records 
trade_common = spark.read.parquet(f'{blob_container}/output_dir/partition=T')
quote_common = spark.read.parquet(f'{blob_container}/output_dir/partition=Q')
bad_records = spark.read.parquet(f'{blob_container}/output_dir/partition=B')


########################################################################
####################        COMMON DATAFRAMES        ###################       
########################################################################

# select only necessary columns for trade records
trade_df = trade_common.select('trade_dt', 'symbol', 'exchange', 'event_tm', 'event_seq_nb', 'arrival_tm', 'trade_pr')
# print(trade_df.rdd.getNumPartitions())

# select only necessary columns for quote records
quote_df = quote_common.select('trade_dt', 'symbol', 'exchange', 'event_tm', 'event_seq_nb', 'arrival_tm', 'bid_pr', 'bid_size', 'ask_pr', 'ask_size')
# print(quote_df.rdd.getNumPartitions())

# assign dataframes to common variable to apply date correction in for loop
dataframes = [trade_df, quote_df]


########################################################################
####################        EOD_LOADER CLASS         ###################       
########################################################################

class EOD_Loader:
    """
    Class for loading end of day (EOD) stock prices.
    """

    def __init__(self, df):
        self.df = df


    def apply_latest(self, df):
        """
        Returns DataFrame containing only unique records based on set of columns and filters for only the newest records.
        """

        # creates partition containing only unique records
        unique_record = Window.partitionBy('trade_dt', 'symbol', 'exchange', 'event_tm', 'event_seq_nb') 

        # filter to return only the latest unique records within partition
        return df.withColumn('latest', F.max('arrival_tm').over(unique_record)) \
            .where(F.col('latest') == F.col('arrival_tm')) \
            .drop(F.col('latest')) 

    
    def extract_dates(self, df):
        """
        Selects disctinct dates from 'trade_dt' column and adds to list for use in creating date partitions
        """
        trade_dt_arr = df.select('trade_dt').distinct().rdd.flatMap(lambda row: row).collect()

        trade_dt_lst = []

        for trade_dt in trade_dt_arr:

            trade_dt_str = trade_dt.strftime('%Y-%m-%d')
            trade_dt_lst.append(trade_dt_str)

        return trade_dt_lst


########################################################################
####################        EXECUTE EOD LOAD         ###################       
########################################################################

if __name__ == '__main__':

    for df in dataframes:

        try:
            if df == trade_df:

                trade_corrected = EOD_Loader(df).apply_latest(trade_df)
                # trade_corrected.show()
                # print(trade_corrected.count())

                date_pool = EOD_Loader(df).extract_dates(trade_df)

                for trade_dt in date_pool:
                    output_dir = f'{blob_container}/trade/trade_dt={trade_dt}'

                    # create DF with records matching parsed trade dates 
                    trade_dt_partitioned = trade_corrected.filter(trade_corrected['trade_dt'] == trade_dt)

                    # write trades to blob storage as parquet files partitioned by trade_dt
                    print(f'Writing ** TRADES ** for {trade_dt} to blob storage...')
                    trade_dt_partitioned.write.mode('append').parquet(output_dir)
                    print(f'Successfully wrote ** TRADES ** for {trade_dt} to {output_dir}')
        
        except Exception as e:
            logging.Exception(f'Encountered exception while attempting to write ** TRADE ** DataFrames to blob storage:\n{e}')
            print(f'Encountered exception while attempting to write ** TRADE ** DataFrames to blob storage:\n{e}')

        try:
            if df == quote_df:
                
                quote_corrected = EOD_Loader(df).apply_latest(quote_df)
                # quote_corrected.show()
                # print(quote_corrected.count())

                date_pool = EOD_Loader(df).extract_dates(quote_df)

                for trade_dt in date_pool:
                    output_dir = f'{blob_container}/quote/trade_dt={trade_dt}'

                    # create DF with records matching parsed trade dates 
                    trade_dt_partitioned = quote_corrected.filter(quote_corrected['trade_dt'] == trade_dt)
 
                    # write quotes to blob storage as parquet files partitioned by trade_dt 
                    print(f'Writing ** QUOTES ** for {trade_dt} to blob storage...')
                    trade_dt_partitioned.write.mode('append').parquet(output_dir)
                    print(f'Successfully wrote ** QUOTES ** for {trade_dt} to {output_dir}')
        
        except Exception as e:
            logging.Exception(f'Encountered exception while attempting to write ** QUOTE ** DataFrames to blob storage:\n{e}')
            print(f'Encountered exception while attempting to write ** QUOTE ** DataFrames to blob storage:\n{e}')