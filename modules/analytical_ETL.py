from pyspark.sql import SparkSession
import logging

from toml_config import config
from job_tracker import Tracker

# intantiate Tracker
tracker = Tracker(config)


#######################################################################
####################         BUILD SPARK          #####################       
#######################################################################

spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName('SpringCapital_Analytical_ETL') \
    .getOrCreate()

# set number of partitions to match RDD partitions on blob storage, reduce shuffles
spark.conf.set('spark.sql.shuffle.partitions', 4)


#######################################################################
#################      EOD DATA DIRECTORY PATHS     ###################       
#######################################################################

# base path for EOD trade and quote data
blob_base = f"{config['AZURE']['PREFIX']}{config['AZURE']['CONTAINER']}@{config['AZURE']['STORAGE_ACC']}{config['AZURE']['SUFFIX']}"

# trade paths
trade_base_path = f"{blob_base}{config['EOD']['TRADE']}"
trade_all = f'{trade_base_path}*'
trade_earlier = f"{trade_base_path}{config['EOD']['EARLIER_DATE']}"
# trade_later = f"{trade_base_path}{config['EOD']['LATER_DATE']}"

# quote paths
quote_base_path = f"{blob_base}{config['EOD']['QUOTE']}"
quote_all = f'{quote_base_path}*'
# quote_earlier = f"{quote_base_path}{config['EOD']['EARLIER_DATE']}"
# quote_later = f"{quote_base_path}{config['EOD']['LATER_DATE']}"


########################################################################
####################            DATAFRAMES           ###################
########################################################################

trade_all_df = spark.read.parquet(trade_all)
# trade_later_df = spark.read.parquet(trade_later)
trade_earlier_df = spark.read.parquet(trade_earlier)

quote_all_df = spark.read.parquet(quote_all)
# quote_earlier_df = spark.read.parquet(quote_earlier)
# quote_later_df = spark.read.parquet(quote_later)



class EOD_Analytics:

    """
    EOD_Analytics class performs end of day analytics on two given days of stock prices.
    Writes analytics report out to Azure blob storage. 
    :param config:
    :type .toml config file:
    """

    def __init__(self, config):
        self.config = config


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


    def analytics(self, *df):

        """
        Performs analytics on two given days of stock prices. 
        :param *df:
        :type Spark DataFrame(s):
        """

        ########################################################################
        ####################              STEP 1             ###################
        ########################################################################

        # create view from all trade records to derive moving average of trade price based on previous trade record value
        trade_all_df.createOrReplaceTempView('trade_all_view')

        moving_avg = spark.sql("""
                                SELECT *, AVG(trade_pr)
                                    OVER(
                                        PARTITION BY symbol, exchange
                                        ORDER BY event_tm
                                        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS moving_avg
                                FROM trade_all_view
                                ORDER BY event_tm;
                                """)

        # # visualize results
        # moving_avg.show(truncate=False)
        # print(f'Number of records: {moving_avg.count()}')

        moving_avg.createOrReplaceTempView('trade_moving_avg_view')


        ########################################################################
        ####################              STEP 2             ###################
        ########################################################################

        # create view of trade records from earlier day
        trade_earlier_df.createOrReplaceTempView('trade_earlier_view')

        # only select last records for each symbol/exchange/date combo
        last_trade = spark.sql("""
                                SELECT trade_dt, symbol, exchange, trade_pr AS closing_pr 
                                FROM
                                    (SELECT trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_pr, ROW_NUMBER()
                                        OVER(
                                            PARTITION BY symbol, exchange
                                            ORDER BY event_tm, event_seq_nb DESC) as row_number
                                    FROM trade_earlier_view) 
                                    WHERE row_number = 1;
                                """)
            
        # create view of the last trades to later be used for broadcast join in final step
        last_trade.createOrReplaceTempView('last_trade_view')

        # # visualize results
        # last_trade.show(truncate=False)
        # print(f'Number of records: {last_trade.count()}')


        ########################################################################
        ####################              STEP 3             ###################
        ########################################################################

        # create view of all quote records 
        quote_all_df.createOrReplaceTempView('quote_all_view')

        # create denormalized union view of trade and quote records by including all
        # columns for each record type and inserting null values for newly added columns.
        # re-create 'T' and 'Q' columns to later filter on in step 5.
        quote_trade_union = spark.sql("""
                                SELECT trade_dt, symbol, exchange, event_tm, 
                                    event_seq_nb, arrival_tm, null AS trade_pr, 
                                    null AS moving_avg, bid_pr, bid_size, ask_pr, 
                                    ask_size, 'Q' AS rec_type
                                FROM quote_all_view
                                UNION
                                SELECT trade_dt, symbol, exchange, event_tm, 
                                    event_seq_nb, arrival_tm, trade_pr, 
                                    moving_avg, null AS bid_pr, NULL AS bid_size, 
                                    null AS ask_pr, null AS ask_size, 'T' AS rec_type
                                FROM trade_moving_avg_view
                                ORDER BY event_tm;
                                """)

        # # visualize results
        # quote_trade_union.show(truncate=False)
        # print(f'Number of records: {quote_trade_union.count()}')


        ########################################################################
        ####################              STEP 4             ###################
        ########################################################################

        # create view of the denormalized union of trade and quote records
        quote_trade_union.createOrReplaceTempView('quote_trade_union_view')

        # use a window function to select last non-null value (TRUE) for trade_pr and moving_avg columns,
        # create new colomns holding these values; last_trade_pr and last_moving_avg_pr respectively
        quote_trade_update = spark.sql("""
                                        SELECT *, LAST_VALUE(trade_pr, TRUE) 
                                            OVER w AS last_trade_pr, 

                                        LAST_VALUE(moving_avg, TRUE) 
                                            OVER w AS last_moving_avg_pr
                                        
                                        FROM quote_trade_union_view
                            
                                        WINDOW w AS (PARTITION BY symbol, exchange ORDER BY event_tm
                                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
                                        """)

        # # visualize results
        # quote_trade_update.show(truncate=False)
        # print(f'Number of records: {quote_trade_update.count()}')


        ########################################################################
        ####################              STEP 5             ###################
        ########################################################################

        # create updated view of unionized quote and trade records with newly created 
        # last_trade_pr and last_moving_avg_pr columns
        quote_trade_update.createOrReplaceTempView('quote_trade_update_view')

        # select desired columns (replacing original trade_pr and moving_avg columns with last_trade_pr and last_moving_avg_pr columns)
        # filter on rec_type = 'Q' to only return quote records 
        quote_update = spark.sql("""
                                SELECT trade_dt, symbol, exchange, event_tm, event_seq_nb, bid_pr, 
                                    bid_size, ask_pr, ask_size, last_trade_pr, last_moving_avg_pr
                                FROM quote_trade_update_view
                                WHERE rec_type = 'Q';
                                """)

        # # visualize results
        # quote_update.show(truncate=False)
        # print(f'Number of records: {quote_update.count()}')


        ########################################################################
        ####################              STEP 6             ###################
        ########################################################################

        # create view of updated quote records containing all necessary data for desired computations
        quote_update.createOrReplaceTempView('quote_trade_update_final_view')

        # use broadcast join to join records from last_trade_view (containing closing price values from earlier day for each symbol/exchange combo) 
        # use earlier days closing price column to calculate movement for each bid price in relation to earlier day closing price 
        # use earlier days closing price column to calculate movement for each ask price in relation to earlier day closing price
        quote_final = spark.sql("""
                                SELECT /*+ BROADCASTJOIN(t) */
                                    q.trade_dt, q.symbol, q.exchange, q.event_tm, q.event_seq_nb, q.bid_pr, 
                                    q.bid_size, q.ask_pr, q.ask_size, q.last_trade_pr, q.last_moving_avg_pr, 
                                    q.bid_pr - t.closing_pr AS bid_pr_movement,
                                    q.ask_pr - t.closing_pr AS ask_pr_movement
                                FROM quote_trade_update_final_view q
                                LEFT JOIN last_trade_view t
                                WHERE
                                    q.symbol = t.symbol AND
                                    q.exchange = t.exchange
                                ORDER BY trade_dt DESC, symbol, exchange, event_tm DESC;
                                """)

        # visualize results
        quote_final.show(truncate=False)
        print(f'Number of records: {quote_final.count()}')


        ########################################################################
        ####################              STEP 7             ###################
        ########################################################################

        # extract dates from final dataframe, write dataframes to blob storage
        # partitioned by values within trade_dt column
        try:
            date_pool = EOD_Analytics(config).extract_dates(quote_final)

            for trade_dt in date_pool:

                output_dir = f"{blob_base}{config['EOD']['ANALYTIC']}{config['EOD']['REPORT']}{trade_dt}"

                # create DF with records matching parsed trade dates 
                quote_final_partitioned = quote_final.filter(quote_final['trade_dt'] == trade_dt)

                # write trades to blob storage as parquet files partitioned by trade_dt
                print(f'Writing EOD REPORT for ** {trade_dt} ** to blob storage...')
                quote_final_partitioned.write.mode('overwrite').parquet(output_dir)
                print(f'Successfully wrote EOD REPORT for ** {trade_dt} ** to {output_dir}')

            # if execution successful, update job status tracker table for each record date to 'succeeded'
            tracker.update_job_status(target='report', dates=date_pool, status='succeeded')

        except Exception as e:
            logging.Exception(f'Encountered exception while attempting to write ** {trade_dt} EOD REPORT ** DataFrame to blob storage:\n{e}')
            print(f'Encountered exception while attempting to write ** {trade_dt} EOD REPORT ** DataFrames to blob storage:\n{e}')

            date_pool = EOD_Analytics(config).extract_dates(quote_final)

            # if execution failed, update job status tracker table for each record date to 'failed'
            tracker.update_job_status(target='report', dates=date_pool, status='failed')


if __name__ == '__main__':

    EOD_Analytics(config).analytics(trade_all_df, trade_earlier_df, quote_all_df)
    