## Ingest & Transform of stock data on Azure Blob Storage using Pyspark
• Details coming soon
![spark-df](/screenshots/spark_df.png)



## Analytical ETL

### Step 3
• Moving Avg
![step_1_moving_avg](/screenshots/step_1_moving_avg.png)


### Step 2
• Previous day closing prices
![step_2_earlier_day_closing_pr](/screenshots/step_2_earlier_day_closing_pr.png)


### Step 3
• Union Quote and Trade records
![step_3_union_quote_trade](/screenshots/step_3_union_quote_trade.png)


### Step 4
• Last trade moving average
![step_4_last_trade_moving_avg](/screenshots/step_4_last_trade_moving_avg.png)


### Step 5
• Filter for Quote records
![step_5_filter_quote_records](/screenshots/step_5_filter_quote_records.png)


### Step 6
• Broadcast join 
![step_6_final_broadcast_join](/screenshots/step_6_final_broadcast_join.png)



## Track Job Status

### Extract
• Succesful extract.py job run
![extract_success](/screenshots/extract_success.png)


### End of day load
• Succesful EOD_load.py job run
![eod_quote](/screenshots/eod_quote.png)
![eod_trade_success](/screenshots/eod_trade_success.png)


### Analytics
• Succesful analytical_ETL.py job run
![analytics_success](/screenshots/analytics_success.png)


## Job status in PostgreSQL table
• Succesfully updated job status table for each job run
![job_tracker_postgres_table](/screenshots/job_tracker_postgres_table.png)



