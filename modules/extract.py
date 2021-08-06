from pyspark.sql import SparkSession
from CustomSchema import common_event_schema
from CommonClass import CommonEvent
from ingest_csv import parse_csv
from ingest_json import parse_json
import os 
import logging



def extractor():
    """
    Builds SparkSession, authenticates Azure Storage credentials, 
    reads from blob storage, and writes parquet files to Blob storage as partitions. 
    """

    # build spark session 
    spark = SparkSession \
        .builder \
        .master('local') \
        .appName('SpringCapitalAzure') \
        .getOrCreate()

    print('======================= SparkSession built =======================')


    # set the credentials for accessing Azure storage
    try:
        access_key = os.getenv('AZURE_STORAGE_ACCESS_KEY')  # must $export AZURE_STORAGE_ACCESS_KEY='value of your access key' to store env variable
        spark.conf.set('fs.azure.account.key.springcapitalstorage.blob.core.windows.net', access_key)

        print('====== Successfully established connection to Azure Storage ======')
               
    except Exception as e:
        print(f'Encounterd exception while connecting to Azure Storage:\n{e}')
        logging.exception(f'Encounterd exception while connecting to Azure Storage:\n{e}')

    # extract and transform CSV data
    try:
        # use wildcard matching to target files spread across directories
        csv_data = 'wasbs://stockdata@springcapitalstorage.blob.core.windows.net/csv/*/NYSE/*.txt'

        # create RDD from contents of 'csv_data'
        raw = spark.sparkContext.textFile(csv_data)
        # parse all lines in RDD with csv_parser
        parsed = raw.map(lambda line: parse_csv(line))
        # create dataframe from RDD w/ custom schema applied
        data_csv = spark.createDataFrame(parsed, schema=common_event_schema)
        data_csv.show()
        print(f'{data_csv.count()} records in DataFrame')
        data_csv.printSchema()
        # data.show(data.count(), False)
        
    except Exception as e:
        print(f'Encountered exception during CSV extraction:\n{e}')
        logging.exception(f'Encountered exception during CSV extraction:\n{e}')

    # extract and transform JSON data
    try:
        # use wildcard matching to target files spread across directories
        json_data = 'wasbs://stockdata@springcapitalstorage.blob.core.windows.net/json/*/NASDAQ/*.txt'
        
        # create RDD from contents of 'json_data'
        raw = spark.sparkContext.textFile(json_data)
        # parse all lines in RDD with csv_parser
        parsed = raw.map(lambda line: parse_json(line))
        # create dataframe from RDD w/ custom schema applied
        data_json = spark.createDataFrame(parsed, schema=common_event_schema)
        data_json.show()
        print(f'{data_json.count()} records in DataFrame')
        # data.show(data.count(), False)
        data_json.printSchema()
                
    except Exception as e:
        print(f'Encountered exception during JSON extraction:\n{e}')
        logging.exception(f'Encountered exception during JSON extraction:\n{e}')

    # write transformed files out to 'B', 'T', or 'Q' partitions
    try:
        output_dir = 'wasbs://stockdata@springcapitalstorage.blob.core.windows.net/output_dir'
        
        # combine csv and json DataFrames to be written out in one pass
        combined_data = data_csv.union(data_json)
        combined_data.write.partitionBy('partition').mode('overwrite').parquet(output_dir)

        print(f'====== Parquet files successfully written to partitions in: {output_dir} ======')

    except Exception as e:
        print(f'Encountered exception while writing parquet files to blob:\n{e}')
        logging.exception(f'Encountered exception while writing parquet files to blob:\n{e}')

if __name__ == '__main__':

    extractor()