from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DecimalType, DateType, TimestampType, StructType, StructField
from CommonClass import CommonEvent
from CustomSchema import common_event_schema
import datetime 
from decimal import Decimal
import json



def parse_json(line):

    record = json.loads(line)
    record_type = record['event_type']

    try:

        if record_type == 'T':
            
            trade_event = CommonEvent(trade_dt = datetime.datetime.strptime(record['trade_dt'], '%Y-%m-%d'),
                                    arrival_tm = datetime.datetime.strptime(record['file_tm'], '%Y-%m-%d %H:%M:%S.%f'),
                                    rec_type = str(record['event_type']),
                                    symbol = str(record['symbol']),
                                    event_tm = datetime.datetime.strptime(record['event_tm'], '%Y-%m-%d %H:%M:%S.%f'),
                                    event_seq_nb = int(record['event_seq_nb']),
                                    exchange = str(record['exchange']),
                                    trade_pr = Decimal(record['price']),
                                    trade_size = int(record['size']),
                                    bid_pr = Decimal(0.0),
                                    bid_size = int(0),
                                    ask_pr = Decimal(0.0),
                                    ask_size = int(0),
                                    partition = 'T',
                                    bad_rec = ''
                                    )

            return trade_event        

        elif record_type == 'Q':

            quote_event = CommonEvent(trade_dt = datetime.datetime.strptime(record['trade_dt'], '%Y-%m-%d'),
                                    arrival_tm = datetime.datetime.strptime(record['file_tm'], '%Y-%m-%d %H:%M:%S.%f'),
                                    rec_type = str(record['event_type']),
                                    symbol = str(record['symbol']),
                                    event_tm = datetime.datetime.strptime(record['event_tm'], '%Y-%m-%d %H:%M:%S.%f'),
                                    event_seq_nb = int(record['event_seq_nb']),
                                    exchange = str(record['exchange']),
                                    trade_pr = Decimal(0.0),
                                    trade_size = int(0),
                                    bid_pr = Decimal(record['bid_pr']),
                                    bid_size = int(record['bid_size']),
                                    ask_pr = Decimal(record['ask_pr']),
                                    ask_size = int(record['ask_size']),
                                    partition = 'Q',
                                    bad_rec = ''
                                    )
            return quote_event

        else:
            
            bad_event = CommonEvent(trade_dt = None,
                                    arrival_tm = None,
                                    rec_type = None,
                                    symbol = None,
                                    event_tm = None,
                                    event_seq_nb = None,
                                    exchange = None,
                                    trade_pr = None,
                                    trade_size = None,
                                    bid_pr = None,
                                    bid_size = None,
                                    ask_pr = None,
                                    ask_size = None,
                                    partition = 'B',
                                    bad_rec = line
                                    )
            return bad_event

    except Exception as e:
        print(f'Encounted exception:\n{e}')
        
        bad_event = CommonEvent(trade_dt = None,
                                arrival_tm = None,
                                rec_type = None,
                                symbol = None,
                                event_tm = None,
                                event_seq_nb = None,
                                exchange = None,
                                trade_pr = None,
                                trade_size = None,
                                bid_pr = None,
                                bid_size = None,
                                ask_pr = None,
                                ask_size = None,
                                partition = 'B',
                                bad_rec = line
                                )
        return bad_event