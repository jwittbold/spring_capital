from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DecimalType, DateType, TimestampType, StructType, StructField
from CommonClass import CommonEvent
from CustomSchema import common_event_schema
import datetime 
from decimal import Decimal




def parse_csv(line):

    record_type_index = 2
    record = line.split(',')

    try:

        if record[record_type_index] == 'T':

            trade_event = CommonEvent(trade_dt = datetime.datetime.strptime(record[0], '%Y-%m-%d'),
                                    arrival_tm = datetime.datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f'),
                                    rec_type = str(record[2]),
                                    symbol = str(record[3]),
                                    event_tm = datetime.datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f'),
                                    event_seq_nb = int(record[5]),
                                    exchange = str(record[6]),
                                    trade_pr = Decimal(record[7]),
                                    trade_size = int(record[8]),
                                    bid_pr = Decimal(0.0),
                                    bid_size = int(0),
                                    ask_pr = Decimal(0.0),
                                    ask_size = int(0),
                                    partition = 'T',
                                    bad_rec = ''
                                    )

            return trade_event        

        elif record[record_type_index] == 'Q':

            quote_event = CommonEvent(trade_dt = datetime.datetime.strptime(record[0], '%Y-%m-%d'),
                                    arrival_tm = datetime.datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f'),
                                    rec_type = str(record[2]),
                                    symbol = str(record[3]),
                                    event_tm = datetime.datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f'),
                                    event_seq_nb = int(record[5]),
                                    exchange = str(record[6]),
                                    trade_pr = Decimal(0.0),
                                    trade_size = int(0),
                                    bid_pr = Decimal(record[7]),
                                    bid_size = int(record[8]),
                                    ask_pr = Decimal(record[9]),
                                    ask_size = int(record[10]),
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
        print(f'Encountered exception:\n{e}')
        
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