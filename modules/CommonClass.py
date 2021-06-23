import datetime 
from decimal import Decimal


class CommonEvent:


    def __init__(self):
        self.trade_dt = datetime.datetime.strptime(self, '%Y-%m-%d')
        self.arrival_tm = datetime.datetime.strptime(self, '%Y-%m-%d %H:%M:%S.%f')
        self.rec_type = str('')
        self.symbol = str('')
        self.event_tm = datetime.datetime.strptime(self, '%Y-%m-%d %H:%M:%S.%f')
        self.event_seq_nb = int(0)
        self.exchange = str('')
        self.trade_pr = Decimal(0.0)
        self.trade_size = int(0)
        self.bid_pr = Decimal(0.0)
        self.bid_size = int(0)
        self.ask_pr = Decimal(0)
        self.ask_size = int(0)
        self.partition = str('')
        self.bad_rec = str(line)

    def __init__(self, trade_dt, arrival_tm, rec_type, symbol, event_tm, event_seq_nb, exchange,
                 trade_pr, trade_size, bid_pr, bid_size, ask_pr, ask_size, partition, bad_rec):
        
        self.trade_dt = trade_dt
        self.arrival_tm = arrival_tm
        self.rec_type = rec_type
        self.symbol = symbol
        self.event_tm = event_tm
        self.event_seq_nb = event_seq_nb
        self.exchange = exchange
        self.trade_pr = trade_pr
        self.trade_size = trade_size
        self.bid_pr = bid_pr
        self.bid_size = bid_size
        self.ask_pr = ask_pr
        self.ask_size = ask_size
        self.partition = partition
        self.bad_rec = bad_rec

