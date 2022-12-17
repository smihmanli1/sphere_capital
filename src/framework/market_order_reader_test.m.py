import sys

from coinbase_md_reader import CoinbaseMdReader
from market_order_aggregator import MarketOrderAggregator
from printer_pipe import PrinterPipe
from pipeline import Pipeline


snapshotFile = sys.argv[1]
eventsFile = sys.argv[2]

marketOrderReader = CoinbaseMdReader(snapshotFile,eventsFile)
marketOrderAggregator = MarketOrderAggregator(sell=True, onlyMarketOrder=True)
printerPipe = PrinterPipe()


p = Pipeline([marketOrderReader,marketOrderAggregator,printerPipe])
p.start()