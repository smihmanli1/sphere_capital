
import sys

from market_data_replayer import MarketDataReplayer
from limit_order_reader2 import LimitOrderReader2
from market_order_reader import MarketOrderReader
from market_order_aggregator import MarketOrderAggregator
from printer_pipe import PrinterPipe
from pipeline import Pipeline

limitOrderFileName = sys.argv[1]
marketOrderFileName = sys.argv[2]


limitOrderReader = LimitOrderReader2(limitOrderFileName)
marketOrderReader = MarketOrderReader(marketOrderFileName)

replayer = MarketDataReplayer(limitOrderReader,marketOrderReader)

limitOrderReaderToReplayerPipeline = Pipeline([limitOrderReader,replayer])

marketOrderAggregator = MarketOrderAggregator()
marketOrderReaderToReplayerPipeline = Pipeline([marketOrderReader,marketOrderAggregator,replayer])


printerPipe = PrinterPipe()
backtesterPipeline = Pipeline([replayer,printerPipe])

backtesterPipeline.start()