
import random

import sys

from bist_order_reader import BistOrderReader
from market_order_aggregator import MarketOrderAggregator
from l3_limit_order_book import L3LimitOrderBook
from backtest_order_submitter import BacktestOrderSubmitter
from match_engine_simulator import MatchEngineSimulator
from market_order import MarketOrder
from printer_pipe import PrinterPipe
from pipeline import Pipeline
from pipe import Pipe
from trading_algo import TradingAlgo
from limit_order import LimitOrder, Side

from distribution import MarketOrderSizeDistribution
from dummy_algo import DummyTradingAlgo
from market_order_rate_direction_prediction_test_algo import MarketOrderRatePriceDirectionAlgo

limitOrderFileName = sys.argv[1]

mdReader = BistOrderReader(limitOrderFileName)
limitOrderBook = L3LimitOrderBook()
backtestOrderSubmitter = BacktestOrderSubmitter(lob = limitOrderBook)
tradingAlgo = DummyTradingAlgo(backtestOrderSubmitter, limitOrderBook)

matchEngineSimulator = MatchEngineSimulator(tradingAlgo, limitOrderBook)



#TODO: Two issues with delay here:
# 1) Algo gets the events immediately. It currently sees the exchange lob and gets the events immediately.
#    Algo would normally have its own lob and would get the data delayed. In other words, we need incoming delay.
# 2) In addition algo would see the conirmation of its own order delayed. Currently, effectively algo's order is inserted
#    as soon as the outgoing delay is surpassed.


backtesterPipeline = Pipeline([mdReader,[backtestOrderSubmitter,limitOrderBook,matchEngineSimulator,tradingAlgo]])
                                                
backtesterPipeline.start()





