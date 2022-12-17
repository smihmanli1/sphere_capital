
import random

import sys
import os
import datetime

from bist_order_reader import BistOrderReader
from market_order_aggregator import MarketOrderAggregator
from l3_limit_order_book import L3LimitOrderMultiBook
from backtest_order_submitter import BacktestOrderSubmitter
from match_engine_simulator import MatchEngineSimulator
from market_order import MarketOrder
from printer_pipe import PrinterPipe
from pipeline import Pipeline
from pipe import Pipe
from trading_algo import TradingAlgo
from limit_order import LimitOrder, Side

from distribution import MarketOrderSizeDistribution
from bist_pairs_trading_algo import BistPairsTradingAlgo
from market_order_rate_direction_prediction_test_algo import MarketOrderRatePriceDirectionAlgo
from collections import defaultdict

class PairsTradingPair:

    def __init__(self, ticker1, ticker2, ticker1StartPrice, ticker2StartPrice, percentChangeDiffMean, percentChangeDiffStdDev):
        self.ticker1 = ticker1
        self.ticker2 = ticker2
        self.ticker1StartPrice = ticker1StartPrice
        self.ticker2StartPrice = ticker2StartPrice
        self.percentChangeDiffStdDev = percentChangeDiffStdDev
        self.percentChangeDiffMean = percentChangeDiffMean


def getLimitOrderFileName(dateInDatetime):
    return f"equities_{dateInDatetime.year}{dateInDatetime.month:02}{dateInDatetime.day:02}.csv"

def parseIndicatorCalculationFile(indicatorCalculationFile):
    result = defaultdict(list)
    # 2021-1-11,ARCLK.E,DOHOL.E,0.01993118231272816,30.43,3.105
    with open(indicatorCalculationFile) as f:
        for line in f:
            lineSplit = line.split(',')
            dateInDatetime = datetime.datetime.strptime(lineSplit[0], '%Y-%m-%d')
            newPair = PairsTradingPair(
                lineSplit[1],
                lineSplit[2],
                float(lineSplit[5]),
                float(lineSplit[6]),
                float(lineSplit[3]),
                float(lineSplit[4]))
            
            result[dateInDatetime].append(newPair)

    return result


def runBist50PairsTrading(limitOrderFilesDir, date, params, pairs):
    
    limitOrderFileName = os.path.join(limitOrderFilesDir,getLimitOrderFileName(date))
    print (f"Running algo for limit order file: {limitOrderFileName}")

    limitOrderBook = L3LimitOrderMultiBook()
    mdReader = BistOrderReader(limitOrderFileName, limitOrderBook)
    backtestOrderSubmitter = BacktestOrderSubmitter(lob = limitOrderBook)

    #TODO: !!!Pass all the pairs not just one
    # usedPairs = []
    # for pair in pairs:
    #     if (pair.ticker1 == "ARCLK.E" and pair.ticker2 == "KCHOL.E") or \
    #        (pair.ticker2 == "KCHOL.E" and pair.ticker1 == "ARCLK.E"):
    #        usedPairs.append(pair)
    # tradingAlgo = BistPairsTradingAlgo(backtestOrderSubmitter, limitOrderBook, params, usedPairs)
    tradingAlgo = BistPairsTradingAlgo(backtestOrderSubmitter, limitOrderBook, params, pairs)

    matchEngineSimulator = MatchEngineSimulator(tradingAlgo, limitOrderBook)

    #TODO: Two issues with delay here:
    # 1) Algo gets the events immediately. It currently sees the exchange lob and gets the events immediately.
    #    Algo would normally have its own lob and would get the data delayed. In other words, we need incoming delay.
    # 2) In addition algo would see the conirmation of its own order delayed. Currently, effectively algo's order is inserted
    #    as soon as the outgoing delay is surpassed.


    backtesterPipeline = Pipeline([mdReader,[backtestOrderSubmitter,limitOrderBook,matchEngineSimulator,tradingAlgo]])
                                                    
    backtesterPipeline.start()


if len(sys.argv) < 5:
    print ("Usage: bist50_pairs_trading_backtester.py <limitOrderFilesDir> <indicatorCalculationFile> <start date Y-m-d> <end date Y-m-d>")
    exit(1)


limitOrderFilesDir = sys.argv[1]
indicatorCalculationFile = sys.argv[2]
startDate = datetime.datetime.strptime(sys.argv[3], '%Y-%m-%d')
endDate = datetime.datetime.strptime(sys.argv[4], '%Y-%m-%d')

parameters = {}
parameters["trading_upper_buy_threshold"] = 1.0
parameters["trading_lower_buy_threshold"] = 0.0
parameters["trading_buy_threshold"] = 1.0
parameters["trading_sell_threshold"] = 0.0
parameters["exchange_fee_rate"] = 0.000025
parameters["exchange_open_time"] = datetime.time(10,0,0)
parameters["exchange_close_time"] = datetime.time(18,0,0)
parameters["close_all_positions_mins_left"] = 10


allPairs = parseIndicatorCalculationFile(indicatorCalculationFile)

currentDay = startDate
while currentDay != endDate:
    print (f"Running strategy for day: {currentDay}")
    if currentDay in allPairs:
        try:
            runBist50PairsTrading(limitOrderFilesDir, currentDay, parameters, allPairs[currentDay])
        except Exception as e:
            print (f"{currentDay} backtest failed with exception: {str(e)}")

    
    currentDay += datetime.timedelta(days=1)













