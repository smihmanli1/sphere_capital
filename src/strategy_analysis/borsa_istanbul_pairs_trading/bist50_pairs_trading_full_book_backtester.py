from pathlib import Path
import sys
path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))
print(sys.path)

import sys
import os
import datetime
from collections import defaultdict
import pandas as pd
import json

from borsa_istanbul_handlers.bist_order_reader import BistOrderReader
from framework.l3_limit_order_book import L3LimitOrderMultiBook
from framework.pipeline import Pipeline

from full_book_bist_pairs_trading_strategy import FullBookBistPairsTradingStrategy
from full_book_bist_pairs_trading_strategy_just_mid import FullBookBistPairsTradingStrategyJustMid


def getLimitOrderFileName(dateInDatetime):
    return f"equities_{dateInDatetime.year}{dateInDatetime.month:02}{dateInDatetime.day:02}.csv"


def runBist50PairsTrading(limitOrderFilesDir, date, params):
    limitOrderFileName = os.path.join(limitOrderFilesDir,getLimitOrderFileName(date))
    print (f"Running algo for limit order file: {limitOrderFileName}")
    limitOrderBook = L3LimitOrderMultiBook()
    mdReader = BistOrderReader(limitOrderFileName, limitOrderBook)
    tradingAlgo = FullBookBistPairsTradingStrategy(limitOrderBook, params)
    # tradingAlgo = FullBookBistPairsTradingStrategyJustMid(limitOrderBook, params)
    backtesterPipeline = Pipeline([mdReader,[limitOrderBook,tradingAlgo]])
    
    # try:
    backtesterPipeline.start()
    # except:
    #     pass

    return tradingAlgo.maxDiff, tradingAlgo.currentProfit, tradingAlgo.openAmount
    

if len(sys.argv) < 3:
    print ("Usage: bist50_pairs_trading_backtester.py <limitOrderFilesDir> <run date Y-m-d>")
    exit(1)


limitOrderFilesDir = sys.argv[1]
startDateString = sys.argv[2]
endDateString = sys.argv[3]
startDate = datetime.datetime.strptime(startDateString, '%Y-%m-%d')
endDate = datetime.datetime.strptime(endDateString, '%Y-%m-%d')

parameters = {}
parameters["exchange_open_time"] = datetime.time(10,0,0)
parameters["algo_start_time"] = datetime.time(10,5,0)
parameters["algo_end_time"] = datetime.time(17,58,0) #Giving it two minutes to unwind.
parameters["exchange_close_time"] = datetime.time(18,0,0)

parameters["buy_threshold"] = 0.05
parameters["sell_threshold"] = 0
parameters["upper_limit_on_position"] = 100000

if len(sys.argv) > 4:
    parameters["buy_threshold"] = sys.argv[4]
    parameters["sell_threshold"] = sys.argv[5]
    parameters["upper_limit_on_position"] = sys.argv[6]


runDate = startDate

while runDate != endDate:
    print (f"Running strategy for day: {runDate}")    
    try:
        maxDiff, totalProfit, openAmount = runBist50PairsTrading(limitOrderFilesDir, runDate, parameters)
        print (f"Max diff for {runDate}: {maxDiff}")
        print (f"Total profit for {runDate}: {totalProfit}")
        print (f"Open amount in each share: {runDate}: {openAmount}")

    except FileNotFoundError:
        print (f"No trading on {runDate}")
    runDate += datetime.timedelta(days=1)











