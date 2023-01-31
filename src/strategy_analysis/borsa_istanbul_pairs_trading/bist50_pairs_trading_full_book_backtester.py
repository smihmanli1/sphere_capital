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
    # tradingAlgo = FullBookBistPairsTradingStrategy(limitOrderBook, params)
    tradingAlgo = FullBookBistPairsTradingStrategyJustMid(limitOrderBook, params)
    backtesterPipeline = Pipeline([mdReader,[limitOrderBook,tradingAlgo]])
    
    # try:
    backtesterPipeline.start()
    # except:
    #     pass

    return tradingAlgo.totalReturn
    

if len(sys.argv) < 3:
    print ("Usage: bist50_pairs_trading_backtester.py <limitOrderFilesDir> <run date Y-m-d>")
    exit(1)


limitOrderFilesDir = sys.argv[1]
startDateString = sys.argv[2]
endDateString = sys.argv[3]
startDate = datetime.datetime.strptime(startDateString, '%Y-%m-%d')
endDate = datetime.datetime.strptime(endDateString, '%Y-%m-%d')

parameters = {}
parameters["exchange_open_time"] = datetime.time(10,15,0)
parameters["exchange_close_time"] = datetime.time(17,45,0)
parameters["trigger_interval_seconds"] = 15
parameters["threshold"] = 0.2333543059856918/2
# parameters["threshold"] = 0

runDate = startDate
totalReturn = 1
while runDate != endDate:
    print (f"Running strategy for day: {runDate}")    
    try:
        thisDayReturn = runBist50PairsTrading(limitOrderFilesDir, runDate, parameters)
        print (f"Return for {runDate}: {thisDayReturn}")
        totalReturn *= thisDayReturn
    except FileNotFoundError:
        print (f"No trading on {runDate}")
    runDate += datetime.timedelta(days=1)

print (f"Total return is: {totalReturn}")









