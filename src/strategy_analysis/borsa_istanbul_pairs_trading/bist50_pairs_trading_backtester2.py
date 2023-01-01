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

from bist_pairs_trading_strategy import BistPairsTradingStrategy


def getLimitOrderFileName(dateInDatetime):
    return f"equities_{dateInDatetime.year}{dateInDatetime.month:02}{dateInDatetime.day:02}.csv"


def runBist50PairsTrading(limitOrderFilesDir, date, params):
    limitOrderFileName = os.path.join(limitOrderFilesDir,getLimitOrderFileName(date))
    print (f"Running algo for limit order file: {limitOrderFileName}")
    limitOrderBook = L3LimitOrderMultiBook()
    mdReader = BistOrderReader(limitOrderFileName, limitOrderBook)
    tradingAlgo = BistPairsTradingStrategy(limitOrderBook, params)
    backtesterPipeline = Pipeline([mdReader,[limitOrderBook,tradingAlgo]])
    
    backtesterPipeline.start()
    
    return tradingAlgo.getPricesDataFrame(), tradingAlgo.getPercentChangesDataFrame()


if len(sys.argv) < 3:
    print ("Usage: bist50_pairs_trading_backtester.py <limitOrderFilesDir> <run date Y-m-d>")
    exit(1)


limitOrderFilesDir = sys.argv[1]
runDateString = sys.argv[2]
runDate = datetime.datetime.strptime(runDateString, '%Y-%m-%d')

parameters = {}
parameters["exchange_open_time"] = datetime.time(10,0,0)
parameters["exchange_close_time"] = datetime.time(18,0,0)

print (f"Running strategy for day: {runDate}")    
allPricesDataframe,allPriceChangesDataframe = runBist50PairsTrading(limitOrderFilesDir, runDate, parameters)

print ("Prices: ")
print (allPricesDataframe)

dataFrameCsvFile = f"{runDateString}_interval_prices.csv"
allPricesDataframe.to_csv(dataFrameCsvFile)









