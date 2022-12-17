
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
from bist_pairs_trading_algo2 import BistPairsTradingAlgo2
from bist_pairs_trading_strategy import BistPairsTradingStrategy
from collections import defaultdict

import pandas as pd

allEtfs = ['USDTR.F', 'ZPBDL.F', 'ZPX30.F', 'ZRE20.F', 'ZPT10.F', 'GLDTR.F', 'DJIST.F', 'Z30KE.F', 'GMSTR.F', 'Z30KP.F', 'ZTM15.F', 'Z30EA.F', 'ZPLIB.F', 'ZELOT.F', 'ZGOLD.F']
bist50Tickers = ["AKBNK.E",
                "AKSEN.E",
                "ALARK.E",
                "ALKIM.E",
                "ARCLK.E",
                "ASELS.E",
                "BERA.E",
                "BIMAS.E",
                "DOHOL.E",
                "EKGYO.E",
                "ENJSA.E",
                "ENKAI.E",
                "EREGL.E",
                "FROTO.E",
                "GARAN.E",
                "GLYHO.E",
                "GUBRF.E",
                "HALKB.E",
                "HEKTS.E",
                "ISCTR.E",
                "KARSN.E",
                "KCHOL.E",
                "KORDS.E",
                "KOZAA.E",
                "KOZAL.E",
                "KRDMD.E",
                "LOGO.E",
                "MGROS.E",
                "ODAS.E",
                "OTKAR.E",
                "PETKM.E",
                "PGSUS.E",
                "SAHOL.E",
                "SASA.E",
                "SISE.E",
                "SKBNK.E",
                "SOKM.E",
                "TAVHL.E",
                "TCELL.E",
                "THYAO.E",
                "TKFEN.E",
                "TOASO.E",
                "TSKB.E",
                "TTKOM.E",
                "TTRAK.E",
                "TUPRS.E",
                "TURSG.E",
                "VAKBN.E",
                "VESTL.E",
                "YKBNK.E"]

bist30Tickers = ["TCELL.E",
                "KOZAL.E",
                "VESTL.E",
                "AKSEN.E",
                "TTKOM.E",
                "PETKM.E",
                "SISE.E",
                "TUPRS.E",
                "TOASO.E",
                "KRDMD.E",
                "AKBNK.E",
                "TKFEN.E",
                "FROTO.E",
                "GARAN.E",
                "KOZAA.E",
                "YKBNK.E",
                "BIMAS.E",
                "ARCLK.E",
                "EREGL.E",
                "SASA.E",
                "SAHOL.E",
                "EKGYO.E",
                "THYAO.E",
                "HEKTS.E",
                "PGSUS.E",
                "ISCTR.E",
                "KCHOL.E",
                "ASELS.E",
                "TAVHL.E",
                "GUBRF.E"]


def getLimitOrderFileName(dateInDatetime):
    return f"equities_{dateInDatetime.year}{dateInDatetime.month:02}{dateInDatetime.day:02}.csv"


def runBist50PairsTrading(limitOrderFilesDir, date, params):
    
    limitOrderFileName = os.path.join(limitOrderFilesDir,getLimitOrderFileName(date))
    print (f"Running algo for limit order file: {limitOrderFileName}")

    limitOrderBook = L3LimitOrderMultiBook()
    mdReader = BistOrderReader(limitOrderFileName, limitOrderBook)
    backtestOrderSubmitter = BacktestOrderSubmitter(lob = limitOrderBook)

    
    tradingAlgo = BistPairsTradingStrategy(backtestOrderSubmitter, limitOrderBook, params)

    matchEngineSimulator = MatchEngineSimulator(tradingAlgo, limitOrderBook)

    #TODO: Two issues with delay here:
    # 1) Algo gets the events immediately. It currently sees the exchange lob and gets the events immediately.
    #    Algo would normally have its own lob and would get the data delayed. In other words, we need incoming delay.
    # 2) In addition algo would see the conirmation of its own order delayed. Currently, effectively algo's order is inserted
    #    as soon as the outgoing delay is surpassed.


    backtesterPipeline = Pipeline([mdReader,[backtestOrderSubmitter,limitOrderBook,matchEngineSimulator,tradingAlgo]])
             
    #TODO: Remove this try/catch                                       
    try:
        backtesterPipeline.start()
    except:
        pass

    return tradingAlgo.getPricesDataFrame(), tradingAlgo.getPercentChangesDataFrame()


def getTrajectory(tickers, df):

    sums = []
    returned = pd.DataFrame({"time" : df["time"]})
    trajectoryName = "_".join(tickers)
    for ticker in tickers:
        if trajectoryName not in returned:
            returned[trajectoryName] = df[ticker]
        else:
            returned[trajectoryName] += df[ticker]
    
    returned[trajectoryName] /= len(tickers)

    return trajectoryName,returned

def addBist50Column(tickers, pricesDf):
    trajectoryName, df = getTrajectory(tickers, pricesDf)
    pricesDf['BIST50'] = df[trajectoryName]
    return pricesDf

def addBist30Column(tickers, pricesDf):
    trajectoryName, df = getTrajectory(tickers, pricesDf)
    pricesDf['BIST30'] = df[trajectoryName]
    return pricesDf


def getCorrelations(pricesDataFrame, stocks, allEtfs):
    result = defaultdict(dict)
    for stock in stocks:
        for etf in allEtfs:
            if stock in pricesDataFrame and etf in pricesDataFrame:
                result[stock][etf] = pricesDataFrame[stock].corr(pricesDataFrame[etf])

    return result



if len(sys.argv) < 3:
    print ("Usage: bist50_pairs_trading_backtester.py <limitOrderFilesDir> <run date Y-m-d>")
    exit(1)


limitOrderFilesDir = sys.argv[1]
runDate = datetime.datetime.strptime(sys.argv[2], '%Y-%m-%d')

parameters = {}
parameters["exchange_open_time"] = datetime.time(10,0,0)
parameters["exchange_close_time"] = datetime.time(18,0,0)

print (f"Running strategy for day: {runDate}")    
allPricesDataframe,allPriceChangesDataframe = runBist50PairsTrading(limitOrderFilesDir, runDate, parameters)

pricesDataFrame = addBist30Column(bist30Tickers, allPricesDataframe)
pricesDataFrame = addBist50Column(bist50Tickers, allPricesDataframe)

print ("Prices: ")
print (allPricesDataframe)


indexEtfSet = ["BIST30","BIST50"] + allEtfs
dailyPriceCorrelations = getCorrelations(pricesDataFrame, indexEtfSet, allEtfs)

print ("Correlations: ")
for index in indexEtfSet:
    if index in dailyPriceCorrelations:
        print (index)
        correlationsList = list(dailyPriceCorrelations[index].items())
        correlationsList.sort(key=lambda x: x[1])
        print (f"{correlationsList}\n\n\n")











