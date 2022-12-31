
from collections import defaultdict
import datetime
import pandas as pd

from framework.pipe import Pipe
from framework.trading_algo import TradingAlgo

MILLIS_IN_A_SEC = 1000

class BistPairsTradingStrategy(TradingAlgo):

    #Find type for 'parameters' and 'tickerPairs' in bist50_pairs_trading_backtester
    def __init__(self, lob, parameters):
        self.lob = lob
        self.eventCount = 0
        self.parameters = parameters

        self.prices = defaultdict(list)


    def mergeAllPrices(self, prices):
        
        for ticker,pricesTimes in prices.items():
            tradingDateTime = pricesTimes[0][0]
            break

        startTime = tradingDateTime.replace(hour=self.parameters["exchange_open_time"].hour,minute=self.parameters["exchange_open_time"].minute,second=self.parameters["exchange_open_time"].second, microsecond=0)
        endTime = tradingDateTime.replace(hour=self.parameters["exchange_close_time"].hour,minute=self.parameters["exchange_close_time"].minute,second=self.parameters["exchange_close_time"].second,  microsecond=0)

        currentTime = startTime
        timeseries = []
        while currentTime <= endTime:
            timeseries.append(currentTime)
            currentTime = currentTime + datetime.timedelta(seconds=15)

        timeseries = pd.DataFrame({'time' : timeseries})
        for ticker,prices in prices.items():
            tickerPriceChange = pd.DataFrame({'time': [elem[0] for elem in prices], ticker: [elem[1] for elem in prices]})
            timeseries = pd.merge_asof(timeseries, tickerPriceChange, on='time')
            

        return timeseries
            
    def getPricesDataFrame(self):
        return self.mergeAllPrices(self.prices)

    def getPercentChangesDataFrame(self):
        percentChanges = {}

        for ticker,prices in self.prices.items():
            newArray = []
            startingPrice = prices[0][1]
            for timestamp,price in prices:
                newArray.append( (timestamp, (price - startingPrice) / startingPrice) )
            percentChanges[ticker] = newArray

        
        allPrices = self.mergeAllPrices(percentChanges)

        return allPrices
        

    def accept(self, event):
        
        #TODO: We should not trade if our best bid/ask does not match their best bid ask. (For this 
        # we should keep a copy of the book that is not manipulated by our orders)
        
        #This is where the algo takes action
        eventTime = datetime.datetime.utcfromtimestamp(event.timestamp / MILLIS_IN_A_SEC)
        self.eventCount += 1
        if self.eventCount % 100000 == 0:
            print (f"Processed {self.eventCount} events")
            print (f"Time: {eventTime}")


        currentTime = eventTime.time()
        if currentTime < self.parameters["exchange_open_time"]:
            return
        
        if currentTime > self.parameters["exchange_close_time"]:
            return

        # if currentTime > datetime.time(10,15,0):
        #     raise Exception()

        if not self.lob.validBook(event.symbol):
            return

        if not self.lob.hasBid(event.symbol) or not self.lob.hasAsk(event.symbol):
            return

        if event.session != 'P_SUREKLI_ISLEM':
            # print (f"Not in continuous trading. Symbol: {event.symbol} Session: {event.session}")
            return
        
        self.prices[event.symbol].append((eventTime,self.lob.mid(event.symbol)))
        

    def orderAckedCallback(self,operation,orderId):
        pass

    def orderRejectedCallback(self,orderId,side):
        pass

    def notifyFilledCallback(self, matchedMarketOrder, algoOrder):
        # When we get a fill, we should run the above algo again.
        pass
