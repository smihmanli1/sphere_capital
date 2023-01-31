
from collections import defaultdict
import datetime
import pandas as pd

from framework.pipe import Pipe
from framework.trading_algo import TradingAlgo

MILLIS_IN_A_SEC = 1000

class FullBookBistPairsTradingStrategyJustMid(TradingAlgo):

    #Find type for 'parameters' and 'tickerPairs' in bist50_pairs_trading_backtester
    def __init__(self, lob, parameters):
        self.lob = lob
        self.eventCount = 0
        self.parameters = parameters
        
        self.krdmaFirstMid = None
        self.krdmdFirstMid = None

        self.lastTriggerTime = datetime.datetime(1970,1,1)
        
        self.krdmaPercentChange = None
        self.krdmdPercentChange = None

        #Const
        self.threshold = parameters["threshold"]/100
        self.triggerIntervalSeconds = parameters["trigger_interval_seconds"] 

        self.totalReturn = 1

        self.bought = False
        
    
    def checkPriceAndTrigger(self, event, eventTime):
        
        #Trigger only every set amount of seconds
        secondsSinceLastTrigger = (eventTime - self.lastTriggerTime).total_seconds()
        if secondsSinceLastTrigger < self.triggerIntervalSeconds:
            return

        # if self.krdmaAskPercentChange is None or self.krdmdBidPercentChange is None:
        #     return

        if self.krdmaFirstMid is None or\
           self.krdmdFirstMid is None:
           return

        self.lastTriggerTime = eventTime

        priceChangeDiff = abs(self.krdmdPercentChange - self.krdmaPercentChange)
        if priceChangeDiff >= self.threshold:
            self.bought = True
            self.boughtPrice = priceChangeDiff

        if self.bought and priceChangeDiff <= self.threshold/5:
            self.bought = False
            thisReturn = 1 + (self.boughtPrice - priceChangeDiff)
            self.totalReturn *= thisReturn

            

    def processKrdma(self, event, eventTime):
        if self.krdmaFirstMid is None:
            self.krdmaFirstMid = self.lob.mid(event.symbol)
    
        self.krdmaPercentChange = (self.lob.mid(event.symbol) - self.krdmaFirstMid) / self.krdmaFirstMid
        self.checkPriceAndTrigger(event, eventTime)
        
    def processKrdmd(self, event, eventTime):
        if self.krdmdFirstMid is None:
            self.krdmdFirstMid = self.lob.mid(event.symbol)
    
        self.krdmdPercentChange = (self.lob.mid(event.symbol) - self.krdmdFirstMid) / self.krdmdFirstMid
        self.checkPriceAndTrigger(event, eventTime)
        
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
            if self.bought:
                priceChangeDiff = abs(self.krdmdPercentChange - self.krdmaPercentChange)
                thisReturn = 1 + (self.boughtPrice - priceChangeDiff)
                self.totalReturn *= thisReturn
                self.bought = False
            return

        # if currentTime > datetime.time(10,20,0):
        #     raise Exception()

        if not self.lob.validBook(event.symbol):
            return

        if not self.lob.hasBid(event.symbol) or not self.lob.hasAsk(event.symbol):
            return

        if event.session != 'P_SUREKLI_ISLEM':
            # print (f"Not in continuous trading. Symbol: {event.symbol} Session: {event.session}")
            return
        
        # Only consider price if the spread is less than 10 kurus
        if self.lob.spread(event.symbol) <= 0.10:
            pass
        
        if event.symbol == "KRDMA.E":
            self.processKrdma(event, eventTime)
        elif event.symbol == "KRDMD.E":
            self.processKrdmd(event, eventTime)



    def orderAckedCallback(self,operation,orderId):
        pass

    def orderRejectedCallback(self,orderId,side):
        pass

    def notifyFilledCallback(self, matchedMarketOrder, algoOrder):
        # When we get a fill, we should run the above algo again.
        pass
