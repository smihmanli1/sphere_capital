
from collections import defaultdict
import datetime
import pandas as pd

from framework.pipe import Pipe
from framework.trading_algo import TradingAlgo
from framework.market_order import MarketOrder

MILLIS_IN_A_SEC = 1000

class FullBookBistPairsTradingStrategy(TradingAlgo):

    #Find type for 'parameters' and 'tickerPairs' in bist50_pairs_trading_backtester
    def __init__(self, lob, parameters):
        self.lob = lob
        self.eventCount = 0
        self.parameters = parameters
        
        self.lastTriggerTime = datetime.datetime(1970,1,1)
        self.krdmaAskPercentChange = None
        self.krdmdBidPercentChange = None

        self.krdmaBestBid = None
        self.krdmaBestAsk = None

        self.krdmdBestBid = None
        self.krdmdBestAsk = None

        #Conds
        self.threshold = parameters["threshold"]/100
        self.triggerIntervalSeconds = parameters["trigger_interval_seconds"]

        self.maxDiff = -1000000000
        self.minSubtractedPart = 1000000000

        self.bought = False
        self.currentProfit = 0
        self.openAmount = 0
        self.orderId = 1
        
    
    def updateBests(self):
        self.krdmaBestBid = self.lob.bestBid("KRDMA.E").price
        self.krdmaBestAsk = self.lob.bestAsk("KRDMA.E").price
        self.krdmdBestBid = self.lob.bestBid("KRDMD.E").price
        self.krdmdBestAsk = self.lob.bestAsk("KRDMD.E").price

    def openPosition(self):
        #Short KRDMD, Long KRDMA
        krdmaBestAskSize = self.lob.bestAsk("KRDMA.E").size
        krdmdBestBidSize = self.lob.bestBid("KRDMD.E").size
        tradedAmount = min(krdmaBestAskSize, krdmdBestBidSize)
        self.sendBuyOrder("KRDMA.E", tradedAmount)
        self.sendSellOrder("KRDMD.E", tradedAmount)
        self.openAmount = tradedAmount
        self.bought = True
        self.currentProfit += tradedAmount * (self.krdmdBestBid - self.krdmaBestAsk)

        self.updateBests()

    def closePosition(self):
        #Long KRDMD, Short KRDMA
        self.sendSellOrder("KRDMA.E", self.openAmount)
        self.sendBuyOrder("KRDMD.E", self.openAmount)
        self.bought = False
        self.currentProfit += self.openAmount * (self.krdmaBestBid - self.krdmdBestAsk)

        self.updateBests()

    def sendBuyOrder(self, ticker, size):
        self.orderId += 1 
        marketOrder = MarketOrder(symbol=ticker, orderId=self.orderId, sell=False, size=size)
        self.lob.applyMarketOrder(marketOrder)

    def sendSellOrder(self, ticker, size):
        self.orderId += 1 
        marketOrder = MarketOrder(symbol=ticker, orderId=self.orderId, sell=True, size=size)
        self.lob.applyMarketOrder(marketOrder)

    def checkPriceAndTrigger(self, event, eventTime):
        

        if self.krdmdBestBid is None or\
           self.krdmdBestAsk is None or\
           self.krdmaBestBid is None or\
           self.krdmaBestAsk is None:
           return

        # Update the min subtracted part only if it is between 10:15am-10:20am. After 10:20am, we start actual trading.
        if eventTime.time() < datetime.time(10,20,0):
            self.minSubtractedPart = min(self.minSubtractedPart, self.krdmdBestAsk - self.krdmaBestBid)
            return

        # Trigger only every set amount of seconds
        # secondsSinceLastTrigger = (eventTime - self.lastTriggerTime).total_seconds()
        # if secondsSinceLastTrigger < self.triggerIntervalSeconds:
        #     return
        # self.lastTriggerTime = eventTime 
        
        if self.minSubtractedPart == 1000000000:
            print ("Critical: minSubtractedPart is never set")

       
        #ORIGINAL FOERMULA FOR CURRENT DIFF:
        # self.krdmdBestBid - self.krdmaBestAsk + self.krdmaFirstBid - self.krdmdFirstAsk
        currentDiff = self.krdmdBestBid - self.krdmaBestAsk - self.minSubtractedPart
        self.maxDiff = max(self.maxDiff, currentDiff)
        # print (f"Max diff so far: {self.maxDiff}")
        #TODO: Try different thrsholds for different pairs
        while currentDiff > 0.05:
            self.openPosition()
            print (f"Opened position with currentDiff: {currentDiff}")
            currentDiff = self.krdmdBestBid - self.krdmaBestAsk - self.minSubtractedPart

        if currentDiff <= 0 and self.bought:
            self.closePosition()
            print (f"Closed position with profit: {self.currentProfit}")
         
    def processKrdma(self, event, eventTime):
        self.krdmaBestBid = self.lob.bestBid(event.symbol).price
        self.krdmaBestAsk = self.lob.bestAsk(event.symbol).price
        self.checkPriceAndTrigger(event, eventTime)
        
    def processKrdmd(self, event, eventTime):
        self.krdmdBestBid = self.lob.bestBid(event.symbol).price
        self.krdmdBestAsk = self.lob.bestAsk(event.symbol).price
        self.checkPriceAndTrigger(event, eventTime)
        
    def accept(self, event):
        
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
                closePosition()
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
