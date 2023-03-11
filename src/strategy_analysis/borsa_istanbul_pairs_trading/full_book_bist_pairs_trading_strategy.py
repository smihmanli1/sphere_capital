
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

        self.krdmaBestBid = None
        self.krdmaBestAsk = None

        self.krdmdBestBid = None
        self.krdmdBestAsk = None

        #Conds
        self.triggerIntervalSeconds = parameters["trigger_interval_seconds"] #TODO: See if this is necessary

        self.maxDiff = -1000000000
        self.minSubtractedPart = 1000000000

        self.currentProfit = 0
        self.openAmount = 0
        self.orderId = 1

        self.upperLimitOnOpenAmount = 100000
        
    
    def updateBests(self):
        self.krdmaBestBid = self.lob.bestBid("KRDMA.E").price
        self.krdmaBestAsk = self.lob.bestAsk("KRDMA.E").price
        self.krdmdBestBid = self.lob.bestBid("KRDMD.E").price
        self.krdmdBestAsk = self.lob.bestAsk("KRDMD.E").price

    def openPosition(self):
        #Short KRDMD, Long KRDMA
        krdmaBestAskSize = self.lob.bestAsk("KRDMA.E").size
        krdmdBestBidSize = self.lob.bestBid("KRDMD.E").size

        print ("Opening position....")
        self.lob.dumpBook("KRDMA.E", 5)
        print()
        self.lob.dumpBook("KRDMD.E", 5)

        print (f"KRDMA: best ask: {self.krdmaBestAsk}, best ask size: {krdmaBestAskSize}, KRDMD: best bid: {self.krdmdBestBid}, best bid size: {krdmdBestBidSize}")
        tradedAmount = min(krdmaBestAskSize, krdmdBestBidSize)
        print (f"Traded amount: {tradedAmount}")

        if tradedAmount + self.openAmount > self.upperLimitOnOpenAmount:
            tradedAmount = self.upperLimitOnOpenAmount - self.openAmount
            print (f"Downsized traded amount to not go over upper limit amount: {tradedAmount}")

        #This should never happen because we never call openPosition if our open position
        #is already at the limit
        if tradedAmount == 0:
            return

        self.sendBuyOrder("KRDMA.E", tradedAmount)
        self.sendSellOrder("KRDMD.E", tradedAmount)
        self.openAmount += tradedAmount

        print(f"Current open amount in one share: {self.openAmount}")

        self.currentProfit += tradedAmount * (self.krdmdBestBid - self.krdmaBestAsk)

        self.updateBests()

    def closePosition(self):
        
        #Long KRDMD, Short KRDMA
        krdmaBestBidSize = self.lob.bestBid("KRDMA.E").size
        krdmdBestAskSize = self.lob.bestAsk("KRDMD.E").size

        print ("Closing position....")
        self.lob.dumpBook("KRDMA.E", 5)
        print()
        self.lob.dumpBook("KRDMD.E", 5)

        print (f"KRDMA: best bid: {self.krdmaBestBid}, best bid size: {krdmaBestBidSize}, KRDMD: best ask: {self.krdmdBestAsk}, best ask size: {krdmdBestAskSize}")
        tradedAmount = min(krdmaBestBidSize, krdmdBestAskSize)
        print (f"Traded amount: {tradedAmount}")

        if tradedAmount > self.openAmount:
            tradedAmount = self.openAmount
            print (f"Downsized traded amount to not go below 0: {tradedAmount}")

        #This should never happen because we never call closePosition if our open position
        #is already 0
        if tradedAmount == 0:
            return

        #Long KRDMD, Short KRDMA
        self.sendSellOrder("KRDMA.E", tradedAmount)
        self.sendBuyOrder("KRDMD.E", tradedAmount)
        profitFromThis = tradedAmount * (self.krdmaBestBid - self.krdmdBestAsk)
        print (f"Profit from this close: {profitFromThis}")
        self.currentProfit += profitFromThis
        self.openAmount -= tradedAmount

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
        subtracatedPart = self.krdmdBestAsk - self.krdmaBestBid
        if eventTime.time() < datetime.time(10,20,0):
            newSubtractedPart = min(self.minSubtractedPart, subtracatedPart)
            if newSubtractedPart != self.minSubtractedPart:
                print (f"New min subtracted part: {newSubtractedPart}")
            self.minSubtractedPart = newSubtractedPart
            return

        # Trigger only every set amount of seconds
        # secondsSinceLastTrigger = (eventTime - self.lastTriggerTime).total_seconds()
        # if secondsSinceLastTrigger < self.triggerIntervalSeconds:
        #     return
        # self.lastTriggerTime = eventTime 
        
        if self.minSubtractedPart == 1000000000:
            print ("Critical: minSubtractedPart is never set")
            exit(1)

       
        #ORIGINAL FOERMULA FOR CURRENT DIFF:
        # self.krdmdBestBid - self.krdmaBestAsk + self.krdmaFirstBid - self.krdmdFirstAsk
        currentDiff = self.krdmdBestBid - self.krdmaBestAsk - self.minSubtractedPart
        self.maxDiff = max(self.maxDiff, currentDiff)
        #TODO: Try different thrsholds for different pairs
        while currentDiff > 0.05 and self.openAmount < self.upperLimitOnOpenAmount:
            self.openPosition() #This call updates the bests
            print (f"Opened position with currentDiff: {currentDiff}")
            currentDiff = self.krdmdBestBid - self.krdmaBestAsk - self.minSubtractedPart
            print (f"Current open positions in each of KRDMD and KRDMA: {self.openAmount}")
        
        while subtracatedPart <= self.minSubtractedPart and self.openAmount > 0:
            self.closePosition() #This call updates the bests
            print (f"Closed position. Total profit: {self.currentProfit}")
            print (f"Current open positions in each of KRDMD and KRDMA: {self.openAmount}")
            subtracatedPart = self.krdmdBestAsk - self.krdmaBestBid
         
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
            self.lob.dumpBook("KRDMA.E", 5)
            print()
            self.lob.dumpBook("KRDMD.E", 5)
            print (f"Current open position in each stock: {self.openAmount}")
            print (f"Current total profit: {self.currentProfit}")


        currentTime = eventTime.time()
        if currentTime < self.parameters["exchange_open_time"]:
            return
        
        if currentTime > self.parameters["exchange_close_time"]:
            if self.openAmount > 0:
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
        #TODO: We should really look at both spreads
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
