
from collections import defaultdict
import datetime
import pandas as pd
import numpy as np
import math

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
        self.maxDiff = -1000000000
        self.anchorSubtractedPart = None
        self.prevSubtractedPart = None

        self.currentProfit = 0
        self.openAmount = 0
        self.orderId = 1

        self.upperLimitOnOpenAmount = self.parameters["upper_limit_on_position"]

        self.allSubtractedPartsSoFar = []
        self.prevSubtractedPart = 0
        
    
    def updateBests(self): 
        self.krdmaBestBid = self.lob.bestBid("KRDMA.E")
        self.krdmaBestAsk = self.lob.bestAsk("KRDMA.E")
        self.krdmdBestBid = self.lob.bestBid("KRDMD.E")
        self.krdmdBestAsk = self.lob.bestAsk("KRDMD.E")

    def openPosition(self):
        #Short KRDMD, Long KRDMA
        
        if self.krdmaBestAsk is None or self.krdmdBestBid is None:
            print ("CRITICAL: Book doesn't have liquidity when opening positions")
            exit(1)

        print ("Opening position....")
        self.lob.dumpBook("KRDMA.E", 5)
        print()
        self.lob.dumpBook("KRDMD.E", 5)

        print (f"KRDMA: best ask: {self.krdmaBestAsk.price}, best ask size: {self.krdmaBestAsk.size}, KRDMD: best bid: {self.krdmdBestBid.price}, best bid size: {self.krdmdBestBid.size}")
        tradedAmount = min(self.krdmaBestAsk.size, self.krdmdBestBid.size)
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

        self.currentProfit += tradedAmount * (self.krdmdBestBid.price - self.krdmaBestAsk.price)

        self.updateBests()

    def closePosition(self):
        
        #Long KRDMD, Short KRDMA
        if self.krdmaBestBid is None or self.krdmdBestAsk is None:
            #Don't expect this to happen because we close position in two places:
            # 1. Intraday when prices move together. Don't expect to exhaust liquidity there.
            # 2. End of day when unwinding all positions. In this case we call if bests are present before calling
            #    this function.
            print ("CRITICAL: Book doesn't have liquidity when opening positions")
            exit(1)

        print ("Closing position....")
        self.lob.dumpBook("KRDMA.E", 5)
        print()
        self.lob.dumpBook("KRDMD.E", 5)

        print (f"KRDMA: best bid: {self.krdmaBestBid.price}, best bid size: {self.krdmaBestBid.size}, KRDMD: best ask: {self.krdmdBestAsk.price}, best ask size: {self.krdmdBestAsk.size}")
        tradedAmount = min(self.krdmaBestBid.size, self.krdmdBestAsk.size)
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
        profitFromThis = tradedAmount * (self.krdmaBestBid.price - self.krdmdBestAsk.price)
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


    def bestsPresent(self):
        return self.krdmdBestBid is not None and\
           self.krdmdBestAsk is not None and\
           self.krdmaBestBid is not None and\
           self.krdmaBestAsk is not None

    def checkPriceAndTrigger(self, event, eventTime):
        
        if not self.bestsPresent():
           return

        # Note that if one of the below prices changed, subtraced part would have changed.
        # Also not that both can't change on the same checkPriceAndTrigger call
        subtracatedPart = self.krdmdBestAsk.price - self.krdmaBestBid.price
        if subtracatedPart != self.prevSubtractedPart:
            self.prevSubtractedPart = subtracatedPart
            self.allSubtractedPartsSoFar.append(subtracatedPart)
            if self.openAmount == 0:
                potentialNewAnchor = np.median(self.allSubtractedPartsSoFar)
                if self.anchorSubtractedPart != potentialNewAnchor:
                    print (f"Anchor subtracted part updated to: {potentialNewAnchor}")
                self.anchorSubtractedPart = potentialNewAnchor
        
        if eventTime.time() < datetime.time(11,0,0):
            return

        # It's time to unwind if we have any open positions
        if eventTime.time() > self.parameters["algo_end_time"]:
            while self.openAmount > 0 and self.bestsPresent():
                self.closePosition()
                print (f"ALGO END Closed position. Total profit: {self.currentProfit}")
                print (f"ALGO END Current open positions in each of KRDMD and KRDMA: {self.openAmount}")
            return

        #ORIGINAL FOERMULA FOR CURRENT DIFF:
        # self.krdmdBestBid - self.krdmaBestAsk + self.krdmaFirstBid - self.krdmdFirstAsk
        currentDiff = self.krdmdBestBid.price - self.krdmaBestAsk.price - self.anchorSubtractedPart
        self.maxDiff = max(self.maxDiff, currentDiff)
        #TODO: Try different thrsholds for different pairs
        while currentDiff > self.parameters["buy_threshold"] and self.openAmount < self.upperLimitOnOpenAmount:
            self.openPosition() #This call updates the bests
            print (f"Opened position with currentDiff: {currentDiff}")
            currentDiff = self.krdmdBestBid.price - self.krdmaBestAsk.price - self.anchorSubtractedPart
            print (f"Current open positions in each of KRDMD and KRDMA: {self.openAmount}")
        
        while subtracatedPart <= self.anchorSubtractedPart - self.parameters["sell_threshold"] and self.openAmount > 0:
            self.closePosition() #This call updates the bests
            print (f"Closed position. Total profit: {self.currentProfit}")
            print (f"Current open positions in each of KRDMD and KRDMA: {self.openAmount}")
            # Not expecting to run out of liquidity here. If we do, we will crash on the line
            # below.
            subtracatedPart = self.krdmdBestAsk.price - self.krdmaBestBid.price 
         
    def processKrdma(self, event, eventTime):
        self.updateBests()
        self.checkPriceAndTrigger(event, eventTime)
        
    def processKrdmd(self, event, eventTime):
        self.updateBests()
        self.checkPriceAndTrigger(event, eventTime)
        
    def profitFromUnwindingAmount(self, amount):
            
        #Unwind KRDMA by shorting it
        profitFromKrdma, krdmaRemainingToUnwind = self.lob.cumulativeValueAtBidNumShares("KRDMA.E", amount)

        #Unwind KRDMD by longing it
        profitFromKrdmd, krdmdRemainingToUnwind = self.lob.cumulativeValueAtAskNumShares("KRDMD.E", amount)
        profitFromKrdmd = -profitFromKrdmd #Because we are longing KRDMD
        
        return profitFromKrdma + profitFromKrdmd, krdmaRemainingToUnwind, krdmdRemainingToUnwind

    def cumulativeValueAtAskNumShares(self, ticker, numShares):
        return self.limitOrderBooks[ticker].cumulativeValueAtNumShares(numShares, Side.ASK)


    def logProfitOfHypotheticalUnwind(self, openAmountRatio):
        percentage = 100 * openAmountRatio
        amount = math.floor(openAmountRatio * self.openAmount)
        profitFromUnwinding, krdmaRemainingToUnwind, krdmdRemainingToUnwind  = self.profitFromUnwindingAmount(amount)
        if krdmaRemainingToUnwind == 0 and krdmdRemainingToUnwind == 0:
            print (f"Today's profit if unwinded {percentage}% of everything now: {self.currentProfit + profitFromUnwinding}")
        else:
            print (f"WARNING: Cannot unwind {percentage}% of everything now. KRDMA remaining: {krdmaRemainingToUnwind}, KRDMD remaining: {krdmdRemainingToUnwind}")

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
            if self.openAmount > 0:
                for openAmountRatio in [0.25, 0.50, 0.75, 1.0]:
                    self.logProfitOfHypotheticalUnwind(openAmountRatio)
            if self.bestsPresent():
                subtracatedPart = self.krdmdBestAsk.price - self.krdmaBestBid.price
                print (f"Subtracted part now: {subtracatedPart}")
            print ()
            print ()
            print ()


        currentTime = eventTime.time()
        if currentTime < self.parameters["exchange_open_time"]:
            return
        
        if currentTime > self.parameters["exchange_close_time"]:
            return

        if currentTime < self.parameters["algo_start_time"]:
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
        # if self.lob.spread(event.symbol) <= 0.10:
        #     pass
        
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
