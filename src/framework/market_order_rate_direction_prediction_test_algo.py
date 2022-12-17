#Predict price direction based on market order rate (buy/sell pressure based on market orders)

import math
import numpy as np

from trading_algo import TradingAlgo
from limit_order import Side
from distribution import Distribution, WindowedMarketOrderSizeDistribution
from window_maintainer import BackTestWindownMaintainer
from market_order_aggregator import MarketOrderAggregator
from pipeline import Pipeline
from market_order import MarketOrder


class MarketOrderRatePriceDirectionAlgo(TradingAlgo):

    def __init__(
        self, 
        orderSubmitter, 
        limitOrderBook, 
        pastMarketOrderRateSeconds, 
        targetGain, 
        algoDeadlineSeconds, 
        orderSize,
        buyToSellThreshold):
        TradingAlgo.__init__(self)
        self.pastMarketOrderRateMillis = pastMarketOrderRateSeconds * 1000
        self.buyMarketOrderDistribution = WindowedMarketOrderSizeDistribution(BackTestWindownMaintainer(windowLengthSeconds=pastMarketOrderRateSeconds))
        self.sellMarketOrderDistribution = WindowedMarketOrderSizeDistribution(BackTestWindownMaintainer(windowLengthSeconds=pastMarketOrderRateSeconds))
        self.buyMarketOrderAggregator = MarketOrderAggregator(sell = False, onlyMarketOrder=True)
        self.sellMarketOrderAggregator = MarketOrderAggregator(sell = True, onlyMarketOrder=True)
        self.buyToSellThreshold = buyToSellThreshold
        self.limitOrderBook = limitOrderBook

        self.recordedTimeSlices = set()
        self.targetGain = targetGain
        self.algoDeadlineMillis = algoDeadlineSeconds * 1000

        self.buyPipeline = Pipeline([self.buyMarketOrderAggregator,self.buyMarketOrderDistribution])
        self.sellPipeline = Pipeline([self.sellMarketOrderAggregator,self.sellMarketOrderDistribution])
        
        self.boughtPrice = None
        self.boughtTime = None

        self.startTime = None
        
        self.marketOrderRatiosToGains = []
        self.currentMarketOrderRatio = None

    
    def accept(self,event):
        TradingAlgo.accept(self,event)

        self.buyMarketOrderAggregator.accept(event)
        self.sellMarketOrderAggregator.accept(event)

        self.currentTime = event.timestamp
        if self.startTime == None:
            self.startTime = self.currentTime

        #Don't do anything in the first window to build the distribution
        if self.currentTime - self.startTime < self.pastMarketOrderRateMillis:
            return

        if self.limitOrderBook.snapshotComplete == False:
            return

        if self.boughtPrice == None:
            if self.sellMarketOrderDistribution.sum() == 0:
                return

            self.boughtPrice = self.limitOrderBook.bestBid().price
            self.boughtTime = self.currentTime
            #if buy market orders size to sell market order size ratio is greater than
            #threshold amount, buy
            ratio = self.buyMarketOrderDistribution.sum()/self.sellMarketOrderDistribution.sum()
            print ("\n")
            print (f"Buy to sell market order size ratio: {ratio}")
            self.currentMarketOrderRatio = ratio
        
        else:
            #If we surpassed the gain we expect, sell
            #If we are at the deadline, sell
            #If we are below the bought price by the expected gain percentage, sell
            currentPrice = self.limitOrderBook.bestAsk().price
            pnl = (currentPrice - self.boughtPrice)/self.boughtPrice
            timeGapSinceBuy = self.currentTime - self.boughtTime
            #recordedTimeGaps = np.arange(5000,self.algoDeadlineMillis,5000)
            recordedTimeGaps = [self.algoDeadlineMillis]
            for timeGap in recordedTimeGaps:
                if timeGapSinceBuy >= timeGap and timeGap not in self.recordedTimeSlices:
                    print (f"Gain at time {self.currentTime}: {pnl}")
                    print (f"Adding gain to table {(self.currentMarketOrderRatio, pnl)}")
                    self.recordedTimeSlices.add(timeGap)
                    self.marketOrderRatiosToGains.append([self.currentMarketOrderRatio, pnl])
                    break    
            
            #Start a new window
            if timeGapSinceBuy >= recordedTimeGaps[-1]:
                self.boughtPrice = None
                self.recordedTimeSlices.clear()


    def orderSubmittedCallback(self,operation,orderId):
        pass

    def orderRejectedCallback(self,orderId,side):
        # print ("Order rejected= XXXXXXXXXX")
        if side == Side.BID:
            self.currentBidOrderId = None
        elif side == Side.ASK:
            self.currentAskOrderId = None

    def notifyMatchedCallback(self, matchedMarketOrder, algoOrder):
        if algoOrder.side == Side.BID:
            # print ("Bid order matched==========================================================")
            self.totalMoney -= algoOrder.price * (algoOrder.size/self.sizeFactor)
            self.inventory += algoOrder.size
            self.toBeFilledBidSize -= algoOrder.size
            if self.toBeFilledBidSize == 0:
                self.bidOrderFilled = True
            
        elif algoOrder.side == Side.ASK:
            # print ("Ask order matched==========================================================")
            self.totalMoney += algoOrder.price * (algoOrder.size/self.sizeFactor)
            self.inventory -= algoOrder.size
            self.toBeFilledAskSize -= algoOrder.size
            if self.toBeFilledAskSize == 0:
                self.askOrderFilled = True

