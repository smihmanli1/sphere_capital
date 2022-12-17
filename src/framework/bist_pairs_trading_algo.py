
from pipe import Pipe
from trading_algo import TradingAlgo
from collections import defaultdict
from limit_order import Side
import datetime
from market_order import MarketOrder
import math

MILLIS_IN_A_SEC = 1000

class BistPairsTradingAlgo(TradingAlgo):

    #Find type for 'parameters' and 'tickerPairs' in bist50_pairs_trading_backtester
    def __init__(self, backtestOrderSubmitter, lob, parameters, tickerPairs):
        self.backtestOrderSubmitter = backtestOrderSubmitter
        self.lob = lob
        self.eventCount = 0
        self.parameters = parameters
        self.tickerPairs = tickerPairs
        self.allTickers = set()
        self.startPrices = {}
        self.tickerPairsMapping = defaultdict(list)
        self.means = {}
        self.standardDeviations = {}
        self.ordersOutgoing = set()
        self.positions = {}
        self._initializePositions(tickerPairs)
        self.currentUsdTry = 8.5
        self.positionValueLimit = 50000
        self.minimumPositionValueToOpen = 1000
        self.securityStatuses = {}
        self.doneForTheDay = False
        self.invalidBooks = defaultdict(bool)
        self.lastTimeTraded = parameters["exchange_open_time"]

        self.totalMoney = 0

        for tickerPair in tickerPairs:
            self.allTickers.add(tickerPair.ticker1)
            self.allTickers.add(tickerPair.ticker2)
            self.startPrices[tickerPair.ticker1] = tickerPair.ticker1StartPrice
            self.startPrices[tickerPair.ticker2] = tickerPair.ticker2StartPrice
            self.tickerPairsMapping[tickerPair.ticker1].append(tickerPair.ticker2)
            self.tickerPairsMapping[tickerPair.ticker2].append(tickerPair.ticker1)

            pairSorted = self._getPairSorted(tickerPair.ticker1, tickerPair.ticker2)
            self.standardDeviations[pairSorted] = tickerPair.percentChangeDiffStdDev
            self.means[pairSorted] = tickerPair.percentChangeDiffMean


        print ("Pairs trading algo created with following pairs")
        for pair in tickerPairs:
            print (pair.ticker1,pair.ticker2)
            print (f"{pair.ticker1}: {pair.ticker1StartPrice}, {pair.ticker2}: {pair.ticker2StartPrice}, mean percent change diff: {pair.percentChangeDiffMean}, percent change diff std dev: {pair.percentChangeDiffStdDev}")

    
    def _initializePositions(self, tickerPairs):
        for tickerPair in tickerPairs:
            pairSorted = self._getPairSorted(tickerPair.ticker1, tickerPair.ticker2)
            self.positions[pairSorted] = {}
            self.positions[pairSorted][tickerPair.ticker1] = 0
            self.positions[pairSorted][tickerPair.ticker2] = 0


    def _getPairSorted(self, ticker1, ticker2):
        tickerPairList = [ticker1, ticker2]
        tickerPairList = sorted(tickerPairList)
        return (tickerPairList[0], tickerPairList[1])

    def _getPriceDifference(self, ticker, newPrice):
        initialPrice = self.startPrices[ticker]
        return (newPrice - initialPrice)/initialPrice

    def _sendOrders(self, newMarketOrders):
        for pairSorted, ticker, positionChange in newMarketOrders:
            self.positions[pairSorted][ticker] += positionChange
            if positionChange < 0:
                moneyMade = self.lob.cumulativeValueAtBidNumShares(ticker, abs(positionChange))
                print (f"Money made by selling {ticker} {abs(positionChange)} shares: {moneyMade}")
                self.totalMoney += moneyMade
                self.backtestOrderSubmitter.sendMarketOrder(MarketOrder(symbol=ticker, sell=True, size=-positionChange))
                
            else:
                moneyLost = self.lob.cumulativeValueAtAskNumShares(ticker, positionChange)
                print (f"Money lost by buying {ticker} {positionChange} shares: {moneyLost}")
                self.totalMoney -= moneyLost
                self.backtestOrderSubmitter.sendMarketOrder(MarketOrder(symbol=ticker, sell=False, size=positionChange))
                

        if len(newMarketOrders) > 0:
            print (f"Positions: {self.positions}")
            print (f"totalMoney: {self.totalMoney} TL")

        
    def _getIn(self, askTicker, bidTicker, positionValueLimitPerTicker):
        
        askTickerBestAsk = self.lob.bestAsk(askTicker)
        bidTickerBestBid = self.lob.bestBid(bidTicker)
        
        availableValueToLong = askTickerBestAsk.price * askTickerBestAsk.size
        availableValueToShort = bidTickerBestBid.price * bidTickerBestBid.size

        availableValueToTrade = min(availableValueToLong, availableValueToShort, positionValueLimitPerTicker)
        totalToLong = availableValueToTrade / askTickerBestAsk.price
        totalToShort = availableValueToTrade / bidTickerBestBid.price

        # return askTicker, math.floor(totalToLong), bidTicker, math.ceil(totalToShort)
        return askTicker, (totalToLong), bidTicker, (totalToShort)

    def _getOut(self, askTicker, bidTicker, askTickerNumSharesToClose, bidTickerNumSharesToClose):
        
        if bidTickerNumSharesToClose == 0:
            return askTicker, askTickerNumSharesToClose, bidTicker, bidTickerNumSharesToClose

        askTickerBestAsk = self.lob.bestAsk(askTicker)
        bidTickerBestBid = self.lob.bestBid(bidTicker)

        askToBidNumSharesRatio = askTickerNumSharesToClose / bidTickerNumSharesToClose

        totalToLong = min(askTickerNumSharesToClose, askTickerBestAsk.size)
        totalToShort = min(bidTickerNumSharesToClose, bidTickerBestBid.size) 

        actionableRatio = totalToLong/totalToShort
        #If the ratios are close enough we are good to go
        if abs(actionableRatio - askToBidNumSharesRatio) <= 0.01:
            return askTicker, totalToLong, bidTicker, totalToShort
        elif actionableRatio - askToBidNumSharesRatio > 0.01:
            #We have to reduce totalToLong so that the ratios are same
            totalToLong = totalToShort * askToBidNumSharesRatio
        else: #actionableRatio - askToBidNumSharesRatio < -0.01
            #We have to reduce totalToShort so that the ratios are same
            totalToShort = totalToLong / askToBidNumSharesRatio

        return askTicker, totalToLong, bidTicker, totalToShort

    def _getPositionValueLimitInLocalCurrency(self):
        return self.positionValueLimit * self.currentUsdTry

    def _getMinimumPositionValueToOpenInLocalCurrency(self):
        return self.minimumPositionValueToOpen * self.currentUsdTry   

    
    def _closeAllPositions(self):
        totalPositions = defaultdict(float)

        allMarketOrders = []
        for pairSorted in self.positions:
            for ticker in self.positions[pairSorted]:
                totalPositions[ticker] += self.positions[pairSorted][ticker]


        print (f"Closing all these positions at mid price level: {totalPositions}")
        for ticker in totalPositions:
            tickerMidPrice = self.lob.mid(ticker)
            moneyMade = tickerMidPrice * totalPositions[ticker]
            print (f"Closing {ticker} {totalPositions[ticker]} shares at {tickerMidPrice}")
            print (f"Money made {moneyMade}")
            self.totalMoney += moneyMade

        print (f"Total money at end of day: {self.totalMoney} TL")


    def _printBookForTicker(self, ticker):
        bidPriceLevels = self.lob.getBidPriceLevels(ticker, 10)
        askPriceLevels = self.lob.getAskPriceLevels(ticker, 10)
        for askPriceLevel in reversed(askPriceLevels):
            print (f"{askPriceLevel}, {self.lob.limitOrderBooks[ticker].volumeAtPriceLevel(askPriceLevel, Side.ASK)}")
        print ("---")
        for bidPriceLevel in bidPriceLevels:
            print (f"{bidPriceLevel}, {self.lob.limitOrderBooks[ticker].volumeAtPriceLevel(bidPriceLevel, Side.BID)}")
        print ()


    #Assume: bidTicker is already shorted or is 0
    #Assume: askTicker is already longed or is 0
    #If this returns positive, it means this is suggesting opening some positions.
    #If this returns negative or 0, this is suggesting we should close some positions. But,
    #we can't use this negative target position value. We have to see what _getCloseTargetPosition will tell us.
    def _getOpenTargetPosition(self, askTickerBestAsk, bidTickerBesBid, askTicker, bidTicker, positionValueLimitPerTicker):
        
        bidPriceChange = self._getPriceDifference(bidTicker, bidTickerBesBid)
        askPriceChange = self._getPriceDifference(askTicker, askTickerBestAsk)

        pairSorted = self._getPairSorted(bidTicker, askTicker)
        percentChangeDiffStdDev = self.standardDeviations[pairSorted]
        percentChangeDiffMean = self.means[self._getPairSorted(bidTicker, askTicker)]
        lowerBuyThreshold = percentChangeDiffMean + self.parameters["trading_lower_buy_threshold"] * percentChangeDiffStdDev
        upperBuyThreshold = percentChangeDiffMean + self.parameters["trading_upper_buy_threshold"] * percentChangeDiffStdDev

        priceChangeDifference = bidPriceChange - askPriceChange
        bidTickerValue = self.lob.cumulativeValueAtAskNumShares(bidTicker, abs(self.positions[pairSorted][bidTicker]))
        askTickerValue = self.lob.cumulativeValueAtBidNumShares(askTicker, abs(self.positions[pairSorted][askTicker]))
        #Note that bid price change has to be above ask price change for this to hold true
        #because lhs is always positive
        if lowerBuyThreshold < priceChangeDifference and priceChangeDifference < upperBuyThreshold:
            
            #How much do we want to open, if we want to

            #Ratio of positionValueLimitPerTicker to have
            #If actual r is greater than 1, we set it to 1
            r = min((priceChangeDifference - lowerBuyThreshold) / (upperBuyThreshold - lowerBuyThreshold), 1)

            targetPositionValue = r * positionValueLimitPerTicker
            
            result = targetPositionValue - (bidTickerValue + askTickerValue)/2
        elif upperBuyThreshold <= priceChangeDifference:
            result = positionValueLimitPerTicker - (bidTickerValue + askTickerValue)/2
        elif priceChangeDifference <= lowerBuyThreshold:
            result = 0

        if result < 100:
            result = 0

        return result



    #Assume: bidTicker is already longed or is 0
    #Assume: askTicker is already shorted or is 0
    #If this returns negative or 0, it means this is suggesting closing some positions.
    #If this returns positive, this is suggesting we should open some positions. But,
    #we can't use this target position value. We have to see what _getOpenTargetPosition told us to do.
    def _getCloseTargetPosition(self, askTickerBestAsk, bidTickerBesBid, askTicker, bidTicker):
        bidPriceChange = self._getPriceDifference(bidTicker, bidTickerBesBid)
        askPriceChange = self._getPriceDifference(askTicker, askTickerBestAsk)

        pairSorted = self._getPairSorted(bidTicker, askTicker)
        percentChangeDiffStdDev = self.standardDeviations[pairSorted]
        percentChangeDiffMean = self.means[self._getPairSorted(bidTicker, askTicker)]
        lowerBuyThreshold = percentChangeDiffMean + self.parameters["trading_lower_buy_threshold"] * percentChangeDiffStdDev
        upperBuyThreshold = percentChangeDiffMean + self.parameters["trading_upper_buy_threshold"] * percentChangeDiffStdDev

        priceChangeDifference = askPriceChange - bidPriceChange
        #Note that bid price change has to be above ask price change for this to hold true
        #because lhs is always positive
        # if lowerBuyThreshold < priceChangeDifference and priceChangeDifference < upperBuyThreshold:
        if lowerBuyThreshold < priceChangeDifference and priceChangeDifference < upperBuyThreshold:
            
            #How much do we want to close if we want to

            #Ratio of positionValueLimitPerTicker to have
            #If actual r is greater than 1, we set it to 1
            r = min((priceChangeDifference - lowerBuyThreshold) / (upperBuyThreshold - lowerBuyThreshold), 1)

            #Ask ticker is the one that will be longed. Therefore its position value must be negative.
            askTickerNumSharesToClose = (1-r) * abs(self.positions[pairSorted][askTicker])
            bidTickerNumSharesToClose = (1-r) * self.positions[pairSorted][bidTicker]

            return 0,0
            # return askTickerNumSharesToClose, bidTickerNumSharesToClose
        elif upperBuyThreshold <= priceChangeDifference:
            return 0,0
        elif priceChangeDifference < lowerBuyThreshold:
            return abs(self.positions[pairSorted][askTicker]), self.positions[pairSorted][bidTicker]


    def _symbolChanged(self, thisTicker, relevantTicker, timestamp):
        
        if not self.lob.validBook(thisTicker) or not self.lob.validBook(relevantTicker):
            return
            # print (f"{thisTicker} invalid book. Time: {datetime.datetime.utcfromtimestamp(timestamp/MILLIS_IN_A_SEC)}")
            # askPriceLevels = self.lob.getAskPriceLevels(thisTicker, 10)
            # bidPriceLevels = self.lob.getBidPriceLevels(thisTicker, 10)
            # print (f"Bids")
            # for bidPriceLevel in bidPriceLevels:
            #     print (f"{bidPriceLevel}, {self.lob.limitOrderBooks[thisTicker].volumeAtPriceLevel(bidPriceLevel, Side.BID)}")
            # print (f"Asks")
            # for askPriceLevel in askPriceLevels:
            #     print (f"{askPriceLevel}, {self.lob.limitOrderBooks[thisTicker].volumeAtPriceLevel(askPriceLevel, Side.ASK)}")
            # self.invalidBooks[thisTicker] = True
            # print (f"FATAL - Book is not valid for {thisTicker}")
            # exit(1)
        # else:
        #     if self.invalidBooks[thisTicker] is True:
        #         self.invalidBooks[thisTicker] = False
        #         print (f"{thisTicker} book became valid. Time: {datetime.datetime.utcfromtimestamp(timestamp/MILLIS_IN_A_SEC)}")

        
        if self.securityStatuses[thisTicker] != 'P_SUREKLI_ISLEM' or self.securityStatuses[relevantTicker] != 'P_SUREKLI_ISLEM':
            print ("Not in continuous trading")
            return

        

        newMarketOrders = []

        thisTickerBestBid = self.lob.bestBid(thisTicker).price
        thisTickerBestAsk = self.lob.bestAsk(thisTicker).price

        relevantTickerBestBid = self.lob.bestBid(relevantTicker).price
        relevantTickerBestAsk = self.lob.bestAsk(relevantTicker).price

        pairSorted = self._getPairSorted(thisTicker, relevantTicker)
        #TODO: Is it possible that we can't fully get out and before we fully get out
        #get in returns true? (Best to try and see. Definitely error out if that happens.
        # i.e. we should error out if we are advised to do opposite of where we currently are.
        # i.e. get out in the wrong direction or get in in the wrong direction)
        #TODO: For now, for simplicity, until our order is filled, we shouldn't send any more orders
        #for either of these securities. (i.e. blacklist the securities until the orders are filled)
        #This should be true for this iteration. (i.e. if getOut was true, we should not act on get in
        #until the get out orders are filled.)
        #TODO: For now, for simplicity putting in market orders. It may partially get filled at an
        # undesired price since the book may change until the market order makes it to the exchange. 
        # Later, we will check how often that happens.
        #TODO: We may want to add a constraint of not exhausting more than 3 price levels.
        # I don't know what consequences does it have to exhaust many price levels.

        positionsForPair = self.positions[pairSorted]
        if self.positions[pairSorted][thisTicker] <= 0 and self.positions[pairSorted][relevantTicker] >= 0:

            openTargetPosition = self._getOpenTargetPosition(relevantTickerBestAsk, thisTickerBestBid, relevantTicker, thisTicker, self._getPositionValueLimitInLocalCurrency()/2)
            thisTickerNumSharesToClose, relevantTickerNumSharesToClose = self._getCloseTargetPosition(thisTickerBestAsk, relevantTickerBestBid, thisTicker, relevantTicker)

            #We have suggestion to close positions and open positions at the same time.
            if openTargetPosition > 0 and (thisTickerNumSharesToClose > 0 or relevantTickerNumSharesToClose > 0):
                print ("FATAL - Contradicting suggestions")
                exit(1)


            if openTargetPosition > 0:
                print (f"Get in ({timestamp}): Value to be opened: {openTargetPosition} Long: {relevantTicker}:{self.lob.bestAsk(relevantTicker).price}, Short: {thisTicker}:{self.lob.bestBid(thisTicker).price}")
                #Open positions as much as possible as dictated by the openTargetPosition
                buyTicker, numSharesBuy, sellTicker, numSharesSell = self._getIn(relevantTicker, thisTicker, openTargetPosition)
                print (f"Num shares to sell: {numSharesSell}, Num shares to buy: {numSharesBuy}" )
                if numSharesSell != 0 and numSharesBuy != 0:
                    newMarketOrders.append((pairSorted, buyTicker, numSharesBuy))
                    newMarketOrders.append((pairSorted, sellTicker, -numSharesSell))

            if thisTickerNumSharesToClose > 0 or relevantTickerNumSharesToClose > 0:
                print (f"Get out () Long: {thisTicker}:{self.lob.bestAsk(thisTicker).price}, Short: {relevantTicker}:{self.lob.bestBid(relevantTicker).price}")
                #Close positions as much as possible as dictated by the closeTargetPosition
                buyTicker, numSharesBuy, sellTicker, numSharesSell = self._getOut(thisTicker, relevantTicker, thisTickerNumSharesToClose, relevantTickerNumSharesToClose)
                print (buyTicker, numSharesBuy, sellTicker, numSharesSell)
                if numSharesSell != 0 and numSharesBuy != 0:
                    newMarketOrders.append((pairSorted, buyTicker, numSharesBuy))
                    newMarketOrders.append((pairSorted, sellTicker, -numSharesSell))
                

        #After uncrossing, send all the market orders
        if len(newMarketOrders) > 0:
            self._sendOrders(newMarketOrders)
            #TODO: We should really call this
            self._symbolChanged(thisTicker, relevantTicker, timestamp)

            

    def _printBook(self, ticker, timestamp, numLevels):
        print (f"{ticker} book mid {self.lob.mid(ticker)} at : {datetime.datetime.utcfromtimestamp(timestamp)}")
        bidPriceLevels = self.lob.getBidPriceLevels(ticker, numLevels)
        askPriceLevels = self.lob.getAskPriceLevels(ticker, numLevels)
        print (f"Asks")
        for askPriceLevel in askPriceLevels:
            print (f"{askPriceLevel}, {self.lob.limitOrderBooks[ticker].volumeAtPriceLevel(askPriceLevel, Side.ASK)}")
        print (f"Bids")
        for bidPriceLevel in bidPriceLevels:
            print (f"{bidPriceLevel}, {self.lob.limitOrderBooks[ticker].volumeAtPriceLevel(bidPriceLevel, Side.BID)}")   

    def accept(self, event):
        
        #TODO: We should not trade if our best bid/ask does not match their best bid ask. (For this 
        # we should keep a copy of the book that is not manipulated by our orders)
        if self.doneForTheDay:
            return
        
        #This is where the algo takes action
        eventTime = datetime.datetime.utcfromtimestamp(event.timestamp / MILLIS_IN_A_SEC)
        self.eventCount += 1
        if self.eventCount % 100000 == 0:
            print (f"Processed {self.eventCount} events")
            print (f"Time: {eventTime}")

        # Close all positions some time before exchange closes
        if (eventTime + datetime.timedelta(minutes = self.parameters["close_all_positions_mins_left"])).time() > self.parameters["exchange_close_time"]:
            print (f"Closing all positions and done for the day. Time: {eventTime}")
            self._closeAllPositions()
            self.doneForTheDay = True
        
        self.securityStatuses[event.symbol] = event.session

        currentTime = eventTime.time()
        # if (eventTime - datetime.timedelta(minutes = 5)).time() > self.lastTimeTraded:
        # self.lastTimeTraded = eventTime.time()
        if currentTime < self.parameters["exchange_open_time"]:
            return
        for tickerPair in self.tickerPairs:

            if event.symbol == tickerPair.ticker1 or event.symbol == tickerPair.ticker2:
                # print (f"Checking for trigger at time: {eventTime}")
                # print (tickerPair.ticker1)
                # self._printBookForTicker(tickerPair.ticker1)
                # print (tickerPair.ticker2)
                # self._printBookForTicker(tickerPair.ticker2)
                self._symbolChanged(tickerPair.ticker1, tickerPair.ticker2, event.timestamp)
                self._symbolChanged(tickerPair.ticker2, tickerPair.ticker1, event.timestamp)

    def orderAckedCallback(self,operation,orderId):
        pass

    def orderRejectedCallback(self,orderId,side):
        pass

    def notifyFilledCallback(self, matchedMarketOrder, algoOrder):
        # When we get a fill, we should run the above algo again.
        pass
