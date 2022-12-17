
from sortedcontainers import SortedDict 
import copy
from collections import defaultdict

from framework.pipe import Pipe
from framework.quote import Quote
from framework.limit_order import LimitOrder, Side, Operation
from framework.market_order import MarketOrder


class L3LimitOrderBookIterator:

    def __init__(self, bookOfSide, side):
        self.bookOfSide = bookOfSide
        self.currentPriceLevelIndex = len(self.bookOfSide)-1 if side == Side.BID else 0
        self.priceLevelIndexIncrement = -1 if side == Side.BID else 1
        self.currentPriceLevelOrderIndex = 0


    def getNextBestOrder(self):
        if self.currentPriceLevelIndex == -1 or self.currentPriceLevelIndex == len(self.bookOfSide):
            return None

        returnedOrder = self.bookOfSide.peekitem(index=self.currentPriceLevelIndex)[1][self.currentPriceLevelOrderIndex]
        self.currentPriceLevelOrderIndex += 1

        #This means we have exhausted all prices at this level. Go to the next level
        if self.currentPriceLevelOrderIndex == len(self.bookOfSide.peekitem(index=self.currentPriceLevelIndex)[1]):
            self.currentPriceLevelIndex += self.priceLevelIndexIncrement
            self.currentPriceLevelOrderIndex = 0

        return returnedOrder            



class L3LimitOrderMultiBook(Pipe):

    def __init__(self):
        self.limitOrderBooks = defaultdict(self._crossAllowedLimitOrderBook)

    def _crossAllowedLimitOrderBook(self):
        return L3LimitOrderBook(allowCrossedBook=True)

    def accept(self,limitOrder):
        if limitOrder.symbol is None:
            print (f"Symbol cannot be None: {limitOrder}")
            raise ("Exception symbol cannot be None")
        self.limitOrderBooks[limitOrder.symbol].accept(limitOrder)
    
    ######################
    #### MANIPULATORS ####
    ######################
    # Add an order at that price level
    def addOrder(self,limitOrder):
        self.limitOrderBooks[limitOrder.symbol].addOrder(limitOrder)
        

    # Update the size to this limitOrder's size for this limitOrder's price level
    # If the new size is 0, that limit order is removed from the book.
    def updateOrder(self,limitOrder):
        self.limitOrderBooks[limitOrder.symbol].updateOrder(limitOrder)

    def decrement(self, symbol, orderId, decrementSize):
        self.limitOrderBooks[symbol].decrement(orderId, decrementSize)

    def removeOrder(self, symbol, orderId):
        self.limitOrderBooks[symbol].removeOrder(orderId)

    def clearBook(self, symbol):
        self.limitOrderBooks[symbol].clearBook()
        

    def walkTheBook(self, marketOrder):
        self.limitOrderBooks[marketOrder.symbol].walkTheBook(marketOrder)

    def bestBid(self, symbol):
        return self.limitOrderBooks[symbol].bestBid()

    def bestAsk(self, symbol):
        return self.limitOrderBooks[symbol].bestAsk()

    def validBook(self, symbol):
        return self.limitOrderBooks[symbol].validBook()

    def hasBid(self, symbol):
        return self.limitOrderBooks[symbol].hasBid()

    def hasAsk(self, symbol):
        return self.limitOrderBooks[symbol].hasAsk()

    def symbols(self):
        return self.limitOrderBooks.keys()

    def getBidPriceLevels(self, ticker, priceLevelCount):
        return self.limitOrderBooks[ticker].getPriceLevels(Side.BID, priceLevelCount)

    def getAskPriceLevels(self, ticker, priceLevelCount):
        return self.limitOrderBooks[ticker].getPriceLevels(Side.ASK, priceLevelCount)

    def cumulativeValueAtBidPriceLevel(self, ticker, priceLevel):
        return self.limitOrderBooks[ticker].cumulativeValueAtPriceLevel(priceLevel, Side.BID)

    def cumulativeValueAtAskPriceLevel(self, ticker, priceLevel):
        return self.limitOrderBooks[ticker].cumulativeValueAtPriceLevel(priceLevel, Side.ASK)

    def cumulativeValueAtBidNumShares(self, ticker, numShares):
        return self.limitOrderBooks[ticker].cumulativeValueAtNumShares(numShares, Side.BID)

    def cumulativeValueAtAskNumShares(self, ticker, numShares):
        return self.limitOrderBooks[ticker].cumulativeValueAtNumShares(numShares, Side.ASK)

    def getMaxPriceLevelForValue(self, ticker, value, sell):
        return self.limitOrderBooks[ticker].getMaxPriceLevelForValue(value, sell)

    def getMaxPriceLevelForVolume(self, ticker, volume, sell):
        return self.limitOrderBooks[ticker].getMaxPriceLevelForVolume(volume, sell)

    def walkTheBookForValue(self, ticker, value, sell):
        return self.limitOrderBooks[ticker].walkTheBookForValue(value, sell)

    def mid(self, ticker):
        return self.limitOrderBooks[ticker].mid()

    def applyMarketOrder(self, marketOrder):
        self.limitOrderBooks[marketOrder.symbol].applyMarketOrder(marketOrder)

class L3LimitOrderBook(Pipe):

    def __init__(self, allowCrossedBook = False):
        self.bids = SortedDict()
        self.asks = SortedDict()
        self.orders = dict() # Find order by ID
        self.snapshotComplete = False
        self.allowCrossedBook = allowCrossedBook

    def _addOrderToSide(self,limitOrder,bookOfSide):
        bookOfSide.setdefault(limitOrder.price,[]).append(limitOrder.orderId)
        self.orders.__setitem__(limitOrder.orderId, limitOrder)

    def _updateOrderOnSide(self,limitOrder,bookOfSide):
        if bookOfSide.get(limitOrder.price,None) == None:
            raise Exception("Order to update for price level does not exist!")

        for i in range(len(bookOfSide[limitOrder.price])):
            if bookOfSide[limitOrder.price][i] == limitOrder.orderId:
                    self.orders[limitOrder.orderId].size = limitOrder.size
                    if self.orders[limitOrder.orderId].size == 0:
                        self._removeOrderOnSide(self.orders[limitOrder.orderId], bookOfSide)
                    break

    def _removeOrderOnSide(self,removedOrder,bookOfSide):
        if bookOfSide.get(removedOrder.price,None) == None:
            raise Exception("Order to remove for price level does not exist!")

        for i in range(len(bookOfSide[removedOrder.price])):
            if bookOfSide[removedOrder.price][i] == removedOrder.orderId:
                
                del bookOfSide[removedOrder.price][i]
                self.orders.pop(removedOrder.orderId)
                if len(bookOfSide[removedOrder.price]) == 0:
                    bookOfSide.pop(removedOrder.price, None)

                return

        raise Exception("Could not remove the order to be removed")
            
    def _getSizeAtLevel(self, priceLevelOrderIdQueue):
        return sum(self.orders[orderId].size for orderId in priceLevelOrderIdQueue)

    def accept(self,limitOrder):
        if type(limitOrder) != LimitOrder:
            return

        try:
            if limitOrder.operation == Operation.ADD:
                self.addOrder(limitOrder)
            elif limitOrder.operation == Operation.UPDATE:
                if limitOrder.size == 0:
                    self.removeOrder(limitOrder.orderId)
                else:
                    self.updateOrder(limitOrder)
            elif limitOrder.operation == Operation.DECREASE:
                self.decrement(limitOrder.orderId,limitOrder.size)
            elif limitOrder.operation == Operation.CLEAR:
                print ("Clear event received by limit order book")
                self.clearBook()
            elif limitOrder.operation == Operation.SNAPSHOT_COMPLETE:
                print ("Snapshot complete event received")
                self.snapshotComplete = True
                
        except KeyError:
            #The incoming order id may be an order that is not in the order book.
            #(It may be an order that's already removed or a market order that was never added)
            pass

    
    ######################
    #### MANIPULATORS ####
    ######################
    # Add an order at that price level
    def addOrder(self,limitOrder):
        if self.orders.get(limitOrder.orderId,None) is not None:
            raise Exception(f"Adding order for an existing order ID: {limitOrder.orderId}")

        if limitOrder.side == Side.BID:
            #Adding a crossing order is not allowed
            if len(self.asks) > 0 and limitOrder.price >= self.bestAsk().price and not self.allowCrossedBook:
                return False
            self._addOrderToSide(limitOrder,self.bids)
        elif limitOrder.side == Side.ASK:
            #Adding a crossing order is not allowed
            if len(self.bids) > 0 and limitOrder.price <= self.bestBid().price and not self.allowCrossedBook:
                return False
            self._addOrderToSide(limitOrder,self.asks)
        else:
            print (f"Unexpected order side being added to limit order book {limitOrder.side}")
            raise Exception("Unexpected order side being added to limit order book")

        # #Sanity check
        if len(self.bids) > 0 and len(self.asks) > 0 and self.bestBid().price >= self.bestAsk().price and not self.allowCrossedBook:
            raise Exception("Crossed book")

        return True

    #This is an O(n) algo. This can be improved to be O(log(n)) but this requires a 
    #a special binary search tree implementation (that keeps track of volume for items less than/greater than).
    def cumulativeVolumeAtPriceLevel(self,priceLevel,side):
        cumulativeSize = 0
        if side == Side.BID:
            iterableBook = reversed(self.bids)
            book = self.bids
        elif side == Side.ASK:
            iterableBook = self.asks
            book = self.asks
        else:
            raise Exception("Unexpected order side being added to limit order book")

        for currentPriceLevel in iterableBook:
            if currentPriceLevel > priceLevel and side == Side.ASK or \
               currentPriceLevel < priceLevel and side == Side.BID:
               break

            cumulativeSize += self._getSizeAtLevel(book[currentPriceLevel])

        return cumulativeSize


    # Update the size to this limitOrder's size for this limitOrder's price level
    # If the new size is 0, that limit order is removed from the book.
    def updateOrder(self,limitOrder):
        #Check if this order id exists at all.
        #If it does not, this will throw
        existingOrder = self.orders.get(limitOrder.orderId)
        if existingOrder is None:
            return

        if limitOrder.side == Side.BID:
            self._updateOrderOnSide(limitOrder,self.bids)
        elif limitOrder.side == Side.ASK:
            self._updateOrderOnSide(limitOrder,self.asks)
        else:
            raise Exception("Unexpected order side being updated on the limit order book")		

    def decrement(self, orderId, decrementSize):
        limitOrder = self.orders.get(orderId)
        if limitOrder is None:
            return
        limitOrder.size -= decrementSize
        limitOrder.operation = Operation.UPDATE
        if limitOrder.size < -0.0001:
            raise Exception(f"Removing size greater than order size. Order Id: {orderId} , New size: {limitOrder.size}")

        self.updateOrder(limitOrder)

    def removeOrder(self, orderId):
        removedOrder = self.orders.get(orderId)
        if removedOrder is None:
            return
        if removedOrder.side == Side.BID:
            self._removeOrderOnSide(removedOrder,self.bids)
        elif removedOrder.side == Side.ASK:
            self._removeOrderOnSide(removedOrder,self.asks)
        else:
            raise Exception("Unexpected order side being updated on the limit order book")

    def clearBook(self):
        self.bids.clear()
        self.asks.clear()
        self.orders.clear()
        self.snapshotComplete = False

    ###################
    #### ACCESSORS ####
    ###################
    def best(self, side):
        if (side == Side.BID):
            return self.bestBid()
        elif (side == Side.ASK):
            return self.bestAsk()
        else:
            raise Exception("Unexpected order side for best quote in limit order book")

    def bestBid(self):
        bestBidQuote = self.bids.peekitem(index=-1)
        return Quote(bestBidQuote[0], self._getSizeAtLevel(bestBidQuote[1]))

    def bestAsk(self):
        bestAskQuote = self.asks.peekitem(index=0)
        return Quote(bestAskQuote[0], self._getSizeAtLevel(bestAskQuote[1]))

    def spread(self):
        return self.bestAsk().price - self.bestBid().price

    def halfSpread(self):
        return self.spread()/2

    def volumeAtPriceLevel(self,priceLevel,side):
        if side == Side.BID:
            return self._getSizeAtLevel(self.bids.get(priceLevel,[]))
        elif side == Side.ASK:
            return self._getSizeAtLevel(self.asks.get(priceLevel,[]))
        else:
            raise Exception("Unexpected order side being updated on the limit order book")

    def mid(self):
        return (self.bestBid().price + self.bestAsk().price)/2

    
    def cumulativeValueAtPriceLevel(self, priceLevel, side):
        cumulativeValue = 0
        if side == Side.BID:
            iterableBook = reversed(self.bids)
            book = self.bids
        elif side == Side.ASK:
            iterableBook = self.asks
            book = self.asks
        else:
            raise Exception("Unexpected order side being added to limit order book")

        for currentPriceLevel in iterableBook:
            if currentPriceLevel > priceLevel and side == Side.ASK or \
               currentPriceLevel < priceLevel and side == Side.BID:
               break

            cumulativeValue += self._getSizeAtLevel(book[currentPriceLevel]) * currentPriceLevel

        return cumulativeValue

    def cumulativeValueAtNumShares(self, numShares, side):
        if side == Side.BID:
            iterableBook = reversed(self.bids)
            book = self.bids
        elif side == Side.ASK:
            iterableBook = self.asks
            book = self.asks
        else:
            raise Exception("Unexpected order side being added to limit order book")

        cumulativeValue = 0
        numSharesToUnwind = numShares
        lastPriceLevel = 0
        foundEnoughSharesInBookToUnwind = False
        for currentPriceLevel in iterableBook:
            lastPriceLevel = currentPriceLevel
            totalSizeAtThisLevel = self._getSizeAtLevel(book[lastPriceLevel])
            if numSharesToUnwind < totalSizeAtThisLevel:
                foundEnoughSharesInBookToUnwind = True
                break

            cumulativeValue += totalSizeAtThisLevel * lastPriceLevel
            numSharesToUnwind -= totalSizeAtThisLevel

        if not foundEnoughSharesInBookToUnwind:
            raise Exception("Not enough shares in book to unwind")
            
        cumulativeValue += numSharesToUnwind * lastPriceLevel
        return cumulativeValue



    def walkTheBook(self, marketOrder):
        otherSide = MarketOrder.getOtherSide(marketOrder)
        matchedOrders = []
        
        if otherSide == Side.BID:
            lobIterator = L3LimitOrderBookIterator(self.bids, Side.BID)
        else:
            lobIterator = L3LimitOrderBookIterator(self.asks, Side.ASK)

        unmatchedAmount = marketOrder.size
        while unmatchedAmount > 0:
            orderId = lobIterator.getNextBestOrder()
            if orderId == None:
                print (f"No limit orders left to match this market order {marketOrder}. Remaining size unmatched is: {unmatchedAmount}")
                break
            order = self.orders[orderId]
            if unmatchedAmount >= order.size:
                unmatchedAmount -= order.size
                matchedOrders.append(order)
            else:
                appendedOrder = copy.copy(order)
                appendedOrder.size = unmatchedAmount
                matchedOrders.append(appendedOrder)
                unmatchedAmount = 0

        return matchedOrders

    def walkTheBookForValue(self, value, sell):
        
        matchedOrders = []
        
        if sell:
            lobIterator = L3LimitOrderBookIterator(self.bids, Side.BID)
        else:
            lobIterator = L3LimitOrderBookIterator(self.asks, Side.ASK)

        unmatchedAmount = value
        while unmatchedAmount > 0:
            orderId = lobIterator.getNextBestOrder()
            if orderId == None:
                print (f"No limit orders left to match this market order {marketOrder}. Remaining size unmatched is: {unmatchedAmount}")
                break
            order = self.orders[orderId]
            if unmatchedAmount >= order.size * order.price:
                unmatchedAmount -= order.size * order.price
                matchedOrders.append(order)
            else:
                appendedOrder = copy.copy(order)
                appendedOrder.size = unmatchedAmount / appendedOrder.price
                matchedOrders.append(appendedOrder)
                unmatchedAmount = 0

        return matchedOrders

    def getMaxPriceLevelForValue(self, value, sell):
        matchedOrders = self.walkTheBookForValue(value, sell)
        matchedOrders = sorted(matchedOrders, key=lambda x: x.price)

        if sell:
            return matchedOrders[0].price
        else:
            return matchedOrders[-1].price

    def getMaxPriceLevelForVolume(self, volume, sell):
        marketOrder = MarketOrder(size=volume, sell=sell)
        matchedOrders = self.walkTheBook(marketOrder)
        matchedOrders = sorted(matchedOrders, key=lambda x: x.price)
        if sell:
            return matchedOrders[0].price
        else:
            return matchedOrders[-1].price

    def getPriceLevels(self, side, priceLevelCount):
        priceLevels = []
        if side == Side.BID:
            for price in reversed(self.bids):
                priceLevels.append(price)
                if len(priceLevels) == priceLevelCount:
                    break
        elif side == Side.ASK:
            for price in self.asks:
                priceLevels.append(price)
                if len(priceLevels) == priceLevelCount:
                    break
        return priceLevels

    def validBook(self):
        return not (len(self.bids) > 0 and len(self.asks) > 0 and self.bestBid().price >= self.bestAsk().price)

    def hasBid(self):
        return len(self.bids) > 0

    def hasAsk(self):
        return len(self.asks) > 0

    def applyMarketOrder(self, marketOrder):
        
        matchedOrders = self.walkTheBook(marketOrder)
        unfilledSize = marketOrder.size
        for limitOrder in matchedOrders:
            if unfilledSize == 0:
                return
                
            if self.orders[limitOrder.orderId].size <= unfilledSize:
                unfilledSize -= self.orders[limitOrder.orderId].size
                self.removeOrder(limitOrder.orderId)
            else:
                self.decrement(limitOrder.orderId, unfilledSize)
                return






