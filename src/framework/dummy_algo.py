# This algorithm is a market making algorithm. It places orders based on
# current best bid and ask and the current market order size distribution and tries to pocket the spread.

import math

from trading_algo import TradingAlgo
from limit_order import Side

class DummyTradingAlgo(TradingAlgo):

	def __init__(self, orderSubmitter, limitOrderBook):
		TradingAlgo.__init__(self)
		self.orderSubmitter = orderSubmitter
		self.orderSubmitter.registerOrderAckedListener(self)
		self.limitOrderBook = limitOrderBook
		
		self.currentTime = 0
		self.windowStart = 0
		self.windowEnd = 0
		self.currentBidOrderId = None
		self.currentAskOrderId = None
		self.windowLengthMillis = 60000
		self.startTime = None
		self.bidFillPrice = 0
		self.askFillPrice = 0
		self.bidFillSize = 0
		self.askFillSize = 0

		self.toBeFilledBidSize = 1000
		self.toBeFilledAskSize = 1000
		self.totalSizeInFrontOfBidOrder = None
		self.totalSizeInFrontOfAskOrder = None


	def _getOptimalDelta(self, side):
		priceLevels = self.limitOrderBook.getPriceLevels(side,30)
		midPrice = self.limitOrderBook.mid()
		previousCandidateDelta,previousExpectedProfit = None,None
		previousPriceLevel = midPrice
		if side == Side.BID:
			tickSizeIncrement = -0.01
		elif side == Side.ASK:
			tickSizeIncrement = 0.01
		
		marketOrderDistribution = self.getMarketOrderDistributionForSide(side)
		for priceLevel in priceLevels:
			#Calculate expected profit of an order put on this price level
			normalDelta = abs(priceLevel - midPrice)
			sizeAheadOfPriceLevel = self.limitOrderBook.cumulativeVolumeAtPriceLevel(priceLevel,side)
			probabilityOfFillAtPriceLevel = 1 - marketOrderDistribution.getPercentileOfValue(sizeAheadOfPriceLevel)
			normalExpectedProfit = normalDelta * probabilityOfFillAtPriceLevel

			#Calculate expected profit of an order put at one tick size worse that this price level
			probabilityOfFillAtPriceLevelOneTickSizeMore = 0
			oneTickSizeMoreDelta = 0
			if priceLevel + tickSizeIncrement != previousPriceLevel:
				sizeAheadOfOneTickMorePriceLevel = self.limitOrderBook.cumulativeVolumeAtPriceLevel(priceLevel + tickSizeIncrement,side)
				probabilityOfFillAtPriceLevelOneTickSizeMore = 1 - marketOrderDistribution.getPercentileOfValue(sizeAheadOfOneTickMorePriceLevel)
				oneTickSizeMoreDelta = abs(priceLevel + tickSizeIncrement - midPrice)
			oneTickSizeMoreExpectedProfit = oneTickSizeMoreDelta * probabilityOfFillAtPriceLevelOneTickSizeMore

			#Calculate which order has higher expected profit
			if oneTickSizeMoreExpectedProfit > normalExpectedProfit:
				delta = oneTickSizeMoreDelta
				expectedProfit = oneTickSizeMoreExpectedProfit
			else:
				delta = normalDelta
				expectedProfit = normalExpectedProfit

			# Check if we have found the max. Return once we find the max
			if previousCandidateDelta is None:
				previousCandidateDelta,previousExpectedProfit = delta, expectedProfit
			else:
				if previousExpectedProfit <= expectedProfit:
					previousCandidateDelta,previousExpectedProfit = delta, expectedProfit
				else:
					return delta
			
			previousPriceLevel = priceLevel

		return delta
	

	def _getOptimalDeltas(self):
		
		optimalBidDelta = self._getOptimalDelta(Side.BID)
		optimalAskDelta = self._getOptimalDelta(Side.ASK)
		
		return optimalBidDelta,optimalAskDelta

	def _liquidate(self,side):
		if side == Side.BID:
			toBeFilledSize = self.toBeFilledBidSize
			bestPrice = self.limitOrderBook.bestBid().price
			currentOrderId = self.currentBidOrderId
		else:
			toBeFilledSize = self.toBeFilledAskSize
			bestPrice = self.limitOrderBook.bestAsk().price
			currentOrderId = self.currentAskOrderId

		if toBeFilledSize > 0:
			if currentOrderId is not None:	
				currentPriceLevel = self.limitOrderBook.orders[currentOrderId].price
				if currentPriceLevel == bestPrice:
					return
			
			self.orderSubmitter.cancelOrder(currentOrderId)
			if side == Side.BID:
				self.currentBidOrderId = self.orderSubmitter.addOrder(bestPrice, toBeFilledSize, side)
			else:
				self.currentAskOrderId = self.orderSubmitter.addOrder(bestPrice, toBeFilledSize, side)


	def _refreshOrder(self,side,):
		currentlyLiveOrder = None
		midPrice = self.limitOrderBook.mid()
		if side == Side.BID:
			currentOrderId = self.currentBidOrderId
			currentTotalSizeInFrontOfOrder = self.totalSizeInFrontOfBidOrder
			adjustmentFactor = -1
		elif side == Side.ASK:
			currentOrderId = self.currentAskOrderId
			currentTotalSizeInFrontOfOrder = self.totalSizeInFrontOfAskOrder
			adjustmentFactor = 1


		#If size in front of our order did not change do not replace the order
		if currentOrderId is not None:
			currentlyLiveOrder = self.limitOrderBook.orders[currentOrderId]
			totalSizeInFrontOfOrder = self.limitOrderBook.cumulativeVolumeAtPriceLevel(currentlyLiveOrder.price,side)
			if currentTotalSizeInFrontOfOrder == totalSizeInFrontOfOrder:
				return
			
		optimalDelta = self._getOptimalDelta(side)
		
		newOrderPrice = midPrice + adjustmentFactor*optimalDelta/2
		
		if side == Side.BID:
			self.totalSizeInFrontOfBidOrder = self.limitOrderBook.cumulativeVolumeAtPriceLevel(newOrderPrice,side)
		else:
			self.totalSizeInFrontOfAskOrder = self.limitOrderBook.cumulativeVolumeAtPriceLevel(newOrderPrice,side)
		
		#If the price of the order did not change, do not replace the order
		if currentlyLiveOrder is not None and newOrderPrice == currentlyLiveOrder.price:
			return

		if currentOrderId is not None:
			self.orderSubmitter.cancelOrder(currentOrderId)
		
		if side == Side.BID:
			self.currentBidOrderId = self.orderSubmitter.addOrder(newOrderPrice, self.toBeFilledBidSize, Side.BID)
		else:
			self.currentAskOrderId = self.orderSubmitter.addOrder(newOrderPrice, self.toBeFilledAskSize, Side.ASK)

	#Accepts an event and calls appropriate action
	#on OrderSubmitter (modifyOrder|addNewOrder)
	def accept(self,event):
		TradingAlgo.accept(self,event)
		self.currentTime = event.timestamp
		if self.startTime == None:
			self.startTime = self.currentTime

		#Don't do anything in the first hour to build the distribution
		#We will change this by using previous day's distribution but for now
		#we will do this.
		if self.currentTime - self.startTime < 3600000:
			return

		#Don't do anything if we don't have enough market orders to build a meaningful distribution
		buyMarketOrderDistribution = self.getMarketOrderDistributionForSide(Side.BID)
		sellMarketOrderDistribution = self.getMarketOrderDistributionForSide(Side.ASK)
		if buyMarketOrderDistribution.size() < 1000 or sellMarketOrderDistribution.size() < 1000:
			return

		if self.currentTime >= self.windowEnd:
			print ("Window ended: ")
			print (f"To be filled bid size: {self.toBeFilledBidSize}")
			print (f"To be filled ask size: {self.toBeFilledAskSize}")
			print ()

			#Reset the window
			self.windowStart = self.currentTime
			self.windowEnd = self.windowStart + self.windowLengthMillis

			if self.bidOrderFilled and not self.askOrderFilled:
				self.onlyBidFilled += 1
			elif not self.bidOrderFilled and self.askOrderFilled:
				self.onlyAskFilled += 1
			elif self.bidOrderFilled and self.askOrderFilled:
				self.bothFilled += 1

			self.totalWindows += 1

			if self.currentBidOrderId:
				self.orderSubmitter.cancelOrder(self.currentBidOrderId)
			if self.currentAskOrderId:
				self.orderSubmitter.cancelOrder(self.currentAskOrderId)

			self.currentBidOrderId = None
			self.currentAskOrderId = None
			self.bidOrderFilled = False
			self.askOrderFilled = False
			self.toBeFilledBidSize = 1000
			self.toBeFilledAskSize = 1000
			# print (f"Total money: {self.totalMoney}")
		else:
			#10 seconds or less left and we still have inventory.
			#Liquidate it
			if self.windowEnd - self.currentTime <= 10000:
				self._liquidate(Side.BID)
				self._liquidate(Side.ASK)
			else:
				if not self.bidOrderFilled:
					self._refreshOrder(Side.BID)
					
				if not self.askOrderFilled:
					self._refreshOrder(Side.ASK)
				

	def orderAckedCallback(self,operation,orderId):
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

