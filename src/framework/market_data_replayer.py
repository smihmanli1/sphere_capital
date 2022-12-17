
from pipe import Pipe
from limit_order import LimitOrder
from market_order import MarketOrder


class MarketDataReplayer(Pipe):

	def __init__(self,limitOrderReader,marketOrderReader):
		Pipe.__init__(self)
		self.limitOrderReader = limitOrderReader
		self.marketOrderReader = marketOrderReader

		self.nextEligibleLimitOrder = None
		self.nextEligibleMarketOrder = None

		self.haveLimitOrders = True
		self.haveMarketOrders = True

	
	def _produceMarketData(self):
		
		if self.haveLimitOrders and self.nextEligibleLimitOrder is None:
			self.haveLimitOrders = self.limitOrderReader.getNext()

		if self.haveMarketOrders and self.nextEligibleMarketOrder is None:
			self.haveMarketOrders = self.marketOrderReader.getNext()

		#We still have data to get from the sources. We can't produce data
		#until we get the necessary data.
		if (self.haveLimitOrders and self.nextEligibleLimitOrder is None) or (self.haveMarketOrders and self.nextEligibleMarketOrder is None): 
			return

		#No more data to send out.
		if not self.haveLimitOrders and not self.haveMarketOrders:
			return
		#No more limit orders to send out. Just send the market order at hand.
        #We know we have a market order because of the checks above 
		elif not self.haveLimitOrders:
			self.produce(self.nextEligibleMarketOrder)
			self.nextEligibleMarketOrder = None
		#No more market orders to send out. Just send the limit order at hand.
        #We know we have a limit order because of the checks above
		elif not self.haveMarketOrders:
			self.produce(self.nextEligibleLimitOrder)
			self.nextEligibleLimitOrder = None
		#We have both market order and limit order at hand.
		#Send the one with the lower timestamp out.
		else:
			if self.nextEligibleLimitOrder.timestamp < self.nextEligibleMarketOrder.timestamp:
				self.produce(self.nextEligibleLimitOrder)
				self.nextEligibleLimitOrder = None
			else:
				self.produce(self.nextEligibleMarketOrder)
				self.nextEligibleMarketOrder = None


	def accept(self,dataPoint):
		if type(dataPoint) == LimitOrder:
			self.nextEligibleLimitOrder = dataPoint
		elif type(dataPoint) == MarketOrder:
			self.nextEligibleMarketOrder = dataPoint


	def start(self):
		while self.haveLimitOrders or\
			  self.haveMarketOrders:
			self._produceMarketData()
					      		