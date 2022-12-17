
from pipe import Pipe

from market_order import MarketOrder

# A market order could match multiple limit orders. This stage constructs the market order,
# by looking at the matches.
# Example:
#      sell limit order 2: 2 shares
#      sell limit order 2: 3 shares
#      buy market order 1: 5 shares
#
#  Receives 4 events:
#      2 shares, limit order id 1
#      3 shares, limit order id 2
#      2 shares, market order id 1
#      3 shares, market order id 1
#  if onlyMarketOrder:
#       Produces 1 event:
#			market order id 1: 5 shares
#  otherwise:
#		Produces 3 events:
#			limit order id 2: 3 shares
#			limit order id 3: 2 shares
#           market order id 1: 5 shares
#      
#
# Consumes: Limit order and matched market order
# Produces: Market order constructed
class MarketOrderAggregator(Pipe):
	# sell: is sell market order or buy?
	# onlyMarketOrder: only send generated market order? or send generated market order and limit orders
	def __init__(self,sell = None, onlyMarketOrder = False):
		Pipe.__init__(self)
		self.sell = sell
		self.onlyMarketOrder = onlyMarketOrder
		self.previousOrder = None

	def _timestampClose(self,timestamp1,timestamp2):
		return timestamp1 == timestamp2

	def accept(self, event):

		if type(event) != MarketOrder:
			if not self.onlyMarketOrder:
				self.produce(event)

			return

		marketOrder = event

		if self.sell is True and not marketOrder.sell or\
		   self.sell is False and marketOrder.sell:
		   return

		if self.previousOrder is None:
			self.previousOrder = marketOrder
			return

		if self.previousOrder.sell == marketOrder.sell and \
		   (self.previousOrder.orderId == marketOrder.orderId or \
		   self._timestampClose(self.previousOrder.timestamp,marketOrder.timestamp)):
		   
		   self.previousOrder = self.previousOrder + marketOrder
		  
		else:
			self.produce(self.previousOrder)
			self.previousOrder = marketOrder
