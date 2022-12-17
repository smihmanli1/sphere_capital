from framework.limit_order import Side

class MarketOrder:
	@staticmethod
	def getOtherSide(marketOrder):
		if (marketOrder.sell):
			return Side.BID
		else:
			return Side.ASK

	def __init__(self, symbol=None, orderId = 0, sell = False, price = 0, size = 0, timestamp = 0):
		self.symbol = symbol
		self.orderId = orderId
		self.sell = sell
		self.size = size
		self.timestamp = timestamp

	def __str__(self):
		return f"Market Order: {{ Timestamp: {self.timestamp}, Size: {self.size}, Sell: {self.sell}, orderId: {self.orderId} }}"

	def __add__(self,other):
		returned = other
		returned.size += self.size
		return returned