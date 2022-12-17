
from enum import Enum

class Side(Enum):
	BID = 1
	ASK = 2

class Operation(Enum):
	ADD = 1 #OPEN message in Coinbase format
	UPDATE = 2 #CHANGE message in Coinbase format
	DECREASE = 3
	CLEAR = 4
	SNAPSHOT_COMPLETE = 5
	DELETE = 6

class LimitOrder:

	def __init__(self,symbol = None, orderId = 0,price = 0,size = 0,side = None,timestamp = 0, operation = Operation.ADD, session = ""):

		self.symbol = symbol
		self.orderId = orderId
		self.price = price
		self.size = size
		self.side = side
		self.timestamp = timestamp
		self.operation = operation
		self.session = session


	def __str__(self):
		return f"Limit Order: {{ timestamp: {self.timestamp}, symbol: {self.symbol}, price: {self.price}, size: {self.size}, side: {self.side}, order_id: {self.orderId}, operation: {self.operation}  }}"

	def __eq__(self, other):
		return self.orderId == other.orderId and \
			self.price == other.price and \
			self.size == other.size and \
			self.side == other.side

