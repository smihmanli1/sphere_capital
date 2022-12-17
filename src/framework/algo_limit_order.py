from limit_order import LimitOrder, Side, Operation

class AlgoLimitOrder(LimitOrder):

	def __init__(self,orderId = 0,price = 0,size = 0,side = None,timestamp = 0, operation = Operation.ADD):
		LimitOrder.__init__(self, orderId, price, size, side, timestamp, operation)

	def __str__(self):
		return f"Algo Limit Order: {{ timestamp: {self.timestamp}, price: {self.price}, size: {self.size}, side: {self.side}, order_id: {self.orderId}, operation: {self.operation}  }}"

