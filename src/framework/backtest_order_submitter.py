
from framework.limit_order import LimitOrder, Side, Operation
from framework.algo_limit_order import AlgoLimitOrder
from framework.pipe import Pipe

class OrderSubmitter(Pipe):
	def __init__(self):
		pass

	def modifyOrder(self,orderId):
		raise Exception("Not implemented")

	def addNewOrder(self,price,size):
		raise Exception("Not implemented")

class BacktestOrderSubmitter(OrderSubmitter):

	def __init__(self, lob, orderDelayMs = 0):
		self.orderCounter = 1;
		self.orderDelayMs = orderDelayMs
		self.outgoingOrderQueue = []
		self.limitOrderBook = lob
		self.orderAckedListeners = []

	def _enqueueAddOrder(self,addOrder):
		self.outgoingOrderQueue.append(addOrder)

	def _enqueueRemoveOrder(self,orderId):
		removeOrder = AlgoLimitOrder(orderId=orderId, timestamp=self.currentTime, operation = Operation.DELETE)
		self.outgoingOrderQueue.append(removeOrder)

	def _sendEligibleOrders(self, currentTime):
		while len(self.outgoingOrderQueue) > 0:
			limitOrder = self.outgoingOrderQueue[0]
			if limitOrder.timestamp + self.orderDelayMs <= currentTime:
				if limitOrder.operation == Operation.ADD:
					if self.limitOrderBook.addOrder(limitOrder) == True:
						for listener in self.orderAckedListeners:
							listener.orderAckedCallback(Operation.ADD,limitOrder.orderId)
					else:
						for listener in self.orderAckedListeners:
							listener.orderRejectedCallback(limitOrder.orderId, limitOrder.side)	
						
				elif limitOrder.operation == Operation.DELETE:
					self.limitOrderBook.removeOrder(limitOrder.orderId)
					for listener in self.orderAckedListeners:
						listener.orderAckedCallback(Operation.DELETE,limitOrder.orderId)
				del self.outgoingOrderQueue[0]

			else:
				#Container is sorted in time. If we are here then no other
				#order is ready to go yet.
				break

	def accept(self,event):
		self.currentTime = event.timestamp
		self._sendEligibleOrders(event.timestamp)


	def cancelOrder(self,orderId):
		self._enqueueRemoveOrder(orderId)

	def addOrder(self,price,size,side):
		self.orderCounter += 1
		newOrderId = self.orderCounter
		newLimitOrder = AlgoLimitOrder(orderId=newOrderId,price=price,size=size,side=side, timestamp=self.currentTime, operation = Operation.ADD)
		self._enqueueAddOrder(newLimitOrder)
		return newOrderId


	def registerOrderAckedListener(self, listener):
		self.orderAckedListeners.append(listener)


	def sendMarketOrder(self, marketOrder):
		self.limitOrderBook.applyMarketOrder(marketOrder)





