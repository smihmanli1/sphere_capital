import unittest
from ..backtest_order_submitter import BacktestOrderSubmitter
from ..l3_limit_order_book import L3LimitOrderBook
from ..limit_order import LimitOrder, Side, Operation
from ..trading_algo import TradingAlgo

class TestTradingAlgo(TradingAlgo):

	def __init__(self,orderSubmitter):
		self.submittedOrderIds = set()
		self.cancelledOrderIds = set()
		orderSubmitter.registerOrderSubmittedListener(self)
		
	#Accepts an event and calls appropriate action
	#on OrderSubmitter (modifyOrder|addNewOrder)
	def accept(self,event):
		TradingAlgo.accept(self,event)

	def orderSubmittedCallback(self,operation,orderId):
		if operation == Operation.ADD:
			self.submittedOrderIds.add(orderId)
		elif operation == Operation.DELETE:
			self.cancelledOrderIds.add(orderId)
			

class TestBacktestOrderSubmitter(unittest.TestCase):

	def testDelayedAdd(self):
		limitOrderBook = L3LimitOrderBook()
		orderSubmitter = BacktestOrderSubmitter(lob = limitOrderBook,orderDelayMs = 5)
		tradingAlgo = TestTradingAlgo(orderSubmitter)
		
		incomingLimitOrderFromExchange = LimitOrder(orderId="1", price = 10, size=100, side = Side.BID, timestamp = 1000, operation=Operation.ADD)
		orderSubmitter.accept(incomingLimitOrderFromExchange)
		limitOrderBook.accept(incomingLimitOrderFromExchange)
		
		#Enter our limit order
		ourOrderId = orderSubmitter.addOrder(price = 10.1, size=100, side = Side.BID)

		#Check that our limit order is not yet in the book
		self.assertEqual(limitOrderBook.orders.get(ourOrderId), None)
		self.assertEqual(limitOrderBook.volumeAtPriceLevel(10.1, Side.BID), 0)
		

		#Another order comes in but before our order reaches the exchange
		incomingLimitOrderFromExchange2 = LimitOrder(orderId="2", price = 9, size=100, side = Side.BID, timestamp = 1002, operation=Operation.ADD)
		orderSubmitter.accept(incomingLimitOrderFromExchange2)
		limitOrderBook.accept(incomingLimitOrderFromExchange2)


		#Check that our limit order is still not yet in the book
		self.assertEqual(limitOrderBook.orders.get(ourOrderId), None)
		self.assertEqual(limitOrderBook.volumeAtPriceLevel(10.1, Side.BID), 0)


		#Another order comes in but after our order reaches the exchange
		incomingLimitOrderFromExchange3 = LimitOrder(orderId="3", price = 9, size=200, side = Side.BID, timestamp = 1007, operation=Operation.ADD)
		orderSubmitter.accept(incomingLimitOrderFromExchange3)
		
		#At this point  our limit order should be in the book
		self.assertEqual(limitOrderBook.volumeAtPriceLevel(10.1, Side.BID), 100)
		self.assertTrue(ourOrderId in tradingAlgo.submittedOrderIds)
		limitOrderBook.accept(incomingLimitOrderFromExchange3)


	def testDelayedRemove(self):
		limitOrderBook = L3LimitOrderBook()
		orderSubmitter = BacktestOrderSubmitter(lob = limitOrderBook,orderDelayMs = 5)
		tradingAlgo = TestTradingAlgo(orderSubmitter)
		
		incomingLimitOrderFromExchange = LimitOrder(orderId="1", price = 10, size=100, side = Side.BID, timestamp = 1000, operation=Operation.ADD)
		orderSubmitter.accept(incomingLimitOrderFromExchange)
		limitOrderBook.accept(incomingLimitOrderFromExchange)
		
		#Enter our limit order
		ourOrderId = orderSubmitter.addOrder(price = 10.1, size=100, side = Side.BID)

		#Check that our limit order is not yet in the book
		self.assertEqual(limitOrderBook.orders.get(ourOrderId), None)
		self.assertEqual(limitOrderBook.volumeAtPriceLevel(10.1, Side.BID), 0)
		

		#Another order comes in but before our order reaches the exchange
		incomingLimitOrderFromExchange2 = LimitOrder(orderId="2", price = 9, size=100, side = Side.BID, timestamp = 1002, operation=Operation.ADD)
		orderSubmitter.accept(incomingLimitOrderFromExchange2)
		limitOrderBook.accept(incomingLimitOrderFromExchange2)


		#Check that our limit order is still not yet in the book
		self.assertEqual(limitOrderBook.orders.get(ourOrderId), None)
		self.assertEqual(limitOrderBook.volumeAtPriceLevel(10.1, Side.BID), 0)


		#Another order comes in but after our order reaches the exchange
		incomingLimitOrderFromExchange3 = LimitOrder(orderId="3", price = 9, size=200, side = Side.BID, timestamp = 1007, operation=Operation.ADD)
		orderSubmitter.accept(incomingLimitOrderFromExchange3)
		#At this point  our limit order should be in the book
		self.assertEqual(limitOrderBook.volumeAtPriceLevel(10.1, Side.BID), 100)
		self.assertTrue(ourOrderId in tradingAlgo.submittedOrderIds)		
		limitOrderBook.accept(incomingLimitOrderFromExchange3)


		#Remove the limit order that we added
		orderSubmitter.cancelOrder(ourOrderId)

		#Another order comes in but before our cancellation reaches the exchange
		incomingLimitOrderFromExchange4 = LimitOrder(orderId="4", price = 9, size=200, side = Side.BID, timestamp = 1010, operation=Operation.ADD)
		orderSubmitter.accept(incomingLimitOrderFromExchange4)
		limitOrderBook.accept(incomingLimitOrderFromExchange4)

		#At this point  our limit order should still be in the book
		self.assertEqual(limitOrderBook.volumeAtPriceLevel(10.1, Side.BID), 100)


		#Another order comes in but after our order cancellation reaches the exchange
		incomingLimitOrderFromExchange5 = LimitOrder(orderId="5", price = 9, size=200, side = Side.BID, timestamp = 1013, operation=Operation.ADD)
		orderSubmitter.accept(incomingLimitOrderFromExchange5)
		#At this point  our limit order should no longer be in the book
		self.assertEqual(limitOrderBook.volumeAtPriceLevel(10.1, Side.BID), 0)
		self.assertTrue(ourOrderId in tradingAlgo.cancelledOrderIds)			
		limitOrderBook.accept(incomingLimitOrderFromExchange5)


	def testDelayedAddThenRemoveBeforeDelay(self):
		limitOrderBook = L3LimitOrderBook()
		orderSubmitter = BacktestOrderSubmitter(lob = limitOrderBook,orderDelayMs = 5)
		
		incomingLimitOrderFromExchange = LimitOrder(orderId="1", price = 10, size=100, side = Side.BID, timestamp = 1000, operation=Operation.ADD)
		orderSubmitter.accept(incomingLimitOrderFromExchange)
		limitOrderBook.accept(incomingLimitOrderFromExchange)
		
		#Enter our limit order
		ourOrderId = orderSubmitter.addOrder(price = 10.1, size=100, side = Side.BID)

		#Check that our limit order is not yet in the book
		self.assertEqual(limitOrderBook.orders.get(ourOrderId), None)
		self.assertEqual(limitOrderBook.volumeAtPriceLevel(10.1, Side.BID), 0)
		

		#Another order comes in but before our order reaches the exchange
		incomingLimitOrderFromExchange2 = LimitOrder(orderId="2", price = 9, size=100, side = Side.BID, timestamp = 1002, operation=Operation.ADD)
		orderSubmitter.accept(incomingLimitOrderFromExchange2)
		limitOrderBook.accept(incomingLimitOrderFromExchange2)

		# Remove the limit order that we added
		orderSubmitter.cancelOrder(ourOrderId)

		#Check that our limit order is still not yet in the book
		self.assertEqual(limitOrderBook.orders.get(ourOrderId), None)
		self.assertEqual(limitOrderBook.volumeAtPriceLevel(10.1, Side.BID), 0)


		#Another order comes in but after our add order reaches the exchange
		incomingLimitOrderFromExchange3 = LimitOrder(orderId="3", price = 9, size=200, side = Side.BID, timestamp = 1006, operation=Operation.ADD)
		orderSubmitter.accept(incomingLimitOrderFromExchange3)
		#At this point  our limit order should be in the book
		self.assertEqual(limitOrderBook.volumeAtPriceLevel(10.1, Side.BID), 100)		
		limitOrderBook.accept(incomingLimitOrderFromExchange3)


		#Another order comes in but after our cancellation reaches the exchange
		incomingLimitOrderFromExchange4 = LimitOrder(orderId="4", price = 9, size=200, side = Side.BID, timestamp = 1010, operation=Operation.ADD)
		orderSubmitter.accept(incomingLimitOrderFromExchange4)
		#At this point  our limit order should no longer be in the book
		self.assertEqual(limitOrderBook.volumeAtPriceLevel(10.1, Side.BID), 0)
		limitOrderBook.accept(incomingLimitOrderFromExchange4)


if __name__ == '__main__':
    unittest.main()
		