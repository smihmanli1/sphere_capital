import unittest
from algo_limit_order import AlgoLimitOrder
from match_engine_simulator import MatchEngineSimulator
from market_order import MarketOrder
from limit_order import LimitOrder, Side, Operation
from l3_limit_order_book import L3LimitOrderBook
from backtest_order_submitter import BacktestOrderSubmitter
from trading_algo import TradingAlgo
from coinbase_md_reader import CoinbaseMdReader
from unittest.mock import patch

class MockTradingAlgo(TradingAlgo):
	def __init__(self):
		TradingAlgo.__init__(self)
	def setExpectedMatchedOrder(order):
		pass

	def verifyMatchedOrder(self, unittest, actual):
		pass

class TestMatchEngineSimulator(unittest.TestCase):
	
	def _generateEssentialOrderBook(self):
		orderBook = L3LimitOrderBook()

		orderBook.addOrder(LimitOrder("1",90,10,Side.BID,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("2",92,45,Side.BID,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("3",92,20,Side.BID,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("4",95,20,Side.BID,0, Operation.ADD))
		
		orderBook.addOrder(LimitOrder("5",105,10,Side.ASK,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("6",100,10,Side.ASK,0, Operation.ADD))
		
		orderBook.addOrder(LimitOrder("7",90,15,Side.BID,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("8",92,55,Side.BID,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("9",90,20,Side.BID,0, Operation.ADD))

		orderBook.addOrder(LimitOrder("10",105,20,Side.ASK,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("11",100,30,Side.ASK,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("12",102.5,100,Side.ASK,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("13",103,30,Side.ASK,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("14",103,20,Side.ASK,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("15",103,10,Side.ASK,0, Operation.ADD))

		orderBook.snapshotComplete = True

		return orderBook

	@patch("trading_algo.TradingAlgo", autospec=True)
	def testMatchEngineAlgoOrderMatchedFullySell(self, mockTradingAlgo):
		
		limitOrderBook = self._generateEssentialOrderBook()
		matchEngine = MatchEngineSimulator(mockTradingAlgo, limitOrderBook)

		limitOrderBook.addOrder(AlgoLimitOrder(orderId="16", price=94, size=10, side=Side.BID, timestamp=1))

		matchedMarketOrder = MarketOrder(orderId="17", sell=True, price=0, size=55, timestamp=2)
		matchEngine.accept(matchedMarketOrder)

		self.assertEqual(limitOrderBook.orders.get("16"), None)


	@patch("trading_algo.TradingAlgo", autospec=True)
	def testMatchEngineAlgoOrderMatchedPartiallySell(self, mockTradingAlgo):
		
		limitOrderBook = self._generateEssentialOrderBook()
		matchEngine = MatchEngineSimulator(mockTradingAlgo, limitOrderBook)

		limitOrderBook.addOrder(AlgoLimitOrder(orderId="16", price=94, size=100, side=Side.BID, timestamp=1))

		matchedMarketOrder = MarketOrder(orderId="17", sell=True, price=0, size=55, timestamp=2)
		matchEngine.accept(matchedMarketOrder)

		actualLimitOrder = limitOrderBook.orders["16"]
		expectedLimitOrder = AlgoLimitOrder(orderId="16", price=94, size=65, side=Side.BID, timestamp=1)
		self.assertEqual(actualLimitOrder, expectedLimitOrder)


	@patch("trading_algo.TradingAlgo", autospec=True)
	def testMatchEngineAlgoOrderMatchedFullyBuy(self, mockTradingAlgo):
		
		limitOrderBook = self._generateEssentialOrderBook()
		matchEngine = MatchEngineSimulator(mockTradingAlgo, limitOrderBook)

		limitOrderBook.addOrder(AlgoLimitOrder(orderId="16", price=101, size=30, side=Side.ASK, timestamp=1))

		matchedMarketOrder = MarketOrder(orderId="17", sell=False, price=0, size=70, timestamp=2)
		matchEngine.accept(matchedMarketOrder)

		self.assertEqual(limitOrderBook.orders.get("16"), None)


	@patch("trading_algo.TradingAlgo", autospec=True)
	def testMatchEngineAlgoOrderMatchedPartiallyBuy(self, mockTradingAlgo):
		
		limitOrderBook = self._generateEssentialOrderBook()
		matchEngine = MatchEngineSimulator(mockTradingAlgo, limitOrderBook)

		limitOrderBook.addOrder(AlgoLimitOrder(orderId="16", price=101, size=30, side=Side.ASK, timestamp=1))

		matchedMarketOrder = MarketOrder(orderId="17", sell=False, price=0, size=60, timestamp=2)
		matchEngine.accept(matchedMarketOrder)

		actualLimitOrder = limitOrderBook.orders["16"]
		expectedLimitOrder = AlgoLimitOrder(orderId="16", price=101, size=10, side=Side.ASK, timestamp=1)
		self.assertEqual(actualLimitOrder, expectedLimitOrder)

	@patch("trading_algo.TradingAlgo", autospec=True)
	def testMatchEngineAlgoUncrossBookTestAsk1(self, mockTradingAlgo):

		limitOrderBook = self._generateEssentialOrderBook()
		matchEngine = MatchEngineSimulator(mockTradingAlgo, limitOrderBook)
		
		#Algo adds a limit order
		limitOrderBook.addOrder(AlgoLimitOrder(orderId="16", price=96, size=30, side=Side.ASK, timestamp=1))
		
		incomingLimitOrder = LimitOrder("17", 99, 20, Side.BID, 0, Operation.ADD)
		limitOrderBook.accept(incomingLimitOrder)
		matchEngine.accept(incomingLimitOrder)

		self.assertEqual(limitOrderBook.orders.get("17"),None)
		self.assertEqual(limitOrderBook.orders["16"],AlgoLimitOrder(orderId="16", price=96, size=10, side=Side.ASK, timestamp=1))
		self.assertListEqual(limitOrderBook.crossedAskOrders, [])
		self.assertListEqual(limitOrderBook.crossedBidOrders, [])
		self.assertTrue(limitOrderBook.validBook())


	@patch("trading_algo.TradingAlgo", autospec=True)
	def testMatchEngineAlgoUncrossBookTestAsk2(self, mockTradingAlgo):

		limitOrderBook = self._generateEssentialOrderBook()
		matchEngine = MatchEngineSimulator(mockTradingAlgo, limitOrderBook)
		
		#Algo adds a limit order
		limitOrderBook.addOrder(AlgoLimitOrder(orderId="16", price=96, size=30, side=Side.ASK, timestamp=1))
		
		incomingLimitOrder = LimitOrder("17", 99, 40, Side.BID, 0, Operation.ADD)
		limitOrderBook.accept(incomingLimitOrder)
		matchEngine.accept(incomingLimitOrder)

		self.assertEqual(limitOrderBook.orders.get("17"),LimitOrder("17", 99, 10, Side.BID, 0, Operation.ADD))
		self.assertEqual(limitOrderBook.orders.get("16"),None)
		self.assertListEqual(limitOrderBook.crossedAskOrders, [])
		self.assertListEqual(limitOrderBook.crossedBidOrders, [])
		self.assertTrue(limitOrderBook.validBook())


	@patch("trading_algo.TradingAlgo", autospec=True)
	def testMatchEngineAlgoUncrossBookTestBid1(self, mockTradingAlgo):

		limitOrderBook = self._generateEssentialOrderBook()
		matchEngine = MatchEngineSimulator(mockTradingAlgo, limitOrderBook)
		
		#Algo adds a limit order
		limitOrderBook.addOrder(AlgoLimitOrder(orderId="16", price=98, size=30, side=Side.BID, timestamp=1))
		
		incomingLimitOrder = LimitOrder("17", 97, 20, Side.ASK, 0, Operation.ADD)
		limitOrderBook.accept(incomingLimitOrder)
		matchEngine.accept(incomingLimitOrder)

		self.assertEqual(limitOrderBook.orders.get("17"),None)
		self.assertEqual(limitOrderBook.orders["16"],AlgoLimitOrder(orderId="16", price=98, size=10, side=Side.BID, timestamp=1))
		self.assertListEqual(limitOrderBook.crossedAskOrders, [])
		self.assertListEqual(limitOrderBook.crossedBidOrders, [])
		self.assertTrue(limitOrderBook.validBook())


	@patch("trading_algo.TradingAlgo", autospec=True)
	def testMatchEngineAlgoUncrossBookTestBid2(self, mockTradingAlgo):

		limitOrderBook = self._generateEssentialOrderBook()
		matchEngine = MatchEngineSimulator(mockTradingAlgo, limitOrderBook)
		
		#Algo adds a limit order
		limitOrderBook.addOrder(AlgoLimitOrder(orderId="16", price=98, size=30, side=Side.BID, timestamp=1))
		
		incomingLimitOrder = LimitOrder("17", 97, 40, Side.ASK, 0, Operation.ADD)
		limitOrderBook.accept(incomingLimitOrder)
		matchEngine.accept(incomingLimitOrder)

		self.assertEqual(limitOrderBook.orders.get("17"),LimitOrder("17", 97, 10, Side.ASK, 0, Operation.ADD))
		self.assertEqual(limitOrderBook.orders.get("16"),None)
		self.assertListEqual(limitOrderBook.crossedAskOrders, [])
		self.assertListEqual(limitOrderBook.crossedBidOrders, [])
		self.assertTrue(limitOrderBook.validBook())

		
if __name__ == '__main__':
    unittest.main()

