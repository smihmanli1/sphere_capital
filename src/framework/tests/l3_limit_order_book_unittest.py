import unittest
from l3_limit_order_book import L3LimitOrderBook, L3LimitOrderBookIterator
from limit_order import LimitOrder, Side, Operation
from market_order import MarketOrder
from quote import Quote
import sys

class TestL3LimitOrderBook(unittest.TestCase):
	#				Price			Size		OrderId
	#				90				20				9
	#				90				15				7
	#				90				10				1
	#				92				55				8
	#				92				20				3
	#				92				45				2
	#				95				20				4
	# BIDS
	#	----------------------------------------------------
	# ASKS			Price			Size		OrderId
	#				100				10				6
	#				100				30				11
	#				102.5			100				12
	#				103				30				13
	#				103				20				14
	#				103				10				15
	#				105				10				5
	#				105				20				10
	def _generateEssentialOrderBook(self):
		orderBook = L3LimitOrderBook()

		orderBook.addOrder(LimitOrder("AAPL", "1",90,10,Side.BID,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "2",92,45,Side.BID,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "3",92,20,Side.BID,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "4",95,20,Side.BID,0, Operation.ADD))
		
		orderBook.addOrder(LimitOrder("AAPL", "5",105,10,Side.ASK,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "6",100,10,Side.ASK,0, Operation.ADD))
		
		orderBook.addOrder(LimitOrder("AAPL", "7",90,15,Side.BID,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "8",92,55,Side.BID,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "9",90,20,Side.BID,0, Operation.ADD))

		orderBook.addOrder(LimitOrder("AAPL", "10",105,20,Side.ASK,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "11",100,30,Side.ASK,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "12",102.5,100,Side.ASK,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "13",103,30,Side.ASK,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "14",103,20,Side.ASK,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "15",103,10,Side.ASK,0, Operation.ADD))

		orderBook.snapshotComplete = True

		return orderBook

	def _generateOnlyAsksBook(self):

		orderBook = L3LimitOrderBook()
		
		orderBook.addOrder(LimitOrder("AAPL", "5",105,10,Side.ASK,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "6",100,10,Side.ASK,0, Operation.ADD))
		
		orderBook.addOrder(LimitOrder("AAPL", "10",105,20,Side.ASK,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "11",100,30,Side.ASK,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "12",102.5,100,Side.ASK,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "13",103,30,Side.ASK,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "14",103,20,Side.ASK,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "15",103,10,Side.ASK,0, Operation.ADD))

		orderBook.snapshotComplete = True

		return orderBook

	def _generateOnlyBidsBook(self):

		orderBook = L3LimitOrderBook()

		orderBook.addOrder(LimitOrder("AAPL", "1",90,10,Side.BID,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "2",92,45,Side.BID,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "3",92,20,Side.BID,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "4",95,20,Side.BID,0, Operation.ADD))
		
		orderBook.addOrder(LimitOrder("AAPL", "7",90,15,Side.BID,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "8",92,55,Side.BID,0, Operation.ADD))
		orderBook.addOrder(LimitOrder("AAPL", "9",90,20,Side.BID,0, Operation.ADD))

		orderBook.snapshotComplete = True

		return orderBook

	def testSingleLevel(self):
		orderBook = L3LimitOrderBook()
		limit_order1 = LimitOrder("AAPL", "1",90,10,Side.BID,0, Operation.ADD)
		limit_order3 = LimitOrder("AAPL", "3",105,10,Side.ASK,0, Operation.ADD)
		orderBook.addOrder(limit_order1)
		orderBook.addOrder(limit_order3)
		orderBook.snapshotComplete = True
		self.assertEqual(orderBook.spread(), 15)

	def testFullBookCumulativeSize(self):
		
		orderBook = self._generateEssentialOrderBook()

		self.assertEqual(orderBook.spread(), 5)
		
		#BID SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(90, Side.BID), 45)
		self.assertEqual(orderBook.volumeAtPriceLevel(95, Side.BID), 20)
		self.assertEqual(orderBook.volumeAtPriceLevel(92, Side.BID), 120)
		
		self.assertEqual(orderBook.volumeAtPriceLevel(90, Side.ASK), 0)
		self.assertEqual(orderBook.volumeAtPriceLevel(95, Side.ASK), 0)

		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(93, Side.BID), 20)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95, Side.BID), 20)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95.1, Side.BID), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(91, Side.BID), 140)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(90, Side.BID), 185)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(89.8, Side.BID), 185)

		#ASK SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(103, Side.ASK), 60)
		self.assertEqual(orderBook.volumeAtPriceLevel(102.5, Side.ASK), 100)
		self.assertEqual(orderBook.volumeAtPriceLevel(100, Side.ASK), 40)
		
		self.assertEqual(orderBook.volumeAtPriceLevel(103, Side.BID), 0)
		self.assertEqual(orderBook.volumeAtPriceLevel(102.5, Side.BID), 0)
		self.assertEqual(orderBook.volumeAtPriceLevel(100, Side.BID), 0)

		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(104, Side.ASK), 200)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(103, Side.ASK), 200)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(102.51, Side.ASK), 140)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100.01, Side.ASK), 40)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100, Side.ASK), 40)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(99, Side.ASK), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105, Side.ASK), 230)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105.2, Side.ASK), 230)

	def testHalfSpread(self):
		orderBook = self._generateEssentialOrderBook()
		self.assertEqual(orderBook.halfSpread(), 2.5)

	def testBestBidAsk(self):
		orderBook = self._generateEssentialOrderBook()
		self.assertEqual(orderBook.bestBid(), Quote(95,20))
		self.assertEqual(orderBook.bestAsk(), Quote(100,40))

	def testRemoveBestBidOrder(self):

		orderBook = self._generateEssentialOrderBook()
		orderBook.removeOrder("4")

		self.assertEqual(orderBook.spread(), 8)
		
		#BID SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(90, Side.BID), 45)
		self.assertEqual(orderBook.volumeAtPriceLevel(95, Side.BID), 0)

		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(93, Side.BID), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95, Side.BID), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95.1, Side.BID), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(91, Side.BID), 120)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(90, Side.BID), 165)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(89.8, Side.BID), 165)

		#ASK SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(103, Side.ASK), 60)
		self.assertEqual(orderBook.volumeAtPriceLevel(102.5, Side.ASK), 100)
		self.assertEqual(orderBook.volumeAtPriceLevel(100, Side.ASK), 40)

		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(104, Side.ASK), 200)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(103, Side.ASK), 200)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(102.51, Side.ASK), 140)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100.01, Side.ASK), 40)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100, Side.ASK), 40)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(99, Side.ASK), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105, Side.ASK), 230)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105.2, Side.ASK), 230)

		self.assertEqual(orderBook.bestBid(), Quote(92,120))

	def testRemoveAnyBidOrder(self):

		orderBook = self._generateEssentialOrderBook()
		orderBook.removeOrder("2")
		
		self.assertEqual(orderBook.spread(), 5)
		
		#BID SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(90, Side.BID), 45)
		self.assertEqual(orderBook.volumeAtPriceLevel(95, Side.BID), 20)
		self.assertEqual(orderBook.volumeAtPriceLevel(92, Side.BID), 75)

		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(93, Side.BID), 20)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95, Side.BID), 20)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95.1, Side.BID), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(91, Side.BID), 95)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(90, Side.BID), 140)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(89.8, Side.BID), 140)

		#ASK SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(103, Side.ASK), 60)
		self.assertEqual(orderBook.volumeAtPriceLevel(102.5, Side.ASK), 100)
		self.assertEqual(orderBook.volumeAtPriceLevel(100, Side.ASK), 40)

		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(104, Side.ASK), 200)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(103, Side.ASK), 200)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(102.51, Side.ASK), 140)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100.01, Side.ASK), 40)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100, Side.ASK), 40)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(99, Side.ASK), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105, Side.ASK), 230)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105.2, Side.ASK), 230)

	def testRemoveBestAskOrder(self):

		orderBook = self._generateEssentialOrderBook()
		orderBook.removeOrder("6")

		self.assertEqual(orderBook.spread(), 5)
		
		#BID SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(90, Side.BID), 45)
		self.assertEqual(orderBook.volumeAtPriceLevel(95, Side.BID), 20)
		self.assertEqual(orderBook.volumeAtPriceLevel(92, Side.BID), 120)

		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(93, Side.BID), 20)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95, Side.BID), 20)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95.1, Side.BID), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(91, Side.BID), 140)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(90, Side.BID), 185)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(89.8, Side.BID), 185)

		#ASK SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(103, Side.ASK), 60)
		self.assertEqual(orderBook.volumeAtPriceLevel(102.5, Side.ASK), 100)
		self.assertEqual(orderBook.volumeAtPriceLevel(100, Side.ASK), 30)

		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(104, Side.ASK), 190)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(103, Side.ASK), 190)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(102.51, Side.ASK), 130)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100.01, Side.ASK), 30)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100, Side.ASK), 30)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(99, Side.ASK), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105, Side.ASK), 220)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105.2, Side.ASK), 220)

		self.assertEqual(orderBook.bestAsk(), Quote(100,30))

	def testRemoveAnyAskOrder(self):
		
		orderBook = self._generateEssentialOrderBook()
		orderBook.removeOrder("13")

		self.assertEqual(orderBook.spread(), 5)
		
		#BID SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(90, Side.BID), 45)
		self.assertEqual(orderBook.volumeAtPriceLevel(95, Side.BID), 20)
		self.assertEqual(orderBook.volumeAtPriceLevel(92, Side.BID), 120)

		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(93, Side.BID), 20)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95, Side.BID), 20)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95.1, Side.BID), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(91, Side.BID), 140)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(90, Side.BID), 185)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(89.8, Side.BID), 185)

		#ASK SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(103, Side.ASK), 30)
		self.assertEqual(orderBook.volumeAtPriceLevel(102.5, Side.ASK), 100)
		self.assertEqual(orderBook.volumeAtPriceLevel(100, Side.ASK), 40)

		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(104, Side.ASK), 170)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(103, Side.ASK), 170)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(102.51, Side.ASK), 140)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100.01, Side.ASK), 40)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100, Side.ASK), 40)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(99, Side.ASK), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105, Side.ASK), 200)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105.2, Side.ASK), 200)


	def testUpdateBestBidOrder(self):

		orderBook = self._generateEssentialOrderBook()
		orderBook.updateOrder(LimitOrder("AAPL", "4",95,5,Side.BID,0, Operation.UPDATE))

		self.assertEqual(orderBook.spread(), 5)
		
		#BID SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(90, Side.BID), 45)
		self.assertEqual(orderBook.volumeAtPriceLevel(95, Side.BID), 5)
		self.assertEqual(orderBook.volumeAtPriceLevel(92, Side.BID), 120)

		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(93, Side.BID), 5)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95, Side.BID), 5)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95.1, Side.BID), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(91, Side.BID), 125)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(90, Side.BID), 170)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(89.8, Side.BID), 170)

		#ASK SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(103, Side.ASK), 60)
		self.assertEqual(orderBook.volumeAtPriceLevel(102.5, Side.ASK), 100)
		self.assertEqual(orderBook.volumeAtPriceLevel(100, Side.ASK), 40)


		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(104, Side.ASK), 200)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(103, Side.ASK), 200)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(102.51, Side.ASK), 140)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100.01, Side.ASK), 40)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100, Side.ASK), 40)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(99, Side.ASK), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105, Side.ASK), 230)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105.2, Side.ASK), 230)


	def testUpdateAnyBidOrder(self):
		
		orderBook = self._generateEssentialOrderBook()
		
		orderBook.updateOrder(LimitOrder("AAPL", "3",92,15,Side.BID,0, Operation.UPDATE))

		#BID SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(90, Side.BID), 45)
		self.assertEqual(orderBook.volumeAtPriceLevel(95, Side.BID), 20)
		self.assertEqual(orderBook.volumeAtPriceLevel(92, Side.BID), 115)

		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(93, Side.BID), 20)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95, Side.BID), 20)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95.1, Side.BID), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(91, Side.BID), 135)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(90, Side.BID), 180)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(89.8, Side.BID), 180)

		#ASK SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(103, Side.ASK), 60)
		self.assertEqual(orderBook.volumeAtPriceLevel(102.5, Side.ASK), 100)
		self.assertEqual(orderBook.volumeAtPriceLevel(100, Side.ASK), 40)

		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(104, Side.ASK), 200)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(103, Side.ASK), 200)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(102.51, Side.ASK), 140)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100.01, Side.ASK), 40)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100, Side.ASK), 40)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(99, Side.ASK), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105, Side.ASK), 230)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105.2, Side.ASK), 230)


	def testUpdateBestAskOrder(self):

		orderBook = self._generateEssentialOrderBook()
		orderBook.updateOrder(LimitOrder("AAPL", "6",100,15,Side.ASK,0, Operation.UPDATE))

		self.assertEqual(orderBook.spread(), 5)
		
		#BID SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(90, Side.BID), 45)
		self.assertEqual(orderBook.volumeAtPriceLevel(95, Side.BID), 20)
		self.assertEqual(orderBook.volumeAtPriceLevel(92, Side.BID), 120)

		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(93, Side.BID), 20)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95, Side.BID), 20)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95.1, Side.BID), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(91, Side.BID), 140)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(90, Side.BID), 185)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(89.8, Side.BID), 185)

		#ASK SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(103, Side.ASK), 60)
		self.assertEqual(orderBook.volumeAtPriceLevel(102.5, Side.ASK), 100)
		self.assertEqual(orderBook.volumeAtPriceLevel(100, Side.ASK), 45)

		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(104, Side.ASK), 205)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(103, Side.ASK), 205)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(102.51, Side.ASK), 145)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100.01, Side.ASK), 45)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100, Side.ASK), 45)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(99, Side.ASK), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105, Side.ASK), 235)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105.2, Side.ASK), 235)


	def testUpdateAnyAskOrder(self):

		orderBook = self._generateEssentialOrderBook()
		orderBook.updateOrder(LimitOrder("AAPL", "13",103,70,Side.ASK,0, Operation.UPDATE))

		self.assertEqual(orderBook.spread(), 5)
		
		#BID SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(90, Side.BID), 45)
		self.assertEqual(orderBook.volumeAtPriceLevel(95, Side.BID), 20)
		self.assertEqual(orderBook.volumeAtPriceLevel(92, Side.BID), 120)

		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(93, Side.BID), 20)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95, Side.BID), 20)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95.1, Side.BID), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(91, Side.BID), 140)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(90, Side.BID), 185)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(89.8, Side.BID), 185)

		#ASK SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(103, Side.ASK), 100)
		self.assertEqual(orderBook.volumeAtPriceLevel(102.5, Side.ASK), 100)
		self.assertEqual(orderBook.volumeAtPriceLevel(100, Side.ASK), 40)

		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(104, Side.ASK), 240)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(103, Side.ASK), 240)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(102.51, Side.ASK), 140)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100.01, Side.ASK), 40)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100, Side.ASK), 40)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(99, Side.ASK), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105, Side.ASK), 270)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105.2, Side.ASK), 270)

	def testDecrementBidOrder(self):

		orderBook = self._generateEssentialOrderBook()
		orderBook.decrement("4",5)

		self.assertEqual(orderBook.spread(), 5)
		
		#BID SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(90, Side.BID), 45)
		self.assertEqual(orderBook.volumeAtPriceLevel(95, Side.BID), 15)
		self.assertEqual(orderBook.volumeAtPriceLevel(92, Side.BID), 120)

		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(93, Side.BID), 15)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95, Side.BID), 15)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95.1, Side.BID), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(91, Side.BID), 135)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(90, Side.BID), 180)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(89.8, Side.BID), 180)

		#ASK SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(103, Side.ASK), 60)
		self.assertEqual(orderBook.volumeAtPriceLevel(102.5, Side.ASK), 100)
		self.assertEqual(orderBook.volumeAtPriceLevel(100, Side.ASK), 40)

		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(104, Side.ASK), 200)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(103, Side.ASK), 200)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(102.51, Side.ASK), 140)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100.01, Side.ASK), 40)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100, Side.ASK), 40)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(99, Side.ASK), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105, Side.ASK), 230)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105.2, Side.ASK), 230)


	def testDecrementAskOrder(self):

		orderBook = self._generateEssentialOrderBook()
		orderBook.decrement("6",5)

		self.assertEqual(orderBook.spread(), 5)
		
		#BID SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(90, Side.BID), 45)
		self.assertEqual(orderBook.volumeAtPriceLevel(95, Side.BID), 20)
		self.assertEqual(orderBook.volumeAtPriceLevel(92, Side.BID), 120)

		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(93, Side.BID), 20)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95, Side.BID), 20)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(95.1, Side.BID), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(91, Side.BID), 140)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(90, Side.BID), 185)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(89.8, Side.BID), 185)

		#ASK SIDE TESTS
		self.assertEqual(orderBook.volumeAtPriceLevel(103, Side.ASK), 60)
		self.assertEqual(orderBook.volumeAtPriceLevel(102.5, Side.ASK), 100)
		self.assertEqual(orderBook.volumeAtPriceLevel(100, Side.ASK), 35)

		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(104, Side.ASK), 195)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(103, Side.ASK), 195)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(102.51, Side.ASK), 135)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100.01, Side.ASK), 35)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(100, Side.ASK), 35)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(99, Side.ASK), 0)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105, Side.ASK), 225)
		self.assertEqual(orderBook.cumulativeVolumeAtPriceLevel(105.2, Side.ASK), 225)

	def _limitOrderEqual(self, l1, l2, msg=None):
		res = l1.orderId == l2.orderId and l1.size == l2.size and l1.price == l2.price and l1.side == l2.side
		return res

	def testWalkTheBook(self):

		orderBook = self._generateEssentialOrderBook()
		# self.addTypeEqualityFunc(LimitOrder, self._limitOrderEqual)

		#				Price			Size		OrderId
		#				90				20				9
		#				90				15				7
		#				90				10				1
		#				92				55				8
		#				92				20				3
		#				92				45				2
		#				95				20				4
		# BIDS
		#	----------------------------------------------------
		# ASKS			Price			Size		OrderId
		#				100				10				6
		#				100				30				11
		#				102.5			100				12
		#				103				30				13
		#				103				20				14
		#				103				10				15
		#				105				10				5
		#				105				20				10

		# Test buy market order
		buyMarketOrder1 = MarketOrder(orderId="30", sell=False, price=0, size=50, timestamp=10)

		# This should pick up order 6 and some of order 11 (20 size)
		expectedMatchedOrders = []
		expectedMatchedOrders.append(LimitOrder("AAPL", "6",100,10,Side.ASK,0, Operation.ADD))
		expectedMatchedOrders.append(LimitOrder("AAPL", "11",100,30,Side.ASK,0, Operation.ADD))
		expectedMatchedOrders.append(LimitOrder("AAPL", "12",102.5,10,Side.ASK,0, Operation.ADD))
		actualMatchedOrders = orderBook.walkTheBook(buyMarketOrder1)
		self.assertListEqual(expectedMatchedOrders,actualMatchedOrders)
		
		#Test sell market order
		sellMarketOrder1 = MarketOrder(orderId="90", sell=True, price=0, size=90, timestamp=10)

		# This should pick up order 6 and some of order 11 (20 size)
		expectedMatchedOrders = []
		expectedMatchedOrders.append(LimitOrder("AAPL", "4",95,20,Side.BID,0, Operation.ADD))
		expectedMatchedOrders.append(LimitOrder("AAPL", "2",92,45,Side.BID,0, Operation.ADD))
		expectedMatchedOrders.append(LimitOrder("AAPL", "3",92,20,Side.BID,0, Operation.ADD))
		expectedMatchedOrders.append(LimitOrder("AAPL", "8",92,5,Side.BID,0, Operation.ADD))
		actualMatchedOrders = orderBook.walkTheBook(sellMarketOrder1)

		self.assertListEqual(expectedMatchedOrders,actualMatchedOrders)		

	def testEmptyOrderBook(self):

		orderBook = L3LimitOrderBook()
		self.assertRaises(Exception, orderBook.spread)
		self.assertRaises(Exception, orderBook.halfSpread)
		self.assertRaises(Exception, orderBook.bestBid)
		self.assertRaises(Exception, orderBook.bestAsk)


	def testEmptyBook(self):

		orderBook = L3LimitOrderBook()

		self.assertRaises(IndexError, orderBook.spread)
		self.assertRaises(IndexError, orderBook.halfSpread)
		self.assertRaises(IndexError, orderBook.bestBid)
		self.assertRaises(IndexError, orderBook.bestAsk)


	def testOnlyAsksBook(self):

		orderBook = self._generateOnlyAsksBook()
		
		self.assertRaises(IndexError, orderBook.spread)
		self.assertRaises(IndexError, orderBook.halfSpread)
		self.assertRaises(IndexError, orderBook.bestBid)

		self.assertEqual(orderBook.bestAsk(), Quote(100,40))		

	def testOnlyBidsBook(self):

		orderBook = self._generateOnlyBidsBook()
		
		self.assertRaises(IndexError, orderBook.spread)
		self.assertRaises(IndexError, orderBook.halfSpread)
		self.assertRaises(IndexError, orderBook.bestAsk)

		self.assertEqual(orderBook.bestBid(), Quote(95,20))		

	def testAddExistingOrderId(self):

		orderBook = self._generateEssentialOrderBook()
		self.assertRaises(Exception,orderBook.addOrder,LimitOrder("AAPL", "1",90,10,Side.BID,0, Operation.ADD))
	
	
	def testDecrementNonexistentSize(self):

		orderBook = self._generateEssentialOrderBook()
		self.assertRaises(Exception,orderBook.decrement,"4",1000)
		

	def testLockedBook(self):
		
		orderBook = self._generateEssentialOrderBook()
		lockingOrder = LimitOrder("AAPL", "16",100,20,Side.BID,0, Operation.ADD)
		result = orderBook.addOrder(lockingOrder)
		self.assertFalse(result)
		self.assertTrue(orderBook.validBook())

		orderBook = self._generateEssentialOrderBook()
		lockingOrder = LimitOrder("AAPL", "16",95,20,Side.ASK,0, Operation.ADD)
		result = orderBook.addOrder(lockingOrder)
		self.assertFalse(result)
		self.assertTrue(orderBook.validBook())

	def testCrossedBook(self):
		orderBook = self._generateEssentialOrderBook()
		crossingOrder = LimitOrder("AAPL", "16",101,20,Side.BID,0, Operation.ADD)
		result = orderBook.addOrder(crossingOrder)
		self.assertFalse(result)
		self.assertTrue(orderBook.validBook())

		orderBook = self._generateEssentialOrderBook()
		crossingOrder = LimitOrder("AAPL", "16",94,20,Side.ASK,0, Operation.ADD)
		result = orderBook.addOrder(crossingOrder)
		self.assertFalse(result)
		self.assertTrue(orderBook.validBook())

	def testOrderBookIterator(self):
		orderBook = self._generateEssentialOrderBook()
		bidsIterator = L3LimitOrderBookIterator(orderBook.bids,Side.BID)
	
		self.assertEqual(orderBook.orders[bidsIterator.getNextBestOrder()], LimitOrder("AAPL", "4",95,20,Side.BID,0, Operation.ADD))
		self.assertEqual(orderBook.orders[bidsIterator.getNextBestOrder()], LimitOrder("AAPL", "2",92,45,Side.BID,0, Operation.ADD))
		self.assertEqual(orderBook.orders[bidsIterator.getNextBestOrder()], LimitOrder("AAPL", "3",92,20,Side.BID,0, Operation.ADD))
		self.assertEqual(orderBook.orders[bidsIterator.getNextBestOrder()], LimitOrder("AAPL", "8",92,55,Side.BID,0, Operation.ADD))
		self.assertEqual(orderBook.orders[bidsIterator.getNextBestOrder()], LimitOrder("AAPL", "1",90,10,Side.BID,0, Operation.ADD))
		self.assertEqual(orderBook.orders[bidsIterator.getNextBestOrder()], LimitOrder("AAPL", "7",90,15,Side.BID,0, Operation.ADD))
		self.assertEqual(orderBook.orders[bidsIterator.getNextBestOrder()], LimitOrder("AAPL", "9",90,20,Side.BID,0, Operation.ADD))
		self.assertEqual(bidsIterator.getNextBestOrder(), None)

		asksIterator = L3LimitOrderBookIterator(orderBook.asks,Side.ASK)
		self.assertEqual(orderBook.orders[asksIterator.getNextBestOrder()], LimitOrder("AAPL", "6",100,10,Side.ASK,0, Operation.ADD))
		self.assertEqual(orderBook.orders[asksIterator.getNextBestOrder()], LimitOrder("AAPL", "11",100,30,Side.ASK,0, Operation.ADD))
		self.assertEqual(orderBook.orders[asksIterator.getNextBestOrder()], LimitOrder("AAPL", "12",102.5,100,Side.ASK,0, Operation.ADD))
		self.assertEqual(orderBook.orders[asksIterator.getNextBestOrder()], LimitOrder("AAPL", "13",103,30,Side.ASK,0, Operation.ADD))
		self.assertEqual(orderBook.orders[asksIterator.getNextBestOrder()], LimitOrder("AAPL", "14",103,20,Side.ASK,0, Operation.ADD))
		self.assertEqual(orderBook.orders[asksIterator.getNextBestOrder()], LimitOrder("AAPL", "15",103,10,Side.ASK,0, Operation.ADD))
		self.assertEqual(orderBook.orders[asksIterator.getNextBestOrder()], LimitOrder("AAPL", "5",105,10,Side.ASK,0, Operation.ADD))
		self.assertEqual(orderBook.orders[asksIterator.getNextBestOrder()], LimitOrder("AAPL", "10",105,20,Side.ASK,0, Operation.ADD))
		self.assertEqual(asksIterator.getNextBestOrder(), None)

	def testPriceLevel(self):
		orderBook = self._generateEssentialOrderBook()
		
		expectedBidPriceLevels = [95,92,90]
		expectedAskPriceLevels = [100,102.5,103,105]

		self.assertEqual(orderBook.getPriceLevels(Side.BID, 30), expectedBidPriceLevels)
		self.assertEqual(orderBook.getPriceLevels(Side.ASK, 30), expectedAskPriceLevels)
		

if __name__ == '__main__':
	unittest.main()
