
from sortedcontainers import SortedDict 

from pipe import Pipe
from quote import Quote
from limit_order import LimitOrder, Side, Operation


class LimitOrderBook(Pipe):

	def __init__(self):
		self.bids = SortedDict()
		self.asks = SortedDict()

	def _addOrderToSide(self,limitOrder,bookOfSide):
		bookOfSide.setdefault(limitOrder.price,0)
		bookOfSide[limitOrder.price] += limitOrder.size

	def _updateOrderOnSide(self,limitOrder,bookOfSide):
		if bookOfSide.get(limitOrder.price,None) == None:
			print ("This price level does not exist!")
			return

		if limitOrder.size == 0:
			self.clearPriceLevel(limitOrder.price, bookOfSide)
		else: 
			bookOfSide[limitOrder.price] = limitOrder.size

	def _removeVolumeAtLevel(self,priceLevel,removedVolume,bookOfSide):
		volume = bookOfSide.get(priceLevel,None)
		if volume == None:
			return

		if volume == removedVolume:
			self.clearPriceLevel(priceLevel,bookOfSide)
		elif volume > removedVolume:
			bookOfSide[priceLevel] -= removedVolume
		else:
			print ("Trying to remove volume that does not exist. Ignoring operation.")
		
	# Behaviour is undefined if the price level does not already
	# exist in 'bookOfSide'
	def clearPriceLevel(self,price,bookOfSide):
		index = bookOfSide.bisect_right(price)
		bookOfSide.popitem(index=index)

	def walkTheBook(size,side):
		#TODO: Implement
		pass


	def accept(self,limitOrder):
		if type(limitOrder) != LimitOrder:
			return

		if limitOrder.operation == Operation.ADD:
			self.addOrder(limitOrder)
		elif limitOrder.operation == Operation.UPDATE:
			self.updateOrder(limitOrder)

	# Add an order at that price level
	def addOrder(self,limitOrder):

		if limitOrder.side == Side.BID:
			self._addOrderToSide(limitOrder,self.bids)
		elif limitOrder.side == Side.ASK:
			self._addOrderToSide(limitOrder,self.asks)
		else:
			raise Exception("Unexpected order side being added to limit order book")

	# Update the size to this limitOrder's size for this limitOrder's price level
	# If the new size is 0, that price level is removed from the book.
	def updateOrder(self,limitOrder):
		if limitOrder.side == Side.BID:
			self._updateOrderOnSide(limitOrder,self.bids)
		elif limitOrder.side == Side.ASK:
			self._updateOrderOnSide(limitOrder,self.asks)
		else:
			raise Exception("Unexpected order side being updated on the limit order book")		

	def removeVolumeAtPriceLevel(self, priceLevel, removedVolume, side):
		if side == Side.BID:
			self._removeVolumeAtLevel(priceLevel,removedVolume,self.bids)
		elif side == Side.ASK:
			self._removeVolumeAtLevel(priceLevel,removedVolume,self.asks)
		else:
			raise Exception("Unexpected order side being updated on the limit order book")


	def bestBid(self):
		bestBidQuote = self.bids.peekitem(index=-1)
		return Quote(bestBidQuote[0],bestBidQuote[1])

	def bestAsk(self):
		bestAskQuote = self.asks.peekitem(index=0)
		return Quote(bestAskQuote[0],bestAskQuote[1])

	def spread(self):
		return self.bestAsk().price - self.bestBid().price

	def halfSpread(self):
		return self.spread()/2

	def volumeAtPriceLevel(self,priceLevel,side):
		if side == Side.BID:
			return self.bids.get(priceLevel,0)
		elif side == Side.ASK:
			return self.asks.get(priceLevel,0)
		else:
			raise Exception("Unexpected order side being updated on the limit order book")

	#This is an O(n) algo. This can be improved to be O(log(n)) but this requires a 
	#a special binary search tree implementation (that keeps track of volume for items less than/greater than).
	def cumulativeVolumeAtPriceLevel(self,priceLevel,side):
		cumulativeSize = 0
		if side == Side.BID:
			iterableBook = reversed(self.bids)
			book = self.bids
		elif side == Side.ASK:
			iterableBook = self.asks
			book = self.asks
		else:
			raise Exception("Unexpected order side being added to limit order book")

		for currentPriceLevel in iterableBook:
			if currentPriceLevel > priceLevel and side == Side.ASK or \
			   currentPriceLevel < priceLevel and side == Side.BID:
			   break

			cumulativeSize += book[currentPriceLevel]

		return cumulativeSize





