
from limit_order import LimitOrder, Side, Operation
from algo_limit_order import AlgoLimitOrder
from l3_limit_order_book import L3LimitOrderBook
from market_order import MarketOrder
from trading_algo import TradingAlgo
from pipe import Pipe
import copy

class MatchEngineSimulator(Pipe):

	def __init__(self, tradingAlgo, limitOrderBook):
		self.tradingAlgo = tradingAlgo
		self.limitOrderBook = limitOrderBook

	def accept(self,event):
		if type(event) == MarketOrder:
			orders = self.limitOrderBook.walkTheBook(event)
			for order in orders:
				if type(order) == AlgoLimitOrder:
					#Remove the hit/lifted limit order partially or fully from the book
					ourHitOrderOnBook = self.limitOrderBook.limitOrderBooks[order.symbol].orders[order.orderId]
					if ourHitOrderOnBook.size == order.size:
						#Our order is fully hit
						self.limitOrderBook.removeOrder(order.symbol, order.orderId)
					else:
						#Our order is partially hit
						ourHitOrderOnBook.operation = Operation.UPDATE
						ourHitOrderOnBook.size -= order.size
						self.limitOrderBook.updateOrder(ourHitOrderOnBook)

					#Notify the algo that the limit order is hit/lifted
					self.tradingAlgo.notifyMatchedCallback(event, order)

