
import csv
from enum import Enum

from market_order import MarketOrder
from pipe import Pipe

class MarketOrderSide(Enum):
	BUY = 1
	SELL = 2

class MarketOrderReader(Pipe):

	#If side is not specified, it will provide all market orders
	def __init__(self,marketOrderFile,marketOrderSide = None, flowCallback = None):
		Pipe.__init__(self,flowCallback)

		#It's extremely important that this size factor is the same as the one in limit order reader
		self.sizeFactor = 10000
		
		self.marketOrderSide = marketOrderSide
		self.csvfile = open(marketOrderFile)
		self.marketOrderReader = csv.DictReader(self.csvfile, delimiter=',', quotechar='|') 

	def _processRow(self,row):

		newMarketOrder = MarketOrder(
			orderId=int(row['id']),
			sell=True if row['sell'] == 'true' else False,
			price=float(row['price']),
			size=float(row['amount'])*self.sizeFactor,
			timestamp=int(row['date']))

		if self.marketOrderSide == None or \
		   (self.marketOrderSide == MarketOrderSide.BUY and newMarketOrder.sell == False) or \
		   (self.marketOrderSide == MarketOrderSide.SELL and newMarketOrder.sell == True):
			self.produce(newMarketOrder)

	def start(self):			
		for row in self.marketOrderReader:
			self._processRow(row)
				
	def getNext(self):
		currentRow = next(self.marketOrderReader,None)
		if currentRow:
			self._processRow(currentRow)
			return True
		else:
			return False





				

				