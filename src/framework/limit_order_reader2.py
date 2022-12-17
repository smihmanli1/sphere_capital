
import csv

from pipe import Pipe
from limit_order import LimitOrder, Side

class LimitOrderReader2(Pipe):

	def __init__(self,limitOrderFile):
		Pipe.__init__(self)
		#It's extremely important that this size factor is the same as the one in limit order reader
		self.sizeFactor = 10000
		
		self.counter = 0
		self.csvfile = open(limitOrderFile)
		self.limitOrderReader = csv.DictReader(self.csvfile, delimiter=',', quotechar='|') 


	def _processRow(self,row):
		self.counter += 1
		newLimitOrder = LimitOrder(
			orderId=self.counter,
			price=float(row['price']),
			size=float(row['amount'])*self.sizeFactor,
			side=Side.BID if row['type'] == 'b' else Side.ASK,
			timestamp=int(row['date']),
			isPartOfSnapshot = True) #TODO: That will depend on the row

		self.produce(newLimitOrder)

	def start(self):
		for row in self.limitOrderReader:
			self._processRow(row)

	def getNext(self):
		currentRow = next(self.limitOrderReader,None)
		if currentRow:
			self._processRow(currentRow)
			return True
		else:
			return False
		

