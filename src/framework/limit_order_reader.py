
from pipe import Pipe
from limit_order import LimitOrder, Side

class LimitOrderReader(Pipe):

	def __init__(self,limitOrderDict):
		self.limitOrderDict = limitOrderDict

		#It's extremely important that this size factor is the same as the one in market order reader
		self.sizeFactor = 10000
		self.counter = 0


	def _processRow(self,row):
		self.counter += 1
		newLimitOrder = LimitOrder(
			orderId=self.counter,
			price=float(row[2]),
			size=float(row[3])*self.sizeFactor,
			side=Side.BID if row[1] == 'b' else Side.ASK,
			timestamp=int(row[0]))

		self.produce(newLimitOrder)

	def start(self):
		
		for row in self.limitOrderDict:
			self.counter += 1
			newLimitOrder = LimitOrder(
				orderId=self.counter,
				price=float(row[2]),
				size=float(row[3])*self.sizeFactor,
				side=Side.BID if row[1] == 'b' else Side.ASK,
				timestamp=int(row[0]),
				isPartOfSnapshot = True)

			self.produce(newLimitOrder)
		

