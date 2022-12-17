
from pipe import Pipe



class TradingAlgo(Pipe):

	def __init__(self):
		pass

	def accept(self,event):
		#This is where the algo takes action
		pass

	def orderAckedCallback(self,operation,orderId):
		pass

	def orderRejectedCallback(self,orderId,side):
		pass

	def notifyFilledCallback(self, matchedMarketOrder, algoOrder):
		pass
