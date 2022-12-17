

class Pipe:

	def __init__(self, flowCallback = None):
		self.nextModule = None
		self.flowCallback = flowCallback

	def accept(self,dataPoint):
		raise Exception("Abstract class pipe not implemented")

	def produce(self,dataPoint):
		if self.nextModule is not None:
			if type(self.nextModule) == list:
				for nextPipe in self.nextModule:
					nextPipe.accept(dataPoint)
			else:
				self.nextModule.accept(dataPoint)

		if self.flowCallback is not None:
			self.flowCallback(dataPoint)

	def start(self):
		pass


	def getNext(self):
		pass

	def _setNextModule(self,nextModule):
		self.nextModule = nextModule
