import numpy as np
import bisect

from pipe import Pipe
from sortedcontainers import SortedDict 

class DistributionDataPoint:

	def __init__(self,data,timestamp):
		self.data = data
		self.timestamp = timestamp
		self.dataPointId = 0

class Distribution(Pipe):

	def __init__(self, windowMaintainer = None):
		Pipe.__init__(self)
		self.data = {} # data point id->DistributionDataPoint
		self.dataTimeSorted = SortedDict() #data point timestamp->list{DistributionDataPoint}
		
		self.windowMaintainer = windowMaintainer
		self.idCounter = 0

	def accept(self,dataPoint):
		dataPoint.dataPointId = self.idCounter
		self.data[self.idCounter] = dataPoint
		self.idCounter+=1

		self.dataTimeSorted.setdefault(dataPoint.timestamp,[])
		self.dataTimeSorted[dataPoint.timestamp].append(dataPoint)
 
	def dataRate(self):
		raise Exception("Distribution: Unimplemented function dataRate")

	def getPercentileOfValue(self,value):	
		data = self.toList()
		data.sort()
		return bisect.bisect(data,value)/len(data)

	def toList(self):
		return [dataPoint.data for dataPoint in self.data.values()]


	def mean(self):
		dataPointsList = self.toList()
		return np.mean(dataPointsList)
		
	def stddev(self):
		dataPointsList = self.toList()
		return np.std(dataPointsList)

	def size(self):
		return len(self.data)

	def sum(self):
		return sum(self.data)

class MarketOrderSizeDistribution(Distribution):

	def __init__(self):
		Distribution.__init__(self)

	def accept(self,marketOrder):
		dp = DistributionDataPoint(marketOrder.size,marketOrder.timestamp)
		Distribution.accept(self,dp)


#IMPORTANT NOTE: In C++, window maintainer would be owned by the Distribution class.
#                It would have access to data and dataTimeSorted by reference.
#                Since we cannot do this in Python, we had to do this weird inheritance.
class WindowedMarketOrderSizeDistribution(MarketOrderSizeDistribution):

	def __init__(self,windowMaintainer):
		MarketOrderSizeDistribution.__init__(self)
		self.windowMaintainer = windowMaintainer

	def accept(self,marketOrder):
		MarketOrderSizeDistribution.accept(self,marketOrder)
		self.data,self.dataTimeSorted = self.windowMaintainer.maintainWindow(self.data,self.dataTimeSorted)

	#This will be called in the future by an event loop when we are processing
	#real-time data. This will ensure that a window is maintained even if 'accept' is not
	#called for a long period of time.
	def periodicInvoke(self):
		self.data,self.dataTimeSorted = self.windowMaintainer.maintainWindow(self.data,self.dataTimeSorted)


	def dataRate(self):
		return self.windowMaintainer.dataRate(self.data,self.dataTimeSorted)
