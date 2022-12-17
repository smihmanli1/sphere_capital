
import time_constants

class WindowMaintainer:

	def __init__(self, windowLengthSeconds):
		self.windowLengthMillis = windowLengthSeconds * time_constants.millisInSecond

	# Returns data points per second
	def dataRate(self,data,dataTimeSorted):
		return len(data)/(self.windowLengthMillis/time_constants.millisInSecond)

	# Maintains data that is within a time window of 'windowLengthSeconds' of data.
	# Timestamps in dataTimeSorted should be milliseconds since epoch in UTC
	def maintainWindow(self,data,dataTimeSorted):
		raise Exception("WindowMaintainer: Unimplemented function maintainWindow")		

# Maintains window by considering last data point's timestamp as 'now'
# This is in contrast to RealtimeWindowMaintainer which maintains window by
# considering current time as 'now'
class BackTestWindownMaintainer(WindowMaintainer):

	def __init__(self, windowLengthSeconds):
		WindowMaintainer.__init__(self,windowLengthSeconds)
		
	def maintainWindow(self,data,dataTimeSorted):
		cutoffPointTime = dataTimeSorted.peekitem(index=-1)[0] - self.windowLengthMillis
		cutOffPointIndex = dataTimeSorted.bisect_right(cutoffPointTime)

		for i in range(0,cutOffPointIndex):
			removedDataPoints = dataTimeSorted.peekitem(index=0)[1]
			for removedDataPoint in removedDataPoints:
				data.pop(removedDataPoint.dataPointId)
			dataTimeSorted.popitem(index=0)	

		return data,dataTimeSorted


# Maintains window by considering current time as 'now'
# This is in contrast to BackTestWindownMaintainer which maintains window by
# considering last data point's timestamp as 'now'
class RealtimeWindowMaintainer(WindowMaintainer):

	def __init__(self, windowLengthSeconds):
		WindowMaintainer.__init__(windowLengthSeconds)

	def dataRate(self,data,dataTimeSorted):
		raise Exception("RealtimeWindowMaintainer: Unimplemented function dataRate")
		
	def maintainWindow(self,data,dataTimeSorted):
		raise Exception("RealtimeWindowMaintainer: Unimplemented function maintainWindow")