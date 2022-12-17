
import sys
import matplotlib.pyplot as plt

from market_order_aggregator import MarketOrderAggregator
from distribution import Distribution, WindowedMarketOrderSizeDistribution
from market_order_reader import MarketOrderReader, MarketOrderSide
from pipeline import Pipeline
from window_maintainer import BackTestWindownMaintainer
from distribution import DistributionDataPoint

class MarketOrderRateCapture:
	def __init__(self):
		self.distribution = Distribution()

	def __call__(self,dataPoint):
		self.distribution.accept(DistributionDataPoint(distribution.dataRate(),0))

		


marketOrderFile = sys.argv[1]

marketOrderRateCapture = MarketOrderRateCapture()
windowLength = 60
marketOrderReader = MarketOrderReader(marketOrderFile, MarketOrderSide.SELL,marketOrderRateCapture)
marketOrderAggregator = MarketOrderAggregator()

global distribution
distribution = WindowedMarketOrderSizeDistribution(BackTestWindownMaintainer(windowLengthSeconds=windowLength))

p = Pipeline([marketOrderReader,marketOrderAggregator,distribution])
p.start()


distroAsList = marketOrderRateCapture.distribution.toList()
# print (f"Num data points in distro: {len(distroAsList)}")
plt.hist(distroAsList, bins='auto')
plt.xlim(0,10)
plt.ylim(0,5000)
plt.show()


