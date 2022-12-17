import sys
import matplotlib.pyplot as plt

from market_order_aggregator import MarketOrderAggregator
from distribution import MarketOrderSizeDistribution
from market_order_reader import MarketOrderReader, MarketOrderSide
from pipeline import Pipeline


marketOrderFile = sys.argv[1]

marketOrderReader = MarketOrderReader(marketOrderFile, MarketOrderSide.BUY)
marketOrderAggregator = MarketOrderAggregator()
distribution = MarketOrderSizeDistribution()


p = Pipeline([marketOrderReader,marketOrderAggregator,distribution])
p.start()

n = 10000
print (f"Percentile of {n}")
print (distribution.getPercentileOfValue(n))
print (f"Total market orders: {len(distribution.data)}")
