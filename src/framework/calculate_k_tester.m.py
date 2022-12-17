import sys
import mmap
import math
import numpy as np
import matplotlib.pyplot as plt

import k_calculator
from market_order_aggregator import MarketOrderAggregator
from distribution import MarketOrderSizeDistribution
from market_order_reader import MarketOrderReader, MarketOrderSide
from pipeline import Pipeline
from limit_order import Side
from limit_order_book import LimitOrderBook
from limit_order_reader import LimitOrderReader


marketOrderFile = sys.argv[1]

marketOrderReader = MarketOrderReader(marketOrderFile, MarketOrderSide.SELL)
marketOrderAggregator = MarketOrderAggregator()
buyMarketOrderDistribution = MarketOrderSizeDistribution()

p = Pipeline([marketOrderReader,marketOrderAggregator,buyMarketOrderDistribution])
p.start()



snapshotFileName = sys.argv[2]
currentOrdersList = []

spreadCounter = 0
previousDate = 0
calculatedSpreads = []
counter = 0
with open(snapshotFileName) as csvfile:
	m = mmap.mmap(csvfile.fileno(), 0, prot=mmap.PROT_READ)
	currentRow = m.readline()
	while currentRow:
		currentRow = currentRow.decode("UTF-8")[:-1]
		
		currentRowSplit = currentRow.split(',')
		if (currentRowSplit[0] != 'date'):
			
			currentDate = int(currentRowSplit[0])
			if currentDate != previousDate and previousDate != 0:
				orderReader = LimitOrderReader(currentOrdersList)
				limitOrderBook = LimitOrderBook()
				orderPipeline = Pipeline([orderReader, limitOrderBook])
				orderPipeline.start()

				currentOrdersList = []

				intercept,k,dataPoints = k_calculator.calculate_k(
					marketOrderSizeDistribution=buyMarketOrderDistribution,
					limitOrderBook=limitOrderBook,
					side=Side.BID,
					maxDepthInCalculationDollars=20.0)
				
				print (counter)
				if len(dataPoints) > 4 and counter not in [1,12]:
					x = []
					y = []
					for xi,yi in dataPoints:
						x.append(xi)
						y.append(yi)

					bestFitLineX = np.arange(0,12,0.01)
					bestFitLineY = [intercept * math.exp(k * xi) for xi in bestFitLineX] 

					plt.plot(bestFitLineX,bestFitLineY)
					plt.scatter(x, y)
					plt.xlabel('x')
					plt.ylabel('y')
					# plt.xlim([0,13])
					# plt.ylim([-10,0])
					plt.show()

					print (dataPoints)
					print (k)
					#Breaking here to calculate only 1 k (only with the first snapshot)
				
					break
				counter += 1
				# if counter == 6:
				# 	break

			currentOrdersList.append(currentRowSplit)
			previousDate = currentDate

		currentRow = m.readline()
