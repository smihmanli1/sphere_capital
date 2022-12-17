
import numpy as np
import math
from limit_order import Side
from sklearn.linear_model import LinearRegression


# Market order size distribution should reflect only the given 'side'.
# It cannot have both sides. The calculated k will be the k for that side
def _dataPointsToFit(
	marketOrderSizeDistribution,
	limitOrderBook,
	side,
	maxDepthInCalculationDollars,
	probability_threshold = 0.01):
	tickSize = 0.01
	bestQuote = None
	if side == Side.BID:
		bestQuotePrice = limitOrderBook.bestBid().price
		stepFactor = -1
	elif side == Side.ASK:
		bestQuotePrice = limitOrderBook.bestAsk().price
		stepFactor = 1
	else:
		raise Exception("Unexpected book side")

	dataPoints = []
	midPrice = bestQuotePrice - stepFactor*limitOrderBook.halfSpread()
	
	previousCumulativeVolumeAtCurrentLevel = -1
	
	for testedPriceLevel in np.arange(midPrice,midPrice + stepFactor*maxDepthInCalculationDollars, stepFactor*tickSize):
		cumulativeVolumeAtCurrentLevel = limitOrderBook.cumulativeVolumeAtPriceLevel(testedPriceLevel,side)
		
		if cumulativeVolumeAtCurrentLevel == previousCumulativeVolumeAtCurrentLevel:
			continue

		previousCumulativeVolumeAtCurrentLevel = cumulativeVolumeAtCurrentLevel
		
		probabilityOfFillAtThisLevel = 1 - marketOrderSizeDistribution.getPercentileOfValue(cumulativeVolumeAtCurrentLevel)
		depthFromMid = abs(testedPriceLevel-midPrice)
		
		#If the probability of fill is 0, we stop here because this probability being 0 is hard to
		#fit into our model P{proability of order at this level being fileed} = e^-kx
		if probabilityOfFillAtThisLevel < probability_threshold:
			break
		
		dataPoints.append((depthFromMid,probabilityOfFillAtThisLevel))
	
	return dataPoints


# def _calculateBestFit(dataPoints):
# 	x = []
# 	w = []
# 	n = len(dataPoints)
# 	for xi,wi in dataPoints:
# 		x.append(xi)
# 		w.append(wi)

# 	nominator_termOne = np.dot(x,w)
# 	nominator_termTwo = (1/n) * sum(x) * sum(w)
# 	denominator_termOne = np.dot(x,x)
# 	denominator_termTwo = (1/n) * pow(sum(x),2)

# 	b = (nominator_termOne-nominator_termTwo)/(denominator_termOne - denominator_termTwo)

# 	a = (1/n) * sum(w) - (1/n) * b * sum(x)

# 	rawDataPoints = [(x,math.exp(y)) for x,y in dataPoints]
# 	return (a,b,rawDataPoints)

def _calculateBestFit(dataPoints):
	x = []
	w = []
	n = len(dataPoints)
	for xi,yi in dataPoints:
		x.append(xi)
		w.append(np.log(yi))

	x = np.array(x).reshape((-1, 1))
	w = np.array(w)

	model = LinearRegression(fit_intercept=False)
	model.fit(x,w)

	return (math.exp(model.intercept_), model.coef_, dataPoints)

def calculate_k(marketOrderSizeDistribution,limitOrderBook,side,maxDepthInCalculationDollars):
	dataPoints = _dataPointsToFit(marketOrderSizeDistribution,limitOrderBook,side,maxDepthInCalculationDollars)
	return _calculateBestFit(dataPoints)





