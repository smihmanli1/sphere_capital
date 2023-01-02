
from index_weights import getWeights, getUnitWeightForTicker
import datetime
import pandas as pd

class Index:

    def __init__(self, name, weightsDir):

        self.name = name
        
        try:
            self.weightsDf = getWeights(name, weightsDir)
        except FileNotFoundError:
            self.weightsDf = getUnitWeightForTicker(name)

    def getDistribution(self, distributionDate):
        weightsDataFrameDatesFormat = '%m/%d/%y'
        
        relevantWeightsDistribution = None
        returnedWeightsDate = None

        #Find the distribution as of the given date
        for col in self.weightsDf:
            if col == "Name":
                continue
            colParsedToDate = datetime.datetime.strptime(col, weightsDataFrameDatesFormat).date()
            if colParsedToDate < distributionDate:
                returnedWeightsDate = col
                relevantWeightsDistribution = self.weightsDf[["Name",col]]

        returned = dict(zip(relevantWeightsDistribution['Name'],relevantWeightsDistribution[returnedWeightsDate]))
        return returned





def addIndexColumn(index, pricesDf):
    
    #Calculate index price
    pricesDate = pricesDf["time"].iat[0].date()
    indexDistribution = index.getDistribution(pricesDate)
    newColumn = pd.Series([0 for i in range(len(pricesDf.index))])
    for ticker,weight in indexDistribution.items():
        
        if ticker in pricesDf:
            newColumn += newColumn + weight * pricesDf[ticker]
        else:
            print (f"Ticker {ticker} is missing for {index.name} on date {pricesDate}. Treating its price as 0")

    pricesDf[index.name] = newColumn

    return pricesDf