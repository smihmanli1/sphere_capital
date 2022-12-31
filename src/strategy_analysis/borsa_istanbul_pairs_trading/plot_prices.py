import sys
import glob
import pandas as pd
import datetime

import plotly.graph_objects as go
import plotly.subplots as ps
import math
import plotly.express as px 

from index_weights import getWeights, getUnitWeightForTicker

class Index:

    def __init__(self, name, weightsDf):

        self.name = name
        self.weightsDf = weightsDf

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


missingData = set()
def addIndexColumn(index, pricesDf):
    
    #Calculate index price
    pricesDate = pricesDf["time"].iat[0].date()
    indexDistribution = index.getDistribution(pricesDate)
    newColumn = pd.Series([0 for i in range(len(pricesDf.index))])
    for ticker,weight in indexDistribution.items():
        
        #TODO: We didn't capture all prices for all the securities in the distributions.
        #This is because BIST50 equities change overtime and we only capture BIST50 equities of now.
        if ticker in pricesDf:
            newColumn += newColumn + weight * pricesDf[ticker]
        else:
            #Log the missing ticker
            if (ticker,index.name) not in missingData:
                print (f"Ticker {ticker} is missing for {index.name}")
                missingData.add((ticker,index.name))

    pricesDf[index.name] = newColumn

    return pricesDf

    
def addPriceChangeColumn(index, pricesDf):
    
    #Calculate index price change
    priceChangeColumnName = f"{index.name}_change"
    startOfDayPrice = pricesDf[index.name].iat[pricesDf[index.name].first_valid_index()]

    pricesDf[priceChangeColumnName] = 100 * (pricesDf[index.name] - startOfDayPrice)/startOfDayPrice

    return pricesDf

def calculateReturn(pricesDf, index1Name, index2Name):

    numberOfMinutes = len(pricesDf[index1Name])
    totalReturn = 1
    bought = False
    boughtPrice = 0
    lastPriceChangeDiff = 0
    
    for index,row in pricesDf.iterrows():
        priceChangeDiff = abs(row["price_change_diff"])
        
        if math.isnan(priceChangeDiff):
            if not math.isnan(row[index1Name]) or not math.isnan(row[index2Name]):
                print ("CRITICAL -- Price change diff is NaN but at least one of the prices is not NaN")
                print (pricesDf[["time", index1Name,index2Name,f"{index1Name}_change", f"{index2Name}_change", "price_change_diff"]].to_string())
                exit()
            else:
                continue

        #Other thresholds tried:
        # 0.5
        # 0.1
        lastPriceChangeDiff = priceChangeDiff
        if priceChangeDiff >= 0.5:
            bought = True
            boughtPrice = priceChangeDiff

        if bought and priceChangeDiff <= 0.1:
            totalReturn *= 1 + (boughtPrice - priceChangeDiff)/100
            bought = False

    if bought:
        totalReturn *= 1 + (boughtPrice - lastPriceChangeDiff)/100

    return totalReturn

def getAllPriceCharts(pricesDir, startDate, endDate, index1, index2, tradingStartTimeString, tradingEndTimeString, plotPriceThreshold):

    currentDate = startDate
    returnedLineCharts = []
    totalReturn = 1
    while currentDate != endDate:
        currentDateString = currentDate.strftime('%Y-%m-%d')
        currentDatePricesFile = f"{pricesDir}/{currentDateString}_minutely_prices.csv"
        try:

            pricesDf = pd.read_csv(currentDatePricesFile)
            pricesDf["time"] = pd.to_datetime(pricesDf["time"])

            #Filter out pricing early and late in the session
            mask = (pricesDf["time"] >= f'{currentDateString} {tradingStartTimeString}') & (pricesDf["time"] <= f'{currentDateString} {tradingEndTimeString}')
            pricesDf = pricesDf.loc[mask]
            pricesDf.reset_index(inplace=True, drop=True)

            pricesDf = addIndexColumn(index1, pricesDf)
            pricesDf = addIndexColumn(index2, pricesDf)

            #Reduce the columns to only the relevant ones
            #Get rid of rows that have NaN price for either of the indices
            pricesDf = pricesDf[["time", index1.name, index2.name]]
            pricesDf = pricesDf.dropna()
            pricesDf.reset_index(inplace=True, drop=True)

            pricesDf = addPriceChangeColumn(index1, pricesDf)
            pricesDf = addPriceChangeColumn(index2, pricesDf)

            priceChangeDiff = pricesDf[f"{index1.name}_change"] - pricesDf[f"{index2.name}_change"]
            pricesDf["price_change_diff"] = priceChangeDiff

            totalReturn *= calculateReturn(pricesDf, index1.name, index2.name)

            priceChangeDiff = [min(i,plotPriceThreshold) for i in priceChangeDiff]
            priceChangeDiff = [max(i,-plotPriceThreshold) for i in priceChangeDiff]

            newLineChart = go.Scatter(x=pricesDf["time"] , y=priceChangeDiff, name=f"{currentDateString}")
            returnedLineCharts.append(newLineChart)
            
        except FileNotFoundError:
            pass

        currentDate += datetime.timedelta(days=1)


    print (f"Total return for pair {index1.name} - {index2.name}: {totalReturn}")

    return returnedLineCharts


pricesDir = sys.argv[1]
startDateString = sys.argv[2]
endDateString = sys.argv[3]

startDate = datetime.datetime.strptime(startDateString, '%Y-%m-%d')
endDate = datetime.datetime.strptime(endDateString, '%Y-%m-%d')

weightsDir = f"{pricesDir}/etf_position_distributions/"
# worthwhileIndices = [('ZRE20.F','Z30EA.F')]

worthwhileIndices = [
                    ('ZPX30.F','ZTM15.F'),
                    ('ZPX30.F','ZRE20.F'),
                    ('ZPX30.F','DJIST.F'),
                    ('ZPX30.F','Z30EA.F'),
                    ('ZRE20.F','Z30EA.F'),
                    ('DJIST.F','Z30EA.F'),
                    ('BIST30_one_each', 'DJIST.F'),
                    ('BIST30_one_each', 'ZPX30.F'),
                    ('BIST30_one_each', 'Z30EA.F')
                    ]



for index1Name, index2Name in worthwhileIndices:
    
    #Use this to use only stocks' mids when calculating alpha        
    index1 = Index(index1Name, getWeights(index1Name, weightsDir))
    index2 = Index(index2Name, getWeights(index2Name, weightsDir))

    #Use this to use ETF prices for everythin except BIST30 when calculating alphs
    # index1 = Index(index1Name, getUnitWeightForTicker(index1Name))
    # index2 = Index(index2Name, getUnitWeightForTicker(index2Name))

    allLineCharts = getAllPriceCharts(pricesDir, startDate, endDate, index1, index2, "10:15:00", "17:45:00", 0.5)

    numColumns = 5
    fig = ps.make_subplots(rows=math.ceil(len(allLineCharts)/numColumns), cols=numColumns)

    count = 0
    for lineChart in allLineCharts:
        fig.add_trace(lineChart, row=int(count/numColumns)+1, col=(count % numColumns)+1)
        count += 1


    fig.update_layout(title=f'Price Diffs {index1.name} - {index2.name}',
        autosize=True,
        height=4000,
    )

    # fig.show()


