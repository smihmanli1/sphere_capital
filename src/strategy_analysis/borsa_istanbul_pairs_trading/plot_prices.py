import sys
import glob
import pandas as pd
import datetime

import plotly.graph_objects as go
import plotly.subplots as ps
import math
import plotly.express as px 

class Index:

    def __init__(self, name, distribution):

        self.name = name
        self.distribution = distribution


def addColumn(index, pricesDf):
    
    #Calculate index price
    indexDistribution = index.distribution
    newColumn = pd.Series([0 for i in range(len(pricesDf.index))])
    for ticker,weight in indexDistribution.items():
        newColumn += newColumn + weight * pricesDf[ticker]

    pricesDf[index.name] = newColumn

    #Calculate index price change
    priceChangeColumnName = f"{index.name}_change"
    startOfDayPrice = pricesDf[index.name].iat[pricesDf[index.name].first_valid_index()]
    pricesDf[priceChangeColumnName] = 100 * (pricesDf[index.name] - startOfDayPrice)/startOfDayPrice

    return pricesDf

def getAllPriceCharts(pricesDir, startDate, endDate, index1, index2, tradingStartHour, tradingEndHour, plotPriceThreshold):

    currentDate = startDate
    returnedLineCharts = []
    while currentDate != endDate:
        currentDateString = currentDate.strftime('%Y-%m-%d')
        currentDatePricesFile = f"{pricesDir}/{currentDateString}_minutely_prices.csv"
        try:

            pricesDf = pd.read_csv(currentDatePricesFile)
            pricesDf["time"] = pd.to_datetime(pricesDf["time"])

            #TODO: Add start time, end time for each day as function params
            mask = (pricesDf["time"].dt.hour > tradingStartHour) & (pricesDf["time"].dt.hour < tradingEndHour)

            pricesDf = pricesDf.loc[mask]    
            pricesDf = addColumn(index1, pricesDf)
            pricesDf = addColumn(index2, pricesDf)
            
            priceChangeDiff = pricesDf[f"{index1.name}_change"] - pricesDf[f"{index2.name}_change"]
            pricesDf["price_change_diff"] = priceChangeDiff

            

            priceChangeDiff = [min(i,plotPriceThreshold) for i in priceChangeDiff]
            priceChangeDiff = [max(i,-plotPriceThreshold) for i in priceChangeDiff]

            newLineChart = go.Scatter(x=pricesDf["time"] , y=priceChangeDiff, name=f"{currentDateString}")
            returnedLineCharts.append(newLineChart)
            
        except FileNotFoundError:
            pass

        currentDate += datetime.timedelta(days=1)

    return returnedLineCharts


pricesDir = sys.argv[1]
startDateString = sys.argv[2]
endDateString = sys.argv[3]

startDate = datetime.datetime.strptime(startDateString, '%Y-%m-%d')
endDate = datetime.datetime.strptime(endDateString, '%Y-%m-%d')

index1 = Index("ZPX30.F", {"ZPX30.F" : 1})
index2 = Index("ZTM15.F", {"ZTM15.F" : 1})

allLineCharts = getAllPriceCharts(pricesDir, startDate, endDate, index1, index2, 10, 17, 0.5)

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

fig.show()


