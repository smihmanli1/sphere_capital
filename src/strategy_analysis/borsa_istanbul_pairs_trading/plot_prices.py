import sys
import glob
import pandas as pd
import datetime

import plotly.graph_objects as go
import plotly.subplots as ps
import math
import plotly.express as px 

from index_weights import getWeights, getUnitWeightForTicker
from utils import Index, addIndexColumn


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

        #Thresholds tried:
        # 0.5
        # 0.1

        #0.2
        #0.01
        lastPriceChangeDiff = priceChangeDiff
        if priceChangeDiff >= 0.2:
            bought = True
            boughtPrice = priceChangeDiff

        if bought and priceChangeDiff <= 0.01:
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
        currentDatePricesFile = f"{pricesDir}/{currentDateString}_interval_prices.csv"
        try:

            pricesDf = pd.read_csv(currentDatePricesFile)
            pricesDf["time"] = pd.to_datetime(pricesDf["time"])

            pricesDf = addIndexColumn(index1, pricesDf)
            pricesDf = addIndexColumn(index2, pricesDf)

            #Reduce the columns to only the relevant ones
            #Get rid of rows that have NaN price for either of the indices
            pricesDf = pricesDf[["time", index1.name, index2.name]]

            pricesDf = pricesDf.dropna()
            pricesDf.reset_index(inplace=True, drop=True)
            if pricesDf.empty:
                currentDate += datetime.timedelta(days=1)
                continue

            
            #We want to backtest starting from 15 minutes after the first pricing started for this
            #pair.
            #Filter out pricing early and late in the session
            pricingStartDatetime = pricesDf["time"].iat[0]
            backtestingStartTime = (pricingStartDatetime + datetime.timedelta(minutes=15)).time()
            
            mask = (pricesDf["time"] >= f'{currentDateString} {backtestingStartTime}') & (pricesDf["time"] <= f'{currentDateString} {tradingEndTimeString}')
            pricesDf = pricesDf.loc[mask]
            pricesDf.reset_index(inplace=True, drop=True)
            if pricesDf.empty:
                currentDate += datetime.timedelta(days=1)
                continue

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
weightsDir = sys.argv[2]
startDateString = sys.argv[3]
endDateString = sys.argv[4]

startDate = datetime.datetime.strptime(startDateString, '%Y-%m-%d')
endDate = datetime.datetime.strptime(endDateString, '%Y-%m-%d')

worthwhileIndices = [
                    # (Index('ZPX30.F', weightsDir), Index('ZTM15.F', weightsDir) ),
                    # (Index('ZPX30.F', weightsDir), Index('ZRE20.F', weightsDir) ),
                    # (Index('ZPX30.F', weightsDir), Index('DJIST.F', weightsDir) ),
                    # (Index('ZPX30.F', weightsDir), Index('Z30EA.F', weightsDir) ),
                    # (Index('ZRE20.F', weightsDir), Index('Z30EA.F', weightsDir) ),
                    # (Index('DJIST.F', weightsDir), Index('Z30EA.F', weightsDir) ),
                    (Index('BIST30_one_each', weightsDir), Index('DJIST.F', weightsDir) ),
                    (Index('BIST30_one_each', weightsDir), Index('ZPX30.F', weightsDir) ),
                    (Index('BIST30_one_each', weightsDir), Index('Z30EA.F', weightsDir) )
                    ]



for index1, index2 in worthwhileIndices:
    
    index1Name = index1.name
    index2Name = index2.name

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

    fig.show()


