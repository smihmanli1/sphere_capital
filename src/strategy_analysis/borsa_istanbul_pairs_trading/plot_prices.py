import sys
import glob
import pandas as pd
import datetime

import plotly.graph_objects as go
import plotly.subplots as ps
import math
import plotly.express as px 
import numpy as np

from index_weights import getWeights, getUnitWeightForTicker
from utils import Index, addIndexColumn


def addPriceChangeColumn(index, pricesDf):
    
    #Calculate index price change
    priceChangeColumnName = f"{index.name}_change"
    startOfDayPrice = pricesDf[index.name].iat[pricesDf[index.name].first_valid_index()]

    pricesDf[priceChangeColumnName] = 100 * (pricesDf[index.name] - startOfDayPrice)/startOfDayPrice

    return pricesDf

def getSharpeRatio(allReturns):
    avgReturn = sum(allReturns)/len(allReturns)
    stdDev = np.std(allReturns)

    return avgReturn / stdDev


def calculateReturn(pricesDf, index1Name, index2Name, buyThreshold, sellThreshold):

    numberOfMinutes = len(pricesDf[index1Name])
    totalReturn = 1
    bought = False
    boughtPrice = 0
    lastPriceChangeDiff = 0
    allReturns = []
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
        if priceChangeDiff >= buyThreshold:
            bought = True
            boughtPrice = priceChangeDiff

        if bought and priceChangeDiff <= sellThreshold:
            thisReturn = 1 + (boughtPrice - priceChangeDiff)/100
            totalReturn *= thisReturn
            bought = False
            allReturns.append(thisReturn)

    if bought:
        thisReturn = (boughtPrice - lastPriceChangeDiff)/100
        totalReturn *= 1 + thisReturn
        allReturns.append(thisReturn)

    return totalReturn, allReturns

def getAllPriceCharts(
    pricesDir, 
    startDate, 
    endDate, 
    index1, 
    index2, 
    tradingStartTimeOffsetMinutes, 
    tradingEndTimeString, 
    plotPriceThreshold, 
    buyThreshold, 
    sellThreshold):

    currentDate = startDate
    returnedLineCharts = []
    totalReturn = 1
    standardDeviations = []
    allReturns = []
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
            backtestingStartTime = (pricingStartDatetime + datetime.timedelta(minutes=tradingStartTimeOffsetMinutes)).time()
            
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

            standardDeviations.append(pricesDf["price_change_diff"].std())

            totalReturnForDay, allReturnsForDay = calculateReturn(pricesDf, index1.name, index2.name, buyThreshold, sellThreshold)
            totalReturn *= totalReturnForDay
            allReturns += allReturnsForDay

            priceChangeDiff = [min(i,plotPriceThreshold) for i in priceChangeDiff]
            priceChangeDiff = [max(i,-plotPriceThreshold) for i in priceChangeDiff]

            newLineChart = go.Scatter(x=pricesDf["time"] , y=priceChangeDiff, name=f"{currentDateString}")
            returnedLineCharts.append(newLineChart)
            
        except FileNotFoundError:
            pass

        currentDate += datetime.timedelta(days=1)

    avgStdDeviationOfPriceChangeDiffs = sum(standardDeviations) / len(standardDeviations)
    print (f"Total return for pair {index1.name} - {index2.name}: {totalReturn}")
    print (f"Sharpe ratio for this return {getSharpeRatio(allReturns)}")
    print (f"Avg std deviation of prices change diffs: {avgStdDeviationOfPriceChangeDiffs}")
    print (f"Calculated optimal threshold: (\"{index1.name}\", \"{index2.name}\") : {avgStdDeviationOfPriceChangeDiffs/2}")
    
    return returnedLineCharts


pricesDir = sys.argv[1]
weightsDir = sys.argv[2]
startDateString = sys.argv[3]
endDateString = sys.argv[4]

startDate = datetime.datetime.strptime(startDateString, '%Y-%m-%d')
endDate = datetime.datetime.strptime(endDateString, '%Y-%m-%d')


newBuyThresholds = {
    ("BIST30_one_each", "DJIST.F_underlying") : (0.23135006184385065),
    ("BIST30_one_each", "ZPX30.F_underlying") : (0.24637276421239263),
    ("BIST30_one_each", "Z30EA.F_underlying") : (0.22715090978788477),
    ("BIST30_one_each", "ZRE20.F_underlying") : (0.22814700181221192),
    ("BIST30_one_each", "ZTM15.F_underlying") : (0.23098515918354467),
    ("KRDMA.E", "KRDMD.E") : (0.2333543059856918),
    ("DJIST.F_underlying", "ZPX30.F_underlying") : (0.03756987732303037),
    ("Z30EA.F_underlying", "ZPX30.F_underlying") : (0.06878611000768027),
    ("Z30EA.F_underlying", "ZRE20.F_underlying") : (0.1027274279616367),
    ("DJIST.F_underlying", "ZTM15.F_underlying") : (0.034190926808404264),
    ("DJIST.F_underlying", "Z30EA.F_underlying") : (0.03922820836931534),
    ("ZTM15.F_underlying", "Z30EA.F_underlying") : (0.0675590694448618),
    ("ZPX30.F_underlying", "ZTM15.F_underlying") : (0.04747330454210902),
    ("ZPX30.F_underlying", "ZPLIB.F_underlying") : (0.05422574679433837),
    ("ZPX30.F_underlying", "ZELOT.F_underlying") : (0.05422574679433837),
    ("DJIST.F_underlying", "ZPLIB.F_underlying") : (0.08000526910535635),
    ("DJIST.F_underlying", "ZELOT.F_underlying") : (0.08000526910535635),
    ("ZTM15.F_underlying", "ZPLIB.F_underlying") : (0.08257641874573943),
    ("ZTM15.F_underlying", "ZELOT.F_underlying") : (0.08257641874573943),
    ("Z30EA.F_underlying", "ZPLIB.F_underlying") : (0.10551719280641346),
    ("Z30EA.F_underlying", "ZELOT.F_underlying") : (0.10551719280641346),
    ("ZPT10.F_underlying", "Z30EA.F_underlying") : (0.13060986415947579),
    ("ZRE20.F_underlying", "ZTM15.F_underlying") : (0.14783775681318326),
    ("ZRE20.F_underlying", "Z30EA.F_underlying") : (0.1027274279616367),
    ("ZPBDL.F_underlying", "Z30EA.F_underlying") : (0.13060986415947579),
    ("ZPBDL.F_underlying", "DJIST.F_underlying") : (0.12549751476420187),
    ("ZPBDL.F_underlying", "ZRE20.F_underlying") : (0.12246690799621894)
}

oldBuyThresholds = {

    ('BIST30_one_each', 'DJIST.F_underlying') : 0.5,
    ('BIST30_one_each', 'ZPX30.F_underlying') : 0.5,
    ('BIST30_one_each', 'Z30EA.F_underlying') : 0.5,
    ('BIST30_one_each', 'ZRE20.F_underlying') : 0.5,
    ('BIST30_one_each', 'ZTM15.F_underlying') : 0.5,
    ('KRDMA.E', 'KRDMD.E')                    : 0.5,

    ("DJIST.F_underlying", "ZPX30.F_underlying") : 0.05,
    ("Z30EA.F_underlying", "ZPX30.F_underlying") : 0.05,
    ("Z30EA.F_underlying", "ZRE20.F_underlying") : 0.05,
    ("DJIST.F_underlying", "ZTM15.F_underlying") : 0.05,
    ("DJIST.F_underlying", "Z30EA.F_underlying") : 0.05,
    ("ZTM15.F_underlying", "Z30EA.F_underlying") : 0.05,
    ("ZPX30.F_underlying", "ZTM15.F_underlying") : 0.05,
    ("ZPX30.F_underlying", "ZPLIB.F_underlying") : 0.05,
    ("ZPX30.F_underlying", "ZELOT.F_underlying") : 0.05,
    ("DJIST.F_underlying", "ZPLIB.F_underlying") : 0.05,
    ("DJIST.F_underlying", "ZELOT.F_underlying") : 0.05,
    ("ZTM15.F_underlying", "ZPLIB.F_underlying") : 0.05,
    ("ZTM15.F_underlying", "ZELOT.F_underlying") : 0.05,
    ("Z30EA.F_underlying", "ZPLIB.F_underlying") : 0.05,
    ("Z30EA.F_underlying", "ZELOT.F_underlying") : 0.05,
    ("ZPT10.F_underlying", "Z30EA.F_underlying") : 0.05,
    ("ZRE20.F_underlying", "ZTM15.F_underlying") : 0.05,
    ("ZRE20.F_underlying", "Z30EA.F_underlying") : 0.05,
    ("ZPBDL.F_underlying", "Z30EA.F_underlying") : 0.05,
    ("ZPBDL.F_underlying", "DJIST.F_underlying") : 0.05,
    ("ZPBDL.F_underlying", "ZRE20.F_underlying") : 0.05,

    
}

worthwhileIndices = [
                    (Index('BIST30_one_each', weightsDir), Index('DJIST.F_underlying', weightsDir) ),
                    (Index('BIST30_one_each', weightsDir), Index('ZPX30.F_underlying', weightsDir) ),
                    (Index('BIST30_one_each', weightsDir), Index('Z30EA.F_underlying', weightsDir) ),
                    (Index('BIST30_one_each', weightsDir), Index('ZRE20.F_underlying', weightsDir) ),
                    (Index('BIST30_one_each', weightsDir), Index('ZTM15.F_underlying', weightsDir) ),
                    (Index('KRDMA.E', weightsDir), Index('KRDMD.E', weightsDir) ),
                    (Index('DJIST.F_underlying', weightsDir), Index('ZPX30.F_underlying', weightsDir) ),
                    (Index('Z30EA.F_underlying', weightsDir), Index('ZPX30.F_underlying', weightsDir) ),
                    (Index('Z30EA.F_underlying', weightsDir), Index('ZRE20.F_underlying', weightsDir) ),
                    (Index('DJIST.F_underlying', weightsDir), Index('ZTM15.F_underlying', weightsDir) ),
                    (Index('DJIST.F_underlying', weightsDir), Index('Z30EA.F_underlying', weightsDir) ),
                    (Index('ZTM15.F_underlying', weightsDir), Index('Z30EA.F_underlying', weightsDir) ),
                    (Index('ZPX30.F_underlying', weightsDir), Index('ZTM15.F_underlying', weightsDir) ),
                    (Index('ZPX30.F_underlying', weightsDir), Index('ZPLIB.F_underlying', weightsDir) ),
                    (Index('ZPX30.F_underlying', weightsDir), Index('ZELOT.F_underlying', weightsDir) ),
                    (Index('DJIST.F_underlying', weightsDir), Index('ZPLIB.F_underlying', weightsDir) ),
                    (Index('DJIST.F_underlying', weightsDir), Index('ZELOT.F_underlying', weightsDir) ),
                    (Index('ZTM15.F_underlying', weightsDir), Index('ZPLIB.F_underlying', weightsDir) ),
                    (Index('ZTM15.F_underlying', weightsDir), Index('ZELOT.F_underlying', weightsDir) ),
                    (Index('Z30EA.F_underlying', weightsDir), Index('ZPLIB.F_underlying', weightsDir) ),
                    (Index('Z30EA.F_underlying', weightsDir), Index('ZELOT.F_underlying', weightsDir) ),
                    (Index('ZPT10.F_underlying', weightsDir), Index('Z30EA.F_underlying', weightsDir) ),
                    (Index('ZRE20.F_underlying', weightsDir), Index('ZTM15.F_underlying', weightsDir) ),
                    (Index('ZRE20.F_underlying', weightsDir), Index('Z30EA.F_underlying', weightsDir) ),
                    (Index('ZPBDL.F_underlying', weightsDir), Index('Z30EA.F_underlying', weightsDir) ),
                    (Index('ZPBDL.F_underlying', weightsDir), Index('DJIST.F_underlying', weightsDir) ),
                    (Index('ZPBDL.F_underlying', weightsDir), Index('ZRE20.F_underlying', weightsDir) ),
                    ]



for index1, index2 in worthwhileIndices:
    
    index1Name = index1.name
    index2Name = index2.name

    allLineCharts = getAllPriceCharts(
        pricesDir, 
        startDate, 
        endDate, 
        index1, 
        index2, 
        tradingStartTimeOffsetMinutes=15, 
        tradingEndTimeString="17:45:00", 
        plotPriceThreshold=0.5, 
        buyThreshold= newBuyThresholds[(index1Name,index2Name)],
        sellThreshold= newBuyThresholds[(index1Name,index2Name)]/5)

    print ()
    print ()

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


