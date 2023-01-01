
import sys
import pandas as pd
import datetime
from collections import defaultdict
import json

allEtfs = ['USDTR.F', 'ZPBDL.F', 'ZPX30.F', 'ZRE20.F', 'ZPT10.F', 'GLDTR.F', 'DJIST.F', 'Z30KE.F', 'GMSTR.F', 'Z30KP.F', 'ZTM15.F', 'Z30EA.F', 'ZPLIB.F', 'ZELOT.F', 'ZGOLD.F']
baskets = [
    'ZPBDL.F_underlying', 
    'ZPX30.F_underlying', 
    'ZRE20.F_underlying', 
    'ZPT10.F_underlying', 
    'GLDTR.F_underlying', 
    'DJIST.F_underlying', 
    'Z30KE.F_underlying', 
    'GMSTR.F_underlying', 
    'Z30KP.F_underlying', 
    'ZTM15.F_underlying', 
    'Z30EA.F_underlying', 
    'ZPLIB.F_underlying', 
    'ZELOT.F_underlying', 
    'ZGOLD.F_underlying',
    'BIST30_one_of_each']

# Calculate each basket based on basket weights located in basketWeightsDir
# and add each basket's trajectory into the pricesDataFrame.
# Return pricesDataFrame.
def addBasketColumns(basketWeightsDir, pricesDataFrame, baskets):
    #TODO: Implement
    return pricesDataFrame

def getAllBaskets():
    returned = etfUnderlyingBaskets + baskets
    return returned

def getCorrelations(pricesDataFrame, priceTimeseries):
    result = defaultdict(dict)
    for timeseries1 in priceTimeseries:
        for timeseries2 in priceTimeseries:
            if timeseries1 in pricesDataFrame and timeseries2 in pricesDataFrame:
                result[timeseries1][timeseries2] = pricesDataFrame[timeseries1].corr(pricesDataFrame[timeseries2])

    return result 

intervalPricesDataDir = sys.argv[1]
startDateString = sys.argv[2]
endDateString = sys.argv[3]
basketWeightsDir = sys.argv[4]

currentDate = datetime.datetime.strptime(startDateString, '%Y-%m-%d')
endDate = datetime.datetime.strptime(endDateString, '%Y-%m-%d')

while currentDate != endDate:
    currentDateString = currentDate.strftime('%Y-%m-%d')
    currentDatePricesFile = f"{intervalPricesDataDir}/{currentDateString}_interval_prices.csv"
    
    try:
        pricesDf = pd.read_csv(currentDatePricesFile)
        pricesDf = addBasketColumns(basketWeightsDir, pricesDf, baskets)

        allArbitrageable = baskets + allEtfs
        dailyPriceCorrelations = getCorrelations(pricesDf, allArbitrageable)

        with open(f"{currentDateString}_correlations.json", "w+") as outfile:
            json.dump(dailyPriceCorrelations, outfile)
    except FileNotFoundError:
        pass
    
    currentDate += datetime.timedelta(days=1)


