allEtfs = ['USDTR.F', 'ZPBDL.F', 'ZPX30.F', 'ZRE20.F', 'ZPT10.F', 'GLDTR.F', 'DJIST.F', 'Z30KE.F', 'GMSTR.F', 'Z30KP.F', 'ZTM15.F', 'Z30EA.F', 'ZPLIB.F', 'ZELOT.F', 'ZGOLD.F']

# Calculate each basket based on basket weights located in basketWeightsDir
# and add each basket's trajectory into the pricesDataFrame.
# Return pricesDataFrame.
def addBasketColumns(basketWeightsDir, pricesDataFrame, baskets):
    pass


def getAllBaskets():

    etfUnderlyingBaskets = []
    for etf in allEtfs:
        etfUnderlyingBaskets.append(f'{etf}_underlying')

    returned = etfUnderlyingBaskets + ['BIST30_one_of_each']
    
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

currentDate = datetime.strptime(startDateString, '%Y-%m-%d')
endDate = datetime.strptime(endDateString, '%Y-%m-%d')

while currentDate != endDate:
    currentDateString = currentDate.strftime('%Y-%m-%d')
    currentDatePricesFile = f"{intervalPricesDataDir}/{currentDateString}_interval_prices.csv"
    pricesDf = pd.read_csv(currentDatePricesFile)


    allBaskets = getAllBaskets()
    
    #TODO: Implement addBasketColumns
    pricesDf = addBasketColumns(basketWeightsDir, pricesDf, allBaskets)
    dailyPriceCorrelations = getCorrelations(pricesDf, allBaskets)

    with open(f"{runDateString}_correlations.json", "w+") as outfile:
        json.dump(dailyPriceCorrelations, outfile)



# print ("Correlations: ")
# for series in timeseries:
#     if series in dailyPriceCorrelations:
#         print (series)
#         correlationsList = list(dailyPriceCorrelations[series].items())
#         correlationsList.sort(key=lambda x: x[1])
#         print (f"{correlationsList}\n\n\n")