
from pipe import Pipe
from trading_algo import TradingAlgo
from collections import defaultdict
from limit_order import Side
import datetime
from market_order import MarketOrder
import math

MILLIS_IN_A_SEC = 1000

class BistPairsTradingAlgo2(TradingAlgo):

    #Find type for 'parameters' and 'tickerPairs' in bist50_pairs_trading_backtester
    def __init__(self, backtestOrderSubmitter, lob, parameters):
        self.backtestOrderSubmitter = backtestOrderSubmitter
        self.lob = lob
        self.eventCount = 0
        self.parameters = parameters
        self.allTickers = set()
        self.startPrices = {}
        self.tickerPairsMapping = defaultdict(list)
        self.means = {}
        self.standardDeviations = {}
        self.ordersOutgoing = set()
        self.positions = {}
        self.currentUsdTry = 8.5
        self.positionValueLimit = 50000
        self.minimumPositionValueToOpen = 1000
        self.securityStatuses = {}
        self.doneForTheDay = False
        self.invalidBooks = defaultdict(bool)
        self.lastTimeTraded = parameters["exchange_open_time"]

        self.priceChanges = {}
        self.startOfDayPrices = {}

        self.totalMoney = 0
        self.lastTriggerTime = None

    def _dumpPriceDiffs(self):

        sortedPriceDiffs = sorted(self.priceChanges.items(), key= lambda x: x[1])

        for sym,priceChange in sortedPriceDiffs:
            print (f"{sym}: {priceChange}")

    def accept(self, event):
        
        #TODO: We should not trade if our best bid/ask does not match their best bid ask. (For this 
        # we should keep a copy of the book that is not manipulated by our orders)
        if self.doneForTheDay:
            return
        
        #This is where the algo takes action
        eventTime = datetime.datetime.utcfromtimestamp(event.timestamp / MILLIS_IN_A_SEC)
        self.eventCount += 1
        if self.eventCount % 100000 == 0:
            print (f"Processed {self.eventCount} events")
            print (f"Time: {eventTime}")

        currentTime = eventTime.time()
        if currentTime < self.parameters["exchange_open_time"]:
            return

        if self.lastTriggerTime is None:
            self.lastTriggerTime = event.timestamp

        if not self.lob.validBook(event.symbol):
            return

        if event.session != 'P_SUREKLI_ISLEM':
            print ("Not in continuous trading")
            return
        
        #Algo
        # print (f"{event.symbol} price: {self.lob.mid(event.symbol)}")

        if event.symbol not in self.startOfDayPrices:
            self.startOfDayPrices[event.symbol] = self.lob.mid(event.symbol)

        if event.symbol in self.startOfDayPrices:
            symbolStartOfDayPrice = self.startOfDayPrices[event.symbol]
            self.priceChanges[event.symbol] = (self.lob.mid(event.symbol) - symbolStartOfDayPrice)/symbolStartOfDayPrice

        if event.timestamp - self.lastTriggerTime > 20 * 60 * MILLIS_IN_A_SEC:
            print (f"Dump price diffs: {event.timestamp}")
            self._dumpPriceDiffs()
            exit(1)

    def orderAckedCallback(self,operation,orderId):
        pass

    def orderRejectedCallback(self,orderId,side):
        pass

    def notifyFilledCallback(self, matchedMarketOrder, algoOrder):
        # When we get a fill, we should run the above algo again.
        pass
