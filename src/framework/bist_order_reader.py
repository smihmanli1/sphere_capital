# DATE
# ORDER NO
# ENTRY DATE AND TIME
# MODIFIED DATE AND TIME
# TRANSACTION CODE
# BUY_SELL
# ORDER PRICE TYPE
# ORDER TYPE
# ORDER CATEGORY
# VALIDITY TYPE
# ORDER STATUS
# CHANGE REASON
# ORDER AMOUNT
# BALANCE
# APPARENT AMOUNT
# PRICE
# AGENCY_FUND CODE (AFC)
# SESSION
# BEST BID PRICE
# BEST ASK PRICE
# PREVIOUS ORDER NR
# TRADE NUMBER
# MATCH QUANTITY
# GIVE UP FLAG
# UPDATE NO
# UPDATE TIME
#Fields in order in historical data

import mmap
import datetime
import operator
from collections import defaultdict
from pipe import Pipe
from limit_order import *


bist50Syms = set(
    ["AKBNK.E",
    "AKSEN.E",
    "ALARK.E",
    "ALKIM.E",
    "ARCLK.E",
    "ASELS.E",
    "BERA.E",
    "BIMAS.E",
    "DOHOL.E",
    "EKGYO.E",
    "ENJSA.E",
    "ENKAI.E",
    "EREGL.E",
    "FROTO.E",
    "GARAN.E",
    "GLYHO.E",
    "GUBRF.E",
    "HALKB.E",
    "HEKTS.E",
    "ISCTR.E",
    "KARSN.E",
    "KCHOL.E",
    "KORDS.E",
    "KOZAA.E",
    "KOZAL.E",
    "KRDMD.E",
    "LOGO.E",
    "MGROS.E",
    "ODAS.E",
    "OTKAR.E",
    "PETKM.E",
    "PGSUS.E",
    "SAHOL.E",
    "SASA.E",
    "SISE.E",
    "SKBNK.E",
    "SOKM.E",
    "TAVHL.E",
    "TCELL.E",
    "THYAO.E",
    "TKFEN.E",
    "TOASO.E",
    "TSKB.E",
    "TTKOM.E",
    "TTRAK.E",
    "TUPRS.E",
    "TURSG.E",
    "VAKBN.E",
    "VESTL.E",
    "YKBNK.E"])

SYMBOL = 4
ORDER_ID = 1
PREVIOUS_ORDER_ID = 20
PRICE = 15
SIZE = 13 
ORDER_AMOUNT = 12
SIDE = 5
TIMESTAMP = 25
ORDER_TYPE = 6
MODIFIED_TIMESTAMP = 3

ORDER_CATEOGRY = 8 #For item 2
VALIDITY_TYPE = 9 #For item 3
ORDER_STATUS = 10 #For item 4
SESSION_STATE = 17
BEST_BID = 18
BEST_ASK = 19
CHANGE_REASON = 11
TRADE_VOLUME = 22
UPDATE_SEQUENCE_NO=24
ORDER_TYPE=7

epoch = datetime.datetime.utcfromtimestamp(0)

class BistOrderReader(Pipe):

    def __init__(self, ordersFile, limitOrderBook, validateBook = False):
        Pipe.__init__(self)
        self.orderFileHandle = open(ordersFile, "r+b")
        self.ordersMmapFile = mmap.mmap(self.orderFileHandle.fileno(), 0, flags=mmap.MAP_PRIVATE, prot=mmap.PROT_READ)
        self.orderIdToPrice = defaultdict(float)
        self.lastTimestamp = datetime.datetime(year=1970,month=1,day=1)
        self.currentOrderBatch = []
        self.limitOrderBook = limitOrderBook
        self.bistContinousTradingSession = 'P_SUREKLI_ISLEM'
        self.validateBook = validateBook

    def _getBistSide(self, lineSplit):
        #a bid s ask
        if lineSplit[SIDE] == 'A':
            return Side.BID
        elif lineSplit[SIDE] == 'S':
            return Side.ASK
        else:
            print (f"Unknown side: {lineSplit[SIDE]}")
            raise Exception("Unknown side")

    def _unixTimeMillis(self, dt):
        #TODO: Can extract millis or micros here
        return (dt - epoch).total_seconds() * 1000.0

    def _bistTimestampToDatetime(self, timestampString):
        return datetime.datetime.strptime(timestampString, "%Y-%m-%d %H:%M:%S.%f")

    def _getOrders(self, lineSplit):
        orderId = lineSplit[ORDER_ID]

        timstampMsSinceEpoch = self._unixTimeMillis(self._bistTimestampToDatetime(lineSplit[MODIFIED_TIMESTAMP].strip()))
        if orderId not in self.orderIdToPrice:
            if lineSplit[ORDER_STATUS] == '1':
                #If it's a midpoint order or 0 price order then ignore
                if lineSplit[ORDER_TYPE] == "64" or float(lineSplit[PRICE]) == 0:
                    return None
                self.orderIdToPrice[orderId] = float(lineSplit[PRICE])
                return [LimitOrder(
                    symbol = lineSplit[SYMBOL],
                    orderId = orderId,
                    price = float(lineSplit[PRICE]),
                    size = float(lineSplit[SIZE]),
                    side = self._getBistSide(lineSplit),
                    timestamp = timstampMsSinceEpoch,
                    operation = Operation.ADD,
                    session = lineSplit[SESSION_STATE])]
        else:
            if lineSplit[ORDER_STATUS] == '1':
                oldPrice = self.orderIdToPrice[orderId]
                self.orderIdToPrice[orderId] = float(lineSplit[PRICE])
                if oldPrice != float(lineSplit[PRICE]):
                    return [
                        LimitOrder(
                            symbol = lineSplit[SYMBOL],
                            orderId = orderId,
                            price = oldPrice,
                            size = 0, #Setting size to 0 to signal that it should be removed.
                            side = self._getBistSide(lineSplit),
                            timestamp = timstampMsSinceEpoch,
                            operation = Operation.UPDATE,
                            session = lineSplit[SESSION_STATE]),
                        LimitOrder(
                            symbol = lineSplit[SYMBOL],
                            orderId = orderId,
                            price = float(lineSplit[PRICE]),
                            size = float(lineSplit[SIZE]),
                            side = self._getBistSide(lineSplit),
                            timestamp = timstampMsSinceEpoch,
                            operation = Operation.ADD,
                            session = lineSplit[SESSION_STATE])   
                    ]
                else:
                    return [LimitOrder(
                        symbol = lineSplit[SYMBOL],
                        orderId = orderId,
                        price = float(lineSplit[PRICE]),
                        size = float(lineSplit[SIZE]),
                        side = self._getBistSide(lineSplit),
                        timestamp = timstampMsSinceEpoch,
                        operation = Operation.UPDATE,
                        session = lineSplit[SESSION_STATE])]
            elif lineSplit[ORDER_STATUS] == '2' or lineSplit[ORDER_STATUS] == '4':
                del self.orderIdToPrice[orderId]
                return [LimitOrder(
                    symbol = lineSplit[SYMBOL],
                    orderId = orderId,
                    price = float(lineSplit[PRICE]),
                    size = 0, #Setting size to 0 to signal that it should be removed.
                    side = self._getBistSide(lineSplit),
                    timestamp = timstampMsSinceEpoch,
                    operation = Operation.UPDATE,
                    session = lineSplit[SESSION_STATE])]
            else:
                print (f"Unknown order status: {lineSplit[SYMBOL]}, {lineSplit[ORDER_ID]}, {lineSplit[ORDER_STATUS]}")
                raise Exception("Unknown order status")

        return None

    def _validateBook(self, symbol, bestBids, bestAsks, currentSessionStates):

        # If current session for this symbol is not continuous trading, no need to validate
        if currentSessionStates[symbol] != self.bistContinousTradingSession:
            return

        # Our book isn't valid but their bests are valid
        if self.limitOrderBook.validBook(symbol) is not True and bestBids[symbol] < bestAsks[symbol]:
            print (f"Our book isn't valid but their bests are valid. Symbol {symbol}")
            print (f"Our best bid {self.limitOrderBook.bestBid(symbol).price}")
            print (f"Our best ask {self.limitOrderBook.bestAsk(symbol).price}")
            print (f"Their best bid {bestBids[symbol]}")
            print (f"Their best ask {bestAsks[symbol]}")
            raise Exception("Our book isn't valid but their bests are valid")

        # Book is not croseed but our bests and their bests don't match
        if bestBids[symbol] < bestAsks[symbol] and self.limitOrderBook.hasBid(symbol) and self.limitOrderBook.hasAsk(symbol) and (self.limitOrderBook.bestBid(symbol).price != bestBids[symbol] or self.limitOrderBook.bestAsk(symbol).price != bestAsks[symbol]):
            print (f"Our bests and their bests don't match")
            print (f"Our best bid {self.limitOrderBook.bestBid(symbol).price}")
            print (f"Our best ask {self.limitOrderBook.bestAsk(symbol).price}")
            print (f"Their best bid {bestBids[symbol]}")
            print (f"Their best ask {bestAsks[symbol]}")
            raise Exception("Our bests and their bests don't match")
        

    def start(self):
        lineCount = 0
        prevTime = 0
        currentBatch = []
        bestBids = {}
        bestAsks = {}
        currentSessionStates = {}
        currentBatchSymbols = set()
        for line in iter(self.ordersMmapFile.readline, b""):
            lineCount += 1
            if lineCount == 1:
                continue

            if lineCount % 1000000 == 0:
                print (lineCount)

            lineString = line.decode("utf-8") 
            lineSplit = lineString.split(';')

            #Consider only BIST 50 equities
            symbol = lineSplit[SYMBOL]
            if symbol[-2:] != ".F" and symbol not in bist50Syms:
                continue

            currentTime = self._unixTimeMillis(self._bistTimestampToDatetime(lineSplit[MODIFIED_TIMESTAMP].strip()))
            if currentTime != prevTime:
                for order in currentBatch:
                    self.produce(order)
                
                # Just sent a batch to the books. Make sure the books are good.
                if self.validateBook:
                    for sym in currentBatchSymbols:
                        self._validateBook(sym, bestBids, bestAsks, currentSessionStates)
                
                currentBatch = []
                currentBatchSymbols.clear()                
            
            #Update the current batch            
            currentOrders = self._getOrders(lineSplit)
            if currentOrders is not None:
                currentBatch += currentOrders
                currentBatchSymbols.add(symbol)

            #Update their bests
            bestBids[symbol] = float(lineSplit[BEST_BID])
            bestAsks[symbol] = float(lineSplit[BEST_ASK])
            currentSessionStates[symbol] = lineSplit[SESSION_STATE]
            #Update prev time
            prevTime = currentTime


        #Don't forget to send the orders in the last batch
        for order in currentBatch:
            self.produce(order)


