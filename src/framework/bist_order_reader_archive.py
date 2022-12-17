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

import sys
import mmap
from collections import defaultdict

from l3_limit_order_book import L3LimitOrderBook

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



orderBookFile = sys.argv[1]
symbol = sys.argv[2]

counter = 0
orderTypes = defaultdict(int)
orderStatuses = defaultdict(int)
sessions = defaultdict(int)
orderCategories = defaultdict(int)
changeReasons = defaultdict(int)
validityTypes = defaultdict(int)
lineCounter = 0

# try:
#     with open(orderBookFile, "r+b") as f:
#         map_file = mmap.mmap(f.fileno(), 0, flags=mmap.MAP_PRIVATE, prot=mmap.PROT_READ)
#         for line in iter(map_file.readline, b""):
#             lineString = line.decode("utf-8") 
#             lineSplit = lineString.split(';')
            
#             if lineSplit[SYMBOL] not in bist50Syms:
#                 continue

#             orderTypes[lineSplit[ORDER_TYPE]] += 1
#             orderStatuses[lineSplit[ORDER_STATUS]] += 1
#             sessions[lineSplit[SESSION_STATE]] += 1
#             orderCategories[lineSplit[ORDER_CATEOGRY]] += 1
#             changeReasons[lineSplit[CHANGE_REASON]] += 1
#             validityTypes[lineSplit[VALIDITY_TYPE]] += 1
#             lineCounter += 1

#             if lineCounter % 1000000 == 0:
#                 print (lineCounter)


#     print (f"Order types: {orderTypes}")
#     print (f"Order statuses: {orderStatuses}")
#     print (f"Sessions: {sessions}")
#     print (f"Order categories: {orderCategories}")
#     print (f"changeReasons {changeReasons}")
#     print (f"validityTypes {validityTypes}")
# except FileNotFoundError:
#     pass

def isMarketOrder(lineSplit):
    orderType = int(lineSplit[ORDER_TYPE])
    return orderType & 2 > 0

def isNewOrder(lineSplit, allOrderIds):
    previousOrderId = lineSplit[PREVIOUS_ORDER_ID] + lineSplit[SYMBOL]
    return lineSplit[ORDER_STATUS] == '1' and \
           previousOrderId not in allOrderIds


def isUpdateOrder(lineSplit):
    pass

def isRemoveOrder(lineSplit):
    pass


lob = L3LimitOrderBook()
lineCount = 0
allOrderIds = defaultdict(list)

with open(orderBookFile, "r+b") as f:
    map_file = mmap.mmap(f.fileno(), 0, flags=mmap.MAP_PRIVATE, prot=mmap.PROT_READ)
    for line in iter(map_file.readline, b""):


        lineCount += 1
        if lineCount == 1:
            continue

        if lineCount %1000000 == 0:
            print (lineCount)

        lineString = line.decode("utf-8") 
        lineSplit = lineString.split(';')

        # if lineSplit[SYMBOL] != symbol:
        #     continue

        # # Ignore market orders
        # if isMarketOrder(lineSplit):
        #     continue

        #Consider only equities
        if lineSplit[SYMBOL] not in bist50Syms:
            continue


        
        
        # if isMarketOrder(lineSplit) and lineSplit[ORDER_STATUS] == '1' and lineSplit[CHANGE_REASON] == '6':
        #     print (printedLine)
        orderId = lineSplit[ORDER_ID]
        symbol = lineSplit[SYMBOL]
        if orderId in ["675D43C1004F48D5"]:
            printedLine = f"Price: {lineSplit[PRICE]}, Size: {lineSplit[SIZE]}, Traded Quantity: {lineSplit[TRADE_VOLUME]}, ChangeReason: {lineSplit[CHANGE_REASON]}, Active: {lineSplit[ORDER_STATUS]}, Timestamp: {lineSplit[TIMESTAMP].strip()}, Modified Timestamp {lineSplit[MODIFIED_TIMESTAMP]}, Seq num: {lineSplit[UPDATE_SEQUENCE_NO]}, Order quantity: {lineSplit[ORDER_AMOUNT]}, Symbol: {lineSplit[SYMBOL]}, Order id: {lineSplit[ORDER_ID]}, Side: {lineSplit[SIDE]}, Best Bid: {lineSplit[BEST_BID]}, Best Ask: {lineSplit[BEST_ASK]}"
            print (line)
            print (printedLine)
            
        # if orderId in allOrderIds:
        #     print (f"{orderId}: ")
        #     allOrderIds[orderId].append(printedLine)
        #     for order in allOrderIds[orderId]:
        #         print (order)
        # else:
        #     allOrderIds[orderId].append(printedLine)

        
        # orderIdOfInterest = "675A7F0200F5502F"
        # if lineSplit[ORDER_ID] == orderIdOfInterest:
        #     print (printedLine)
        #     if (isMarketOrder(lineSplit)):
        #         print ("This is a market order")

        # if lineSplit[SYMBOL] == "BIMAS.E":
        #     print (printedLine)
        #     if (isMarketOrder(lineSplit)):
        #         print ("This is a market order")

        # if lineSplit[SIZE] == '0' and lineSplit[ORDER_STATUS] == '1':
        #     printedLine = lineString[:-1] + f"Price: {lineSplit[PRICE]}, Size: {lineSplit[SIZE]}, Traded Quantity: {lineSplit[TRADE_VOLUME]}, ChangeReason: {lineSplit[CHANGE_REASON]}, Active: {lineSplit[ORDER_STATUS]}"
        #     print (printedLine)



        # if isNewOrder(lineSplit, allOrderIds):
        #     orderId = lineSplit[ORDER_ID] + lineSplit[SYMBOL]
            
        #     appendedLine = lineString[:-1] + f"Price: {lineSplit[PRICE]}, Size: {lineSplit[SIZE]}, Traded Quantity: {lineSplit[TRADE_VOLUME]}, ChangeReason: {lineSplit[CHANGE_REASON]}"
        #     if orderId in allOrderIds:
        #         allOrderIds[orderId].append(appendedLine)
        #         print (f"New order has order id that we saw before: {orderId}")
        #         for orderEntryForId in allOrderIds[orderId]:
        #             print (orderEntryForId)
        #     else:
        #         allOrderIds[orderId].append(appendedLine)
            
        #     #Sanity checks
        #     #Change reason has to be "New"
        #     # if lineSplit[CHANGE_REASON] != "6":
        #     #     print (f"Change reason {lineSplit[CHANGE_REASON]} has to be New for new order")
        #     #     exit(1)
                               
        #     #Previous order ID has to be empty for a new order
        #     if lineSplit[PREVIOUS_ORDER_ID].strip() != "":
        #         print(f"Previous order ID [{lineSplit[PREVIOUS_ORDER_ID]}] has to be empty for a new order")
        #         exit(1)
                
                
            









        
        









        





