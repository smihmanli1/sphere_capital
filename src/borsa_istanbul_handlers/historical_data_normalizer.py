
import sys
import shutil
import os
import datetime

def getFileNameTimePortion(timestampValue):
	d = datetime.datetime.strptime(timestampValue, '%Y-%m-%d')
	return str(d.year) + str(d.month).zfill(2) + str(d.day).zfill(2)

def sortEachFile(assetType, fileNames):

	modifiedTimeIndex = 3
	orderIdIndex = 1
	seqNumIndex = 24

	for fileName in fileNames:
		print (f"Sorting file: {fileName}")
		lineCount = 0
		allLines = []
		with open(fileName,"r") as f:
			for line in f:
				lineCount += 1
				if lineCount == 1:
					continue
				if lineCount % 1000000 == 0:
					print (lineCount)
				allLines.append(line)

		# 2021-07-09 09:40:00.478177
		print ("Sorting...")
		allLines.sort(key = lambda x: (datetime.datetime.strptime(x.split(";")[modifiedTimeIndex], '%Y-%m-%d %H:%M:%S.%f'), int(x.split(';')[orderIdIndex],16), int(x.split(';')[seqNumIndex]) ) )
		print ("Sorted!")
		sortedFileFileName = fileName+"_sorted.csv"
		with open(sortedFileFileName, "w+") as f:
			f.write(headerStrings[assetType])
			lineCount = 0 
			for line in allLines:
				lineCount += 1
				if lineCount % 1000000 == 0:
					print (lineCount)
				f.write(line)


		shutil.move(sortedFileFileName, fileName)

def searchAndDump(mainFileName, timestampIndex, headerString, selection):
	filesMap = {}
	f = open(mainFileName)
	lineCount = 0
	fileNames = set()
	for line in f:
		lineCount += 1
		# Skip the first two lines because those are Borsa Istanbul native
		# header lines and we don't want those.

		if lineCount <= 2:
			continue
		if lineCount % 1000000 == 0:
			print ("Line processed: " + str(lineCount))
		try:
			timestampValue = line.split(';')[timestampIndex]
			timestampValue = timestampValue[:10] 
			if timestampValue not in filesMap:
				timestampPortion = getFileNameTimePortion(timestampValue)
				newFileName = selection + "_" + timestampPortion + ".csv"
				fileNames.add(newFileName)
				filesMap[timestampValue] = open(newFileName, 'w')
				filesMap[timestampValue].write(headerString)
				
			filesMap[timestampValue].write(line)
		except ValueError as e:
			print (e)

	
	return fileNames

						
# =============
headerStrings = {}
headerStrings["equities"] = "DATE;ORDER_NO;ENTRY_DATE_AND_TIME;MODIFIED_DATE_AND_TIME;TRANSACTION_CODE;BUY_SELL;ORDER_PRICE_TYPE;ORDER_TYPE;ORDER_CATEGORY;VALIDITY_TYPE;ORDER_STATUS;CHANGE_REASON;ORDER_AMOUNT;BALANCE;APPARENT_AMOUNT;PRICE;AGENCY_FUND_CODE_(AFC);SESSION;BEST_BID_PRICE;BEST_ASK_PRICE;PREVIOUS_ORDER_NR;TRADE_NUMBER;MATCH_QUANTITY;GIVE_UP_FLAG;UPDATE_NO;UPDATE_TIME\n"
headerStrings["options"] = "ORDER_NO;ENTRY_DATE_AND_TIME;INSTRUMENT_SERIES;MARKET;MARKET_SEGMENT;INSTRUMENT_TYPE;BUY_SELL;ORDER_STATUS;PRICE;ORDER_QUANTITY;SHOWN_QUANTITY;BALANCE;ORDER_TYPE;ORDER_PRICE_TYPE;ORDER_CATEGORY;TIME_VALIDITY_OF_ORDER;VALIDITY_TYPE;POSITION_CLOSING;TRIGGER_TRANSACTION_ID;TRIGGER_PRICE;TRIGGER_CONDITION;SESSION;CHANGE_REASON;MODIFIED_DATE_AND_TIME;ADVERTISING_ORDER;STATE;BEST_BID_PRICE;BEST_ASK_PRICE;OFF_HOURS;ALL_OR_NONE\n"

timestampIndices = {}
timestampIndices["equities"] = 3
timestampIndices["options"] = 23

assetType = sys.argv[1]
fileName = sys.argv[2]

fileNames = searchAndDump(fileName, timestampIndices[assetType], headerStrings[assetType], assetType)
# fileNames = ["equities_0709.csv"]
print (f"File names: {fileNames}")
sortEachFile(assetType, fileNames)













