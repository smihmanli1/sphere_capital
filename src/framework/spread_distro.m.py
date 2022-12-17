
import sys
import os
import csv
import mmap
import pickle

from limit_order_book import LimitOrderBook
from limit_order_reader import LimitOrderReader
from pipeline import Pipeline


#Construct a dict of time->limit order book snapshot
snapshotFileName = sys.argv[1]
currentOrdersList = []

spreadCounter = 0
previousDate = 0
calculatedSpreads = []
with open(snapshotFileName) as csvfile:
	m = mmap.mmap(csvfile.fileno(), 0, prot=mmap.PROT_READ)
	currentRow = m.readline()
	while currentRow:
		currentRow = currentRow.decode("UTF-8")[:-1]
		
		currentRowSplit = currentRow.split(',')
		if (currentRowSplit[0] != 'date'):
			
			currentDate = int(currentRowSplit[0])
			if currentDate != previousDate and previousDate != 0:
				orderReader = LimitOrderReader(currentOrdersList)
				limitOrderBook = LimitOrderBook()
				orderPipeline = Pipeline([orderReader, limitOrderBook])
				orderPipeline.start()

				currentOrdersList = []

				calculatedSpreads.append(limitOrderBook.spread())
				print (spreadCounter)
				spreadCounter+=1

				# if spreadCounter == 20:
					# break

			currentOrdersList.append(currentRowSplit)
			previousDate = currentDate

		currentRow = m.readline()


#Pickle the spreads list
spreadsDistroFileName = f"{snapshotFileName}_spread_distro.dat"
spreadsDistroFile = open(spreadsDistroFileName, 'ab') 
pickle.dump(calculatedSpreads, spreadsDistroFile)                      
spreadsDistroFile.close() 