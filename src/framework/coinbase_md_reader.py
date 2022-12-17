
import json

from pipe import Pipe
from limit_order import LimitOrder, Side, Operation
from market_order import MarketOrder
import time_constants

class CoinbaseMdReader(Pipe):

	def __init__(self,snapshotFile,limitOrderFile, flowCallback = None):
		Pipe.__init__(self,flowCallback)
		#It's extremely important that this size factor is the same as the one in limit order reader
		self.sizeFactor = 10000
		
		self.snapshotFileName = snapshotFile
		self.limitOrderFileName = limitOrderFile
		self.snapshotReader = open(snapshotFile)
		self.limitOrderReader = open(limitOrderFile)
		self.snapshotSequenceNumber = None
		self.nextExpectedSequenceNumber = 0
		self.lineCounter = 0

	def _getEventDict(self,line):
		lineSplit = line.split(';')
		eventDict = {}
		eventDict = json.loads(lineSplit[1])
		timestampNanos = int(lineSplit[0])
		return timestampNanos,eventDict

	def _processSnapshot(self, line, currentSequenceNumber):
		lineSplit = line.split(';')
		eventDict = {}
		eventDict = json.loads(lineSplit[1])
		snapshotSequenceNumber = int(eventDict['sequence'])
		timestamp = int(lineSplit[0])

		if snapshotSequenceNumber <= currentSequenceNumber:
			return snapshotSequenceNumber,False

		sides = ['bids','asks']

		for side in sides:
			for order in eventDict.get(side,[]):
				newLimitOrder = LimitOrder(
					orderId=order[2],
					price=float(order[0]),
					size=float(order[1])*self.sizeFactor,
					side= Side.BID if side == 'bids' else Side.ASK,
					timestamp=timestamp/time_constants.nanosInMilli,
					operation = Operation.ADD)

				self.produce(newLimitOrder)

		snapshotCompleteEvent = LimitOrder(operation=Operation.SNAPSHOT_COMPLETE)
		self.produce(snapshotCompleteEvent)

		return snapshotSequenceNumber,True

	# Fast forward to a seqeuence number after the current sequence number
	def _recoverGap(self,currentSequenceNumber):
		self.snapshotReader.close()
		self.snapshotReader = open(self.snapshotFileName)

		for line in self.snapshotReader:
			# Find snapshot sequence number greater than first sequence number after 
			# gap
			self.snapshotSequenceNumber,snapshotProcessed = self._processSnapshot(line,currentSequenceNumber)
			if not snapshotProcessed:
				continue

			self.nextExpectedSequenceNumber = self.snapshotSequenceNumber + 1

			return

		raise Exception(f"Cannot recover gap since a snapshot with greater sequence number than current sequence number {currentSequenceNumber} does not exist")


	# Detect if there is a gap in data. Recover the gap.
	# Return true if there is a gap. Return false otherwise
	def _detectGap(self,currentSequenceNumber):
		if self.nextExpectedSequenceNumber < currentSequenceNumber:
			print (f"Gap in data. Expected seq: {self.nextExpectedSequenceNumber}, Current seq: {currentSequenceNumber}")

			# First clear the book
			clearBook = LimitOrder(operation = Operation.CLEAR)
			self.produce(clearBook)
			
			self._recoverGap(currentSequenceNumber)
			print (f"Recovered gap. Next expected sequence number: {self.nextExpectedSequenceNumber} - Snapshot sequence number: {self.snapshotSequenceNumber}")
			return True
		elif self.nextExpectedSequenceNumber == currentSequenceNumber:
			self.nextExpectedSequenceNumber += 1
			return False
		else:
			#Next expected is greater than current sequence number.
			#This means data is coming unordered. We currently don't handle this case.
			raise (Exception(f"Unordered data: Expected sequence number: {self.nextExpectedSequenceNumber} - Current sequence number: {currentSequenceNumber}"))

	def _processLine(self,line, snapshotSequenceNumber):
		timestampNanos,eventDict = self._getEventDict(line)
		
		#Process events after snapshot's last event
		currentSequenceNumber = int(eventDict['sequence'])
		if currentSequenceNumber <= snapshotSequenceNumber:
			return

		if self._detectGap(currentSequenceNumber):
			return

		#Select relevant events
		if eventDict['type'] not in ['open','match','done','change']:
			return

		limitOrderUpdate = None
		marketOrderUpdate = None

		if eventDict['type'] == 'open':
			limitOrderUpdate = LimitOrder(
				orderId=eventDict["order_id"],
				price=float(eventDict['price']),
				size=float(eventDict['remaining_size'])*self.sizeFactor,
				side=Side.BID if eventDict['side'] == 'buy' else Side.ASK,
				timestamp=timestampNanos/time_constants.nanosInMilli,
				operation = Operation.ADD)
		elif eventDict['type'] == 'match':
			limitOrderUpdate = LimitOrder(
				orderId=eventDict["maker_order_id"],
				price=float(eventDict['price']),
				size=float(eventDict['size'])*self.sizeFactor,
				side=Side.BID if eventDict['side'] == 'buy' else Side.ASK,
				timestamp=timestampNanos/time_constants.nanosInMilli,
				operation = Operation.DECREASE)

			marketOrderUpdate = MarketOrder(
				orderId=eventDict["taker_order_id"],
				price=float(eventDict['price']),
				size=float(eventDict['size'])*self.sizeFactor,
				sell= eventDict['side'] != 'sell', #'side' tells us the market maker side
				timestamp=timestampNanos/time_constants.nanosInMilli)

		elif eventDict['type'] == 'done':
			limitOrderUpdate = LimitOrder(
				orderId=eventDict["order_id"],
				price=0,#'done' events don't have price field
				size=0,
				side=Side.BID if eventDict['side'] == 'buy' else Side.ASK,
				timestamp=timestampNanos/time_constants.nanosInMilli,
				operation = Operation.UPDATE)
		elif eventDict['type'] == 'change':
			limitOrderUpdate = LimitOrder(
				orderId=eventDict["order_id"],
				price=float(eventDict['price']),
				size=float(eventDict['new_size'])*self.sizeFactor,
				side=Side.BID if eventDict['side'] == 'buy' else Side.ASK,
				timestamp=timestampNanos/time_constants.nanosInMilli,
				operation = Operation.UPDATE)
		else:
			raise Exception("Unkown operation")

		if marketOrderUpdate is not None:
			self.produce(marketOrderUpdate)
		
		if limitOrderUpdate is not None:
			self.produce(limitOrderUpdate)

	def start(self):
		# Get the first sequence number in the limit order file name
		firstSequenceNumber = 0
		for line in self.limitOrderReader:
			timestampNanos,eventDict = self._getEventDict(line)
			firstSequenceNumber = int(eventDict['sequence'])
			break

		self.limitOrderReader.close()
		self.limitOrderReader = open(self.limitOrderFileName)

		print (f"First sequence number: {firstSequenceNumber}")

		#Need to start off with a snapshot
		self._detectGap(firstSequenceNumber)

		for line in self.limitOrderReader:
			self.lineCounter += 1
			if self.lineCounter % 100000 == 0:
				print (self.lineCounter)
			self._processLine(line, self.snapshotSequenceNumber)
		

