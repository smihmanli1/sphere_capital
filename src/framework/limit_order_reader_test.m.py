import sys

from limit_order_reader2 import LimitOrderReader2
from printer_pipe import PrinterPipe
from pipeline import Pipeline


limitOrderFile = sys.argv[1]

limitOrderReader = LimitOrderReader2(limitOrderFile)
printerPipe = PrinterPipe()


p = Pipeline([limitOrderReader,printerPipe])
while limitOrderReader.getNext():
	pass
