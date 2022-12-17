
import sys

from coinbase_md_reader import CoinbaseMdReader
from printer_pipe import PrinterPipe
from pipeline import Pipeline

snapshotFileName = sys.argv[1] 
l3FileName = sys.argv[2]

mdReader = CoinbaseMdReader(snapshotFileName, l3FileName)
printerPipe = PrinterPipe()
mdReaderTestPipeline = Pipeline([mdReader,printerPipe])

mdReaderTestPipeline.start()