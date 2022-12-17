
from pipe import Pipe

class PrinterPipe(Pipe):

	def __init__(self):
		Pipe.__init__(self)

	def accept(self,data):
		print (data)