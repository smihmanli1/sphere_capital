

class Pipeline:

	def __init__(self,pipes):
		self.headPipe = pipes[0] if len(pipes) != 0 else None

		for i in range(len(pipes)-1):
			pipes[i]._setNextModule(pipes[i+1])

	def start(self):
		if self.headPipe is None:
			return
		self.headPipe.start()