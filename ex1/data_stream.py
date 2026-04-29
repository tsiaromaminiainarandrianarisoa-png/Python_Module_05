import abc
import typing
from typing import Any

class DataProcessor(abc.ABC):
	def __init__(self)->None:
		self.rank = 0
		self.storage: list = []
		self.processed = 0
	@abc.abstractmethod
	def validate(self, data: Any)-> bool:
		pass
	@abc.abstractmethod
	def ingest(self, data: Any)-> None:
		pass
	def output(self)->tuple[int, str]:
		result = (self.storage).pop(0)
		self.rank += 1
		return ((self.rank - 1), result)

class NumericProcessor(DataProcessor):
	def __init__(self)-> None:
		self.name = "Numeric Processor"
		super().__init__()
	def validate(self, data: Any)->bool:
		state = isinstance(data, int)
		if not state:
			state = isinstance(data, float)
		if isinstance(data, list):
			for element in data:
				state = isinstance(element, int)
				if not state:
					state = isinstance(element, float)
				if not state:
					break
		return state
	def ingest(self, data: int | float | list)->None:
		if not self.validate(data):
			raise ValueError("Improper numeric data")
		else:
			if isinstance(data, list):
				for element in data:
					(self.storage).append(str(element))
					self.processed += 1
			else:
				(self.storage).append(str(data))
				self.processed += 1

class TextProcessor(DataProcessor):
	def __init__(self)-> None:
		self.name = "Text Processor"
		super().__init__()
	def validate(self, data: Any)->bool:
		state = isinstance(data, str)
		if isinstance(data, list):
			for element in data:
				state = isinstance(element, str)
				if not state:
					break
		return state
	def ingest(self, data: str | list)->None:
		if not self.validate(data):
			raise ValueError("Improper text data")
		else:
			if isinstance(data, list):
				for element in data:
					(self.storage).append(element)
					self.processed += 1
			else:
				(self.storage).append(data)
				self.processed += 1

class LogProcessor(DataProcessor):
	def __init__(self)-> None:
		self.name = "Log Processor"
		super().__init__()
	def validate(self, data: Any)->bool:
		state = isinstance(data, dict)
		if isinstance(data, list):
			for element in data:
				state = isinstance(element, dict)
				if not state:
					break
		return state
	def ingest(self, data: dict | list)->None:
		if not self.validate(data):
			raise ValueError("Improper log data")
		else:
			if isinstance(data, list):
				for element in data:
					(self.storage).append(f"{element['log_level']}: {element['log_message']}")
					self.processed += 1
			else:
				(self.storage).append(f"{data['log_level']}: {data['log_message']}")
				self.processed += 1

class DataStream:
	def __init__(self)-> None:
		self.proc: list = []
	def register_processor(self, proc: DataProcessor) -> None:
		(self.proc).append(proc)
	def process_stream(self, stream: list[typing.Any]) -> None:
		for data in stream:
			state = False
			for proc in self.proc:
				if proc.validate(data):
					proc.ingest(data)
					state = True
					break
			if not state:
				print(f"DataStream error - Can't process element in stream: {data}")
	def print_processors_stats(self) -> None:
		if len(self.proc) == 0:
			print("No processor found, no data")
		else:
			for proc in self.proc:
				remaining = len(proc.storage)
				print(f"{proc.name}: total {proc.processed} items processed, remaining {remaining} on processor")

if __name__=="__main__":
	print("=== Code Nexus - Data Stream ===\n")
	print("Initialize Data Stream...")
	sys = DataStream()
	print("== DataStream statistics ==")
	sys.print_processors_stats()
	print("\nRegistering Numeric Processor\n")
	sys.register_processor(NumericProcessor())
	batch = ['Hello world', [3.14, -1, 2.71], [{'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'}, {'log_level': 'INFO', 'log_message': 'User wil is connected'}], 42, ['Hi', 'five']]
	print(f"Send first batch of data on stream: {batch}")
	sys.process_stream(batch)
	print("== DataStream statistics ==")
	sys.print_processors_stats()
	print("\nRegistering other data processors")
	sys.register_processor(TextProcessor())
	sys.register_processor(LogProcessor())
	print("Send the same batch again")
	sys.process_stream(batch)
	print("== DataStream statistics ==")
	sys.print_processors_stats()
	print("\nConsume some elements from the data processors: Numeric 3, Text 2, Log 1")
	consume = [3,2,1]
	loc = 0
	for proc in sys.proc:
		for n in range(consume[loc]):
			proc.output()
		loc += 1
	print("== DataStream statistics ==")
	sys.print_processors_stats()
