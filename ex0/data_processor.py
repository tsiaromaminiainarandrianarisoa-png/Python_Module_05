import abc
from typing import Any

class DataProcessor(abc.ABC):
	def __init__(self)->None:
		self.rank = 0
		self.storage: list = []
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
			else:
				(self.storage).append(str(data))

class TextProcessor(DataProcessor):
	def __init__(self)-> None:
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
			else:
				(self.storage).append(data)
class LogProcessor(DataProcessor):
	def __init__(self)-> None:
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
			else:
				(self.storage).append(f"{data['log_level']}: {data['log_message']}")

if __name__=="__main__":
	print("=== Code Nexus - Data Processor ===\n")
	print("Testing Numeric Processor...")
	num = NumericProcessor()
	test_data = (42, "Hello")
	for data in test_data:
		print(f" Trying to validate input '{data}': {num.validate(data)}")
	test = 'foo'
	print(f" Test invalid ingestion of string '{test}' without prior validation:")
	try:
		num.ingest(test)
	except ValueError as error:
		print(f" Got exception: {error}")
	data_one = [1, 2, 3, 4, 5]
	print(f" Processing data: {data_one}")
	num.ingest(data_one)
	for n in range(3):
		res = num.output()
		print(f" Numeric value {res[0]}: {res[1]}")
	print("\nTesting Text Processor...")
	text = TextProcessor()
	print(f" Trying to validate input '42': {text.validate(42)}")
	data_two = ['Hello', 'Nexus', 'World']
	print(f" Processing data: {data_two}")
	text.ingest(data_two)
	print(" Extracting 1 value...")
	res = text.output()
	print(f" Text value {res[0]}: {res[1]}")
	print("\nTesting Log Processor...")
	log = LogProcessor()
	print(f" Trying to validate input 'Hello': {log.validate('Hello')}")
	data_three =  [{'log_level': 'NOTICE', 'log_message': 'Connection to server'}, {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}]
	print(f" Processing data: {data_three}")
	log.ingest(data_three)
	print(" Extracting 2 values...")
	for n in range(2):
		res = log.output()
		print(f" Log entry {res[0]}: {res[1]}")
