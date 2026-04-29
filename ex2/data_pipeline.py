import abc
import typing
from typing import Protocol
from typing import Any

class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass

class CSVExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        values = []
        for item in data:
            values.append(item[1])
        csvformat = ",".join(values)
        print("CSV Output:")
        print(csvformat)

class JSONExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        keys = []
        values = []
        for item in data:
            keys.append(item[0])
            values.append(item[1])
        jsonformat = ""
        for n in range(len(data)):
            if n == 0:
                jsonformat += f"\"item_{str(keys[n])}\": \"{values[n]}\""
            else:
                jsonformat += f" \"item_{str(keys[n])}\": \"{values[n]}\""
            if n != (len(data)-1):
                jsonformat += ","
        final_format = "{" + jsonformat + "}"
        print(f"JSON Output:\n{final_format}")

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
	def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
		for proc in self.proc:
			res: list = []
			for n in range(nb):
				if len(proc.storage) > 0:
					res.append(proc.output())
			plugin.process_output(res)

if __name__=="__main__":
    print("=== Code Nexus - Data Pipeline ===\n")
    print("Initialize Data Stream...\n")
    sys = DataStream()
    print("== DataStream statistics ==")
    sys.print_processors_stats()
    print("\nRegistering Processors\n")
    sys.register_processor(NumericProcessor())
    sys.register_processor(TextProcessor())
    sys.register_processor(LogProcessor())
    batch = ['Hello world', [3.14, -1, 2.71], [{'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'}, {'log_level': 'INFO', 'log_message': 'User wil is connected'}], 42, ['Hi', 'five']]
    print(f"Send first batch of data on stream: {batch}\n")
    sys.process_stream(batch)
    print("== DataStream statistics ==")
    sys.print_processors_stats()
    print("Send 3 processed data from each processor to a CSV plugin:")
    sys.output_pipeline(3, CSVExportPlugin())
    print("\n== DataStream statistics ==")
    sys.print_processors_stats()
    batch2 = [21, ['I love AI', 'LLMs are wonderful', 'Stay healthy'], [{'log_level': 'ERROR', 'log_message': '500 server crash'}, {'log_level': 'NOTICE', 'log_message': 'Certificate expires in 10 days'}], [32, 42, 64, 84, 128, 168], 'World hello']
    print(f"\nSend another batch of data: {batch2}")
    sys.process_stream(batch2)
    print("\n== DataStream statistics ==")
    sys.print_processors_stats()
    print("Send 5 processed data from each processor to a JSON plugin:")
    sys.output_pipeline(5, JSONExportPlugin())
    print("\n== DataStream statistics ==")
    sys.print_processors_stats()
