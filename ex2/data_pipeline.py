from typing import Protocol, Any
from abc import ABC, abstractmethod


class colors:
    GREEN = "\033[0;92m"
    RED = "\033[0;91m"
    BLUE = "\033[0;94m"
    MAGENTA = "\033[0;95m"
    RESET = "\033[0m"

    def green(self, text: str) -> str:
        return f"{self.GREEN}{text}{self.RESET}"

    def red(self, text: str) -> str:
        return f"{self.RED}{text}{self.RESET}"

    def blue(self, text: str) -> str:
        return f"{self.BLUE}{text}{self.RESET}"

    def magenta(self, text: str) -> str:
        return f"{self.MAGENTA}{text}{self.RESET}"


color = colors()


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.data: list[str] = []
        self.rank = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        if not self.data:
            raise IndexError("No data available")

        processed_data = self.data.pop(0)
        current_rank = self.rank
        self.rank += 1

        return current_rank, processed_data


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, bool):
            return False

        if isinstance(data, (int, float)):
            return True

        if isinstance(data, list):
            return all(
                isinstance(item, (int, float))
                and not isinstance(item, bool)
                for item in data
            )

        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")

        if isinstance(data, list):
            self.data.extend(str(item) for item in data)
        else:
            self.data.append(str(data))


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True

        if isinstance(data, list):
            return all(isinstance(item, str) for item in data)

        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")

        if isinstance(data, list):
            self.data.extend(data)
        else:
            self.data.append(data)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return (
                all(isinstance(key, str) for key in data)
                and all(isinstance(value, str) for value in data.values())
            )

        if isinstance(data, list):
            return all(
                isinstance(item, dict)
                and all(isinstance(key, str) for key in item)
                and all(isinstance(value, str) for value in item.values())
                for item in data
            )

        return False

    def ingest(
        self,
        data: dict[str, str] | list[dict[str, str]]
    ) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        log_entries = data if isinstance(data, list) else [data]

        for entry in log_entries:
            if "log_level" in entry and "log_message" in entry:
                formatted_log = (
                    f"{entry['log_level']}: {entry['log_message']}"
                )
            else:
                formatted_log = ", ".join(
                    f"{key}: {value}" for key, value in entry.items()
                )

            self.data.append(formatted_log)


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)
        print(color.green(f"Registered {type(proc).__name__}"))

    def process_stream(self, stream: list[Any]) -> None:
        for item in stream:
            processor_found = False

            for proc in self.processors:
                if proc.validate(item):
                    proc.ingest(item)
                    processor_found = True
                    break

            if not processor_found:
                print(
                    color.red(
                        f"Item {item} does not match any "
                        "processor's validation criteria"
                    )
                )

    def print_processors_stats(self) -> None:
        if not self.processors:
            print(color.red("No processor found, no data"))
            return

        print(color.magenta("\n=== Processor Statistics ==="))

        for proc in self.processors:
            total = len(proc.data) + proc.rank

            print(
                f"{color.magenta(type(proc).__name__)}: "
                f"{color.blue(f'total {total} items processed')}, "
                f"{color.green(f'remaining {len(proc.data)}')}"
            )

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for processor in self.processors:
            data = []

            for _ in range(nb):
                if not processor.data:
                    break
                data.append(processor.output())

            if data:
                plugin.process_output(data)


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVPlugin(ExportPlugin):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        values = [item[1] for item in data]
        print(",".join(values))


class JSONPlugin(ExportPlugin):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        values = [f'"item_{item[0]}": "{item[1]}"' for item in data]
        print("{" + ", ".join(values) + "}")


def main() -> None:
    stream = DataStream()

    numeric_processor = NumericProcessor()
    text_processor = TextProcessor()
    log_processor = LogProcessor()

    stream.register_processor(numeric_processor)
    stream.register_processor(text_processor)
    stream.register_processor(log_processor)

    batch_one = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead"
            },
            {
                "log_level": "INFO",
                "log_message": "User wil is connected"
            }
        ],
        42,
        ["Hi", "five"]
    ]

    print(color.magenta("\n=== First Batch ==="))
    stream.process_stream(batch_one)
    stream.print_processors_stats()

    print(color.magenta("\n=== CSV Export (3 items per processor) ==="))
    stream.output_pipeline(3, CSVPlugin())
    stream.print_processors_stats()

    batch_two = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {
                "log_level": "ERROR",
                "log_message": "500 server crash"
            },
            {
                "log_level": "NOTICE",
                "log_message": "Certificate expires in 10 days"
            }
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello"
    ]

    print(color.magenta("\n=== Second Batch ==="))
    stream.process_stream(batch_two)
    stream.print_processors_stats()

    print(color.magenta("\n=== JSON Export (5 items per processor) ==="))
    stream.output_pipeline(5, JSONPlugin())
    stream.print_processors_stats()


if __name__ == "__main__":
    main()
