from typing import Any
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


def main() -> None:
    print("=== Code Nexus - Data Processor ===")

    # Numeric Processor
    print("\nTesting Numeric Processor...")
    numeric_processor = NumericProcessor()

    print(f"Trying to validate input '{color.green("42")}': "
          f"{numeric_processor.validate(42)}")
    print(f"Trying to validate input '{color.green("Hello")}': "
          f"{numeric_processor.validate('Hello')}")

    print(f"Test invalid ingestion of string '{
        color.red("foo")}' without prior validation:")
    try:
        numeric_processor.ingest("foo")     # type: ignore[arg-type]
    except ValueError as error:
        print(f"{color.red('Got exception')}: {error}")

    numbers = [1, 2, 3, 4, 5]
    print(f"Processing data: {color.blue(str(numbers))}")
    numeric_processor.ingest(numbers)   # type: ignore[arg-type]

    print("Extracting 3 values...")
    for index in range(3):
        rank, value = numeric_processor.output()
        print(f"Numeric value {color.magenta(str(rank))}: {value}")

    # Text Processor
    print("\nTesting Text Processor...")
    text_processor = TextProcessor()

    print(f"Trying to validate input '{color.green("42")}': "
          f"{text_processor.validate(42)}")

    words = ["Hello", "Nexus", "World"]
    print(f"Processing data: {color.blue(str(words))}")
    text_processor.ingest(words)

    print("Extracting 1 value...")
    rank, value = text_processor.output()
    print(f"Text value {color.magenta(str(rank))}: {value}")

    # Log Processor
    print("\nTesting Log Processor...")
    log_processor = LogProcessor()

    print(f"Trying to validate input '{color.green("Hello")}': "
          f"{log_processor.validate('Hello')}")

    logs = [
        {
            "log_level": "NOTICE",
            "log_message": "Connection to server"
        },
        {
            "log_level": "ERROR",
            "log_message": "Unauthorized access!!"
        }
    ]

    print(f"Processing data: {color.blue(str(logs))}")
    log_processor.ingest(logs)

    print("Extracting 2 values...")
    for index in range(2):
        rank, value = log_processor.output()
        print(f"Log entry {color.magenta(str(rank))}: {value}")


if __name__ == "__main__":
    main()
