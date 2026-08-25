import csv
import json
from collections.abc import Iterator
from pathlib import Path
from dataclasses import dataclass
from pydantic import ValidationError

from tempfile import NamedTemporaryFile
from typing import TextIO

from src.models import Order

MAX_REJECTION_RATE = 0.01

@dataclass
class ValidationStats:
    total_rows: int = 0
    rejected_rows: int = 0

    @property
    def valid_rows(self) -> int:
        return self.total_rows - self.rejected_rows

    @property
    def rejection_rate(self) -> float:
        if self.total_rows == 0:
            return 0.0

        return self.rejected_rows / self.total_rows


def read_csv_rows(path: Path) -> Iterator[dict[str, str]]:
    """
    Stream rows from a CSV file.

    The entire CSV is never loaded into memory.
    """

    with path.open(
        "r",
        newline="",
        encoding="utf-8",
    ) as file:
        reader = csv.DictReader(file)

        for row in reader:
            yield row


def validate_rows(
    rows: Iterator[dict[str, str]],
    dlq_path: Path,
    stats: ValidationStats,
) -> Iterator[Order]:
    """Validate rows and send invalid records to the DLQ."""

    with dlq_path.open(
        "a",
        encoding="utf-8",
    ) as dlq:

        for row in rows:
            stats.total_rows += 1

            try:
                yield Order.model_validate(row)

            except ValidationError as exc:
                stats.rejected_rows += 1

                dlq_record = {
                    "row": row,
                    "error": exc.errors(),
                }

                dlq.write(
                    json.dumps(
                        dlq_record,
                        default=str,
                    )
                    + "\n"
                )


def check_rejection_rate(stats: ValidationStats) -> None:
	if stats.rejection_rate > MAX_REJECTION_RATE:
		raise ValueError(
			f"Too many rejected rows: "
			f"{stats.rejected_rows}/{stats.total_rows}"
			f"({stats.rejection_rate:.2%})"
		)




def validate_to_tempfile(
    input_path: Path,
    dlq_path: Path,
) -> tuple[TextIO, ValidationStats]:
    """Validate a CSV and store valid rows in a temporary JSONL file."""

    stats = ValidationStats()

    temp_file = NamedTemporaryFile(
        mode="w+",
        encoding="utf-8",
        suffix=".jsonl",
        delete=True,
    )

    rows = read_csv_rows(input_path)

    for order in validate_rows(rows, dlq_path, stats):
        temp_file.write(
            json.dumps(
                order.model_dump(mode="json"),
            )
            + "\n"
        )

    temp_file.flush()
    temp_file.seek(0)

    return temp_file, stats
