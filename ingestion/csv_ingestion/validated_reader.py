import json
from collections.abc import Iterator
from typing import TextIO

from src.models import Order


def read_validated_orders(
    file: TextIO,
) -> Iterator[Order]:
    """Read validated orders from a JSONL file."""

    for line in file:
        data = json.loads(line)

        yield Order.model_validate(data)
