from collections.abc import Iterable, Iterator

from itertools import islice
from typing import TypeVar

T = TypeVar("T")

def batched(iterable: Iterable[T], size: int,) -> Iterator[list[T]]:

	if size <=0:
		raise ValueError("batch must be greator than zero")

	iterator =iter(iterable)

	while True:
		batch = list(islice(iterator, size))

		if not batch:
			break
		yield batch
