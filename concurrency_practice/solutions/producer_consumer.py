"""Finite-buffer producer-consumer exercise.

Implement `ProducerConsumer.produce()` and `ProducerConsumer.consume()`.

The verifier owns the actual buffer and gives you callbacks:
- `put(item)` adds one item to the shared buffer.
- `get()` removes and returns one item from the shared buffer.

Verifier invariants:
- Buffer access is exclusive.
- Buffer size never goes below 0 or above `capacity`.
- Consumers block while the buffer is empty.
- Producers block while the buffer is full.
- Every consumed item was produced.
- No item is consumed more than once.

Run:
    python -m pytest tests/test_producer_consumer.py -q

Stress:
    PRODUCER_CONSUMER_STRESS_RUNS=200 PRODUCER_CONSUMER_CAPACITY=3 \
        python -m pytest tests/test_producer_consumer.py -q
"""

from collections.abc import Callable
from threading import Semaphore


class ProducerConsumer:
    """Starter solution for the finite-buffer producer-consumer exercise."""

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self._mutex = Semaphore(1)
        self._items = Semaphore(0)
        self._spaces = Semaphore(capacity)

    def produce(self, item: int, put: Callable[[int], None]) -> None:
        self._spaces.acquire()
        self._mutex.acquire()
        try:
            put(item)
        finally:
            self._mutex.release()
        self._items.release()

    def consume(self, get: Callable[[], int]) -> int:
        self._items.acquire()
        self._mutex.acquire()
        try:
            item = get()
        finally:
            self._mutex.release()
        self._spaces.release()
        return item
