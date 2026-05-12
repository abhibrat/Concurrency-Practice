"""Bounded blocking queue exercise.

Implement `BoundedBlockingQueue.enqueue()`, `BoundedBlockingQueue.dequeue()`,
and `BoundedBlockingQueue.size()`.

This is the finite-buffer producer-consumer pattern from The Little Book of
Semaphores, but the queue owns the buffer instead of receiving verifier-owned
`put` and `get` callbacks.

Rules:
- `enqueue(item)` adds one item to the back of the queue.
- If the queue is full, `enqueue(item)` must block until space is available.
- `dequeue()` removes and returns one item from the front of the queue.
- If the queue is empty, `dequeue()` must block until an item is available.
- `size()` returns the current number of queued items.

Run:
    python -m pytest tests/test_bounded_blocking_queue.py -q

Stress:
    BOUNDED_BLOCKING_QUEUE_STRESS_RUNS=200 BOUNDED_BLOCKING_QUEUE_CAPACITY=3 \
        python -m pytest tests/test_bounded_blocking_queue.py -q
"""


class BoundedBlockingQueue:
    """Starter solution for the bounded blocking queue exercise."""

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self._queue: list[int] = []
        # Put semaphores, locks, counters, or conditions here.

    def enqueue(self, item: int) -> None:
        # TODO: wait while full, add item exclusively, then signal a waiting
        # dequeuer that an item is available.
        self._queue.append(item)

    def dequeue(self) -> int:
        # TODO: wait while empty, remove one item exclusively, then signal a
        # waiting enqueuer that space is available.
        return self._queue.pop(0)

    def size(self) -> int:
        # TODO: return the queue length safely when other threads are active.
        return len(self._queue)
