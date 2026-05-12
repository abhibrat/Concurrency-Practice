from __future__ import annotations

from collections import Counter
from collections.abc import Callable
import random
import threading
import time
from typing import Protocol

from concurrency_practice.verifiers.common import (
    EventLog,
    VerificationError,
    run_threads,
    sleep_random,
)


class BoundedBlockingQueueSolution(Protocol):
    def enqueue(self, item: int) -> None:
        ...

    def dequeue(self) -> int:
        ...

    def size(self) -> int:
        ...


class BoundedBlockingQueueVerifier:
    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self.log = EventLog()

    def assert_size(
        self,
        queue: BoundedBlockingQueueSolution,
        expected: int,
    ) -> None:
        actual = queue.size()
        self.log.record(f"size#{actual}")
        if actual != expected:
            raise VerificationError(
                f"size() returned {actual}; expected {expected}; "
                f"log=[{self.log.compact()}]"
            )
        if not 0 <= actual <= self.capacity:
            raise VerificationError(
                f"size() returned out-of-bounds value {actual}; "
                f"capacity={self.capacity}; log=[{self.log.compact()}]"
            )


def verify_bounded_blocking_queue(
    solution_factory: Callable[[int], BoundedBlockingQueueSolution],
    *,
    capacity: int = 3,
    producer_threads: int = 4,
    consumer_threads: int = 4,
    items_per_producer: int = 8,
    runs: int = 50,
    timeout: float = 3.0,
    max_jitter: float = 0.001,
) -> None:
    """Verify a bounded FIFO blocking queue."""

    if capacity <= 0:
        raise ValueError("capacity must be positive")
    if producer_threads <= 0 or consumer_threads <= 0:
        raise ValueError("producer_threads and consumer_threads must be positive")
    total_items = producer_threads * items_per_producer
    if total_items % consumer_threads != 0:
        raise ValueError("total produced items must divide evenly among consumers")

    _assert_single_thread_fifo(solution_factory, capacity=capacity)
    _assert_dequeue_blocks_until_item(
        solution_factory,
        capacity=capacity,
        timeout=timeout,
        max_jitter=max_jitter,
    )
    _assert_enqueue_blocks_until_space(
        solution_factory,
        capacity=capacity,
        timeout=timeout,
        max_jitter=max_jitter,
    )

    for run in range(runs):
        _run_stress(
            solution_factory,
            capacity=capacity,
            producer_threads=producer_threads,
            consumer_threads=consumer_threads,
            items_per_producer=items_per_producer,
            seed=run,
            timeout=timeout,
            max_jitter=max_jitter,
        )


def _assert_single_thread_fifo(
    solution_factory: Callable[[int], BoundedBlockingQueueSolution],
    *,
    capacity: int,
) -> None:
    queue = solution_factory(capacity)
    verifier = BoundedBlockingQueueVerifier(capacity)
    verifier.assert_size(queue, 0)

    for item in range(capacity):
        _enqueue_once(queue, item, verifier.log)
        verifier.assert_size(queue, item + 1)

    for item in range(capacity):
        actual = _dequeue_once(queue, verifier.log)
        if actual != item:
            raise VerificationError(
                f"dequeue() returned {actual}; expected FIFO item {item}; "
                f"log=[{verifier.log.compact()}]"
            )
        verifier.assert_size(queue, capacity - item - 1)


def _assert_dequeue_blocks_until_item(
    solution_factory: Callable[[int], BoundedBlockingQueueSolution],
    *,
    capacity: int,
    timeout: float,
    max_jitter: float,
) -> None:
    queue = solution_factory(capacity)
    log = EventLog()
    consumer_entered = threading.Event()
    consumer_done = threading.Event()
    producer_start = threading.Event()
    consumed: list[int] = []
    consumed_lock = threading.Lock()

    def consumer() -> None:
        consumer_entered.set()
        item = _dequeue_once(queue, log)
        with consumed_lock:
            consumed.append(item)
        consumer_done.set()

    def producer() -> None:
        _wait_for_gate(producer_start, timeout, "producer")
        _enqueue_once(queue, 123, log)

    def after_start() -> None:
        try:
            _wait_for_gate(consumer_entered, timeout / 2, "consumer start")
            time.sleep(max(0.02, max_jitter * 20))
            if consumer_done.is_set():
                raise VerificationError(
                    "dequeue() returned while the queue was empty; "
                    f"log=[{log.compact()}]"
                )
        finally:
            producer_start.set()

    run_threads(
        {
            "consumer": consumer,
            "producer": producer,
        },
        timeout=timeout,
        after_start=after_start,
    )

    if consumed != [123]:
        raise VerificationError(
            f"consumer received {consumed}; expected [123]; log=[{log.compact()}]"
        )
    BoundedBlockingQueueVerifier(capacity).assert_size(queue, 0)


def _assert_enqueue_blocks_until_space(
    solution_factory: Callable[[int], BoundedBlockingQueueSolution],
    *,
    capacity: int,
    timeout: float,
    max_jitter: float,
) -> None:
    queue = solution_factory(capacity)
    log = EventLog()

    for item in range(capacity):
        _enqueue_once(queue, item, log)

    BoundedBlockingQueueVerifier(capacity).assert_size(queue, capacity)

    producer_entered = threading.Event()
    producer_done = threading.Event()
    consumer_start = threading.Event()
    consumed: list[int] = []
    consumed_lock = threading.Lock()

    def producer() -> None:
        producer_entered.set()
        _enqueue_once(queue, 999, log)
        producer_done.set()

    def consumer() -> None:
        _wait_for_gate(consumer_start, timeout, "consumer")
        item = _dequeue_once(queue, log)
        with consumed_lock:
            consumed.append(item)

    def after_start() -> None:
        try:
            _wait_for_gate(producer_entered, timeout / 2, "producer start")
            time.sleep(max(0.02, max_jitter * 20))
            if producer_done.is_set():
                raise VerificationError(
                    "enqueue() returned while the queue was full; "
                    f"log=[{log.compact()}]"
                )
        finally:
            consumer_start.set()

    run_threads(
        {
            "producer": producer,
            "consumer": consumer,
        },
        timeout=timeout,
        after_start=after_start,
    )

    drained = [_dequeue_once(queue, log) for _ in range(capacity)]
    actual = consumed + drained
    expected = [*range(capacity), 999]
    if actual != expected:
        raise VerificationError(
            f"queue order after blocked enqueue was {actual}; expected {expected}; "
            f"log=[{log.compact()}]"
        )
    BoundedBlockingQueueVerifier(capacity).assert_size(queue, 0)


def _run_stress(
    solution_factory: Callable[[int], BoundedBlockingQueueSolution],
    *,
    capacity: int,
    producer_threads: int,
    consumer_threads: int,
    items_per_producer: int,
    seed: int,
    timeout: float,
    max_jitter: float,
) -> None:
    queue = solution_factory(capacity)
    log = EventLog()
    total_items = producer_threads * items_per_producer
    items_per_consumer = total_items // consumer_threads
    consumed: list[int] = []
    consumed_lock = threading.Lock()

    def producer(producer_id: int) -> None:
        rng = random.Random((seed + 1) * 10_000 + producer_id)
        start = producer_id * items_per_producer
        for item in range(start, start + items_per_producer):
            sleep_random(rng, max_jitter)
            _enqueue_once(queue, item, log)

    def consumer(consumer_id: int) -> None:
        rng = random.Random((seed + 1) * 20_000 + consumer_id)
        for _ in range(items_per_consumer):
            sleep_random(rng, max_jitter)
            item = _dequeue_once(queue, log)
            with consumed_lock:
                consumed.append(item)

    run_threads(
        {
            **{
                f"P{producer_id}": (
                    lambda producer_id=producer_id: producer(producer_id)
                )
                for producer_id in range(producer_threads)
            },
            **{
                f"C{consumer_id}": (
                    lambda consumer_id=consumer_id: consumer(consumer_id)
                )
                for consumer_id in range(consumer_threads)
            },
        },
        timeout=timeout,
    )

    expected_items = set(range(total_items))
    consumed_counts = Counter(consumed)
    duplicates = [item for item, count in consumed_counts.items() if count > 1]
    if set(consumed) != expected_items or duplicates:
        raise VerificationError(
            f"consumed items mismatch; expected={sorted(expected_items)}, "
            f"actual={consumed}, duplicates={duplicates}; log=[{log.compact()}]"
        )

    actual_size = queue.size()
    log.record(f"size#{actual_size}")
    if actual_size != 0:
        raise VerificationError(
            f"queue not empty after stress run; size={actual_size}; "
            f"log=[{log.compact()}]"
        )


def _enqueue_once(
    queue: BoundedBlockingQueueSolution,
    item: int,
    log: EventLog,
) -> None:
    result = queue.enqueue(item)
    log.record(f"enqueue#{item}")
    if result is not None:
        raise VerificationError(
            f"enqueue({item}) returned {result!r}; expected None; "
            f"log=[{log.compact()}]"
        )


def _dequeue_once(queue: BoundedBlockingQueueSolution, log: EventLog) -> int:
    item = queue.dequeue()
    log.record(f"dequeue#{item}")
    return item


def _wait_for_gate(gate: threading.Event, timeout: float, name: str) -> None:
    if not gate.wait(timeout):
        raise VerificationError(f"{name} was never released by the verifier")
