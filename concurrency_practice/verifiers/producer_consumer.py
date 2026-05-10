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


class ProducerConsumerSolution(Protocol):
    def produce(self, item: int, put: Callable[[int], None]) -> None:
        ...

    def consume(self, get: Callable[[], int]) -> int:
        ...


class ProducerConsumerVerifier:
    def __init__(
        self,
        capacity: int,
        *,
        seed: int = 0,
        max_jitter: float = 0.001,
    ) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self.log = EventLog()
        self._buffer: list[int] = []
        self._produced: set[int] = set()
        self._consumed: list[int] = []
        self._access = threading.Lock()
        self._state = threading.Condition()
        self._rng = random.Random(seed)
        self._rng_lock = threading.Lock()
        self._max_jitter = max_jitter

    def put(self, item: int) -> None:
        self._jitter()
        if not self._access.acquire(blocking=False):
            raise VerificationError(
                f"buffer accessed concurrently during put({item}); "
                f"log=[{self.log.compact()}]"
            )
        try:
            self._jitter()
            with self._state:
                if item in self._produced:
                    raise VerificationError(
                        f"item {item} was produced more than once; "
                        f"log=[{self.log.compact()}]"
                    )
                if len(self._buffer) >= self.capacity:
                    raise VerificationError(
                        f"put({item}) would exceed capacity {self.capacity}; "
                        f"buffer_size={len(self._buffer)}, log=[{self.log.compact()}]"
                    )
                self._buffer.append(item)
                self._produced.add(item)
                self.log.record(f"put#{item}")
                self._state.notify_all()
        finally:
            self._access.release()
        self._jitter()

    def get(self) -> int:
        self._jitter()
        if not self._access.acquire(blocking=False):
            raise VerificationError(
                f"buffer accessed concurrently during get(); "
                f"log=[{self.log.compact()}]"
            )
        try:
            self._jitter()
            with self._state:
                if not self._buffer:
                    raise VerificationError(
                        f"get() would make buffer size negative; "
                        f"log=[{self.log.compact()}]"
                    )
                item = self._buffer.pop(0)
                if item in self._consumed:
                    raise VerificationError(
                        f"item {item} was consumed more than once; "
                        f"log=[{self.log.compact()}]"
                    )
                self._consumed.append(item)
                self.log.record(f"get#{item}")
                self._state.notify_all()
                return item
        finally:
            self._access.release()

    def wait_for_buffer_size(self, expected_size: int, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        with self._state:
            while len(self._buffer) < expected_size:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._state.wait(remaining)
            return True

    def assert_complete(self, expected_items: set[int]) -> None:
        with self._state:
            consumed_counts = Counter(self._consumed)
            duplicates = [
                item for item, count in consumed_counts.items() if count > 1
            ]
            if self._produced != expected_items:
                raise VerificationError(
                    f"produced items mismatch; expected={sorted(expected_items)}, "
                    f"actual={sorted(self._produced)}, log=[{self.log.compact()}]"
                )
            if set(self._consumed) != expected_items or duplicates:
                raise VerificationError(
                    f"consumed items mismatch; expected={sorted(expected_items)}, "
                    f"actual={self._consumed}, duplicates={duplicates}, "
                    f"log=[{self.log.compact()}]"
                )
            if self._buffer:
                raise VerificationError(
                    f"buffer not empty after test; buffer={self._buffer}, "
                    f"log=[{self.log.compact()}]"
                )

    def _jitter(self) -> None:
        sleep_random(self._rng, self._max_jitter)


def verify_producer_consumer(
    solution_factory: Callable[[int], ProducerConsumerSolution],
    *,
    capacity: int = 3,
    producer_threads: int = 4,
    consumer_threads: int = 4,
    items_per_producer: int = 8,
    runs: int = 50,
    timeout: float = 3.0,
    max_jitter: float = 0.001,
) -> None:
    """Verify a finite-buffer producer-consumer solution."""

    if producer_threads <= 0 or consumer_threads <= 0:
        raise ValueError("producer_threads and consumer_threads must be positive")
    total_items = producer_threads * items_per_producer
    if total_items % consumer_threads != 0:
        raise ValueError("total produced items must divide evenly among consumers")

    _run_consumers_first(
        solution_factory,
        capacity=capacity,
        items=consumer_threads,
        timeout=timeout,
        max_jitter=max_jitter,
    )
    _run_producers_first(
        solution_factory,
        capacity=capacity,
        items=max(capacity * 2, producer_threads),
        timeout=timeout,
        max_jitter=max_jitter,
    )

    for run in range(runs):
        solution = solution_factory(capacity)
        verifier = ProducerConsumerVerifier(
            capacity,
            seed=run,
            max_jitter=max_jitter,
        )
        expected_items = set(range(total_items))
        items_per_consumer = total_items // consumer_threads

        def producer(producer_id: int) -> None:
            rng = random.Random((run + 1) * 10_000 + producer_id)
            start = producer_id * items_per_producer
            for item in range(start, start + items_per_producer):
                sleep_random(rng, max_jitter)
                _produce_once(solution, verifier, item)

        def consumer(consumer_id: int) -> None:
            rng = random.Random((run + 1) * 20_000 + consumer_id)
            for _ in range(items_per_consumer):
                sleep_random(rng, max_jitter)
                _consume_once(solution, verifier)

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
        verifier.assert_complete(expected_items)


def _run_consumers_first(
    solution_factory: Callable[[int], ProducerConsumerSolution],
    *,
    capacity: int,
    items: int,
    timeout: float,
    max_jitter: float,
) -> None:
    solution = solution_factory(capacity)
    verifier = ProducerConsumerVerifier(capacity, seed=90_000, max_jitter=max_jitter)
    producer_gate = threading.Event()
    expected_items = set(range(items))

    def producer(item: int) -> None:
        if not producer_gate.wait(timeout):
            raise VerificationError(f"producer {item} was never released")
        _produce_once(solution, verifier, item)

    def consumer() -> None:
        _consume_once(solution, verifier)

    def after_start() -> None:
        try:
            time.sleep(max(0.01, max_jitter * 10))
        finally:
            producer_gate.set()

    run_threads(
        {
            **{f"P{item}": lambda item=item: producer(item) for item in range(items)},
            **{f"C{item}": consumer for item in range(items)},
        },
        timeout=timeout,
        after_start=after_start,
    )
    verifier.assert_complete(expected_items)


def _run_producers_first(
    solution_factory: Callable[[int], ProducerConsumerSolution],
    *,
    capacity: int,
    items: int,
    timeout: float,
    max_jitter: float,
) -> None:
    solution = solution_factory(capacity)
    verifier = ProducerConsumerVerifier(capacity, seed=91_000, max_jitter=max_jitter)
    consumer_gate = threading.Event()
    expected_items = set(range(items))

    def producer(item: int) -> None:
        _produce_once(solution, verifier, item)

    def consumer() -> None:
        if not consumer_gate.wait(timeout):
            raise VerificationError("consumer was never released")
        _consume_once(solution, verifier)

    def after_start() -> None:
        try:
            if not verifier.wait_for_buffer_size(capacity, timeout / 2):
                raise VerificationError(
                    f"buffer did not fill to capacity {capacity}; "
                    f"log=[{verifier.log.compact()}]"
                )
            time.sleep(max(0.01, max_jitter * 10))
        finally:
            consumer_gate.set()

    run_threads(
        {
            **{f"P{item}": lambda item=item: producer(item) for item in range(items)},
            **{f"C{item}": consumer for item in range(items)},
        },
        timeout=timeout,
        after_start=after_start,
    )
    verifier.assert_complete(expected_items)


def _produce_once(
    solution: ProducerConsumerSolution,
    verifier: ProducerConsumerVerifier,
    item: int,
) -> None:
    called = False

    def put_once(value: int) -> None:
        nonlocal called
        if called:
            raise VerificationError(f"put callback called more than once for {item}")
        if value != item:
            raise VerificationError(
                f"produce({item}) called put({value}); expected put({item})"
            )
        called = True
        verifier.put(value)

    solution.produce(item, put_once)
    if not called:
        raise VerificationError(f"produce({item}) returned without calling put")


def _consume_once(
    solution: ProducerConsumerSolution,
    verifier: ProducerConsumerVerifier,
) -> int:
    called = False
    got_item: int | None = None

    def get_once() -> int:
        nonlocal called, got_item
        if called:
            raise VerificationError("get callback called more than once")
        called = True
        got_item = verifier.get()
        return got_item

    returned_item = solution.consume(get_once)
    if not called:
        raise VerificationError("consume() returned without calling get")
    if returned_item != got_item:
        raise VerificationError(
            f"consume() returned {returned_item}; expected {got_item}"
        )
    return returned_item
