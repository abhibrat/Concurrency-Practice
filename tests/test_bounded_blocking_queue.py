import os

from concurrency_practice.solutions.bounded_blocking_queue import (
    BoundedBlockingQueue,
)
from concurrency_practice.verifiers import verify_bounded_blocking_queue


def test_bounded_blocking_queue() -> None:
    runs = int(os.getenv("BOUNDED_BLOCKING_QUEUE_STRESS_RUNS", "50"))
    capacity = int(os.getenv("BOUNDED_BLOCKING_QUEUE_CAPACITY", "3"))
    producer_threads = int(os.getenv("BOUNDED_BLOCKING_QUEUE_PRODUCERS", "4"))
    consumer_threads = int(os.getenv("BOUNDED_BLOCKING_QUEUE_CONSUMERS", "4"))
    items_per_producer = int(
        os.getenv("BOUNDED_BLOCKING_QUEUE_ITEMS_PER_PRODUCER", "8")
    )
    verify_bounded_blocking_queue(
        BoundedBlockingQueue,
        capacity=capacity,
        producer_threads=producer_threads,
        consumer_threads=consumer_threads,
        items_per_producer=items_per_producer,
        runs=runs,
    )
