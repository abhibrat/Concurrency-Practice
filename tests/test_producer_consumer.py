import os

from concurrency_practice.solutions.producer_consumer import ProducerConsumer
from concurrency_practice.verifiers import verify_producer_consumer


def test_producer_consumer() -> None:
    runs = int(os.getenv("PRODUCER_CONSUMER_STRESS_RUNS", "50"))
    capacity = int(os.getenv("PRODUCER_CONSUMER_CAPACITY", "3"))
    verify_producer_consumer(
        ProducerConsumer,
        capacity=capacity,
        runs=runs,
    )
