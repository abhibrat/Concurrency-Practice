import os

from concurrency_practice.solutions.reusable_barrier import ReusableBarrier
from concurrency_practice.verifiers import verify_reusable_barrier


def test_reusable_barrier() -> None:
    runs = int(os.getenv("BARRIER_STRESS_RUNS", "20"))
    generations = int(os.getenv("BARRIER_GENERATIONS", "25"))
    parties = int(os.getenv("BARRIER_PARTIES", "5"))
    verify_reusable_barrier(
        ReusableBarrier,
        parties=parties,
        generations=generations,
        runs=runs,
    )
