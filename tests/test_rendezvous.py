import os

from concurrency_practice.solutions.rendezvous import Rendezvous
from concurrency_practice.verifiers import verify_rendezvous


def test_rendezvous() -> None:
    runs = int(os.getenv("CONCURRENCY_STRESS_RUNS", "100"))
    concurrent_pairs = int(os.getenv("RENDEZVOUS_CONCURRENT_PAIRS", "16"))
    verify_rendezvous(
        Rendezvous,
        runs=runs,
        concurrent_pairs=concurrent_pairs,
    )
