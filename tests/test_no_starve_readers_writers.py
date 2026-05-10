import os

from concurrency_practice.solutions.no_starve_readers_writers import (
    NoStarveReadersWriters,
)
from concurrency_practice.verifiers import verify_no_starve_readers_writers


def test_no_starve_readers_writers() -> None:
    runs = int(os.getenv("NO_STARVE_READERS_WRITERS_STRESS_RUNS", "25"))
    verify_no_starve_readers_writers(
        NoStarveReadersWriters,
        runs=runs,
    )
