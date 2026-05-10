import os

from concurrency_practice.solutions.writer_priority_readers_writers import (
    WriterPriorityReadersWriters,
)
from concurrency_practice.verifiers import verify_writer_priority_readers_writers


def test_writer_priority_readers_writers() -> None:
    runs = int(os.getenv("WRITER_PRIORITY_READERS_WRITERS_STRESS_RUNS", "25"))
    queued_writers = int(os.getenv("WRITER_PRIORITY_QUEUED_WRITERS", "3"))
    verify_writer_priority_readers_writers(
        WriterPriorityReadersWriters,
        runs=runs,
        queued_writers=queued_writers,
    )
