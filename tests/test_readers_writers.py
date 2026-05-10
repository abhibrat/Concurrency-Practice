import os

from concurrency_practice.solutions.readers_writers import ReadersWriters
from concurrency_practice.verifiers import verify_readers_writers


def test_readers_writers() -> None:
    runs = int(os.getenv("READERS_WRITERS_STRESS_RUNS", "50"))
    reader_threads = int(os.getenv("READERS_WRITERS_READER_THREADS", "6"))
    writer_threads = int(os.getenv("READERS_WRITERS_WRITER_THREADS", "3"))
    verify_readers_writers(
        ReadersWriters,
        reader_threads=reader_threads,
        writer_threads=writer_threads,
        runs=runs,
    )
