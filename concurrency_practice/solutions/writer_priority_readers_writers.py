"""Writer-priority readers-writers exercise.

Based on The Little Book of Semaphores, readers-writers sections 4.2.6 and
4.2.7.

Start from the basic readers-writers rules:
- Multiple readers may read at the same time.
- A writer must be alone.
- No reader may enter while a writer is active.
- No writer may enter while any reader is active.

Additional writer-priority rule:
- Once a writer is queued, later readers must wait.
- If several writers are queued, readers should not enter until the queued
  writer group has drained.

The Little Book solution uses separate reader and writer lightswitches plus
`noReaders` and `noWriters` semaphores. The tradeoff is that readers can starve
or face long delays while writers keep arriving.

Run:
    python -m pytest tests/test_writer_priority_readers_writers.py -q

Stress:
    WRITER_PRIORITY_READERS_WRITERS_STRESS_RUNS=100 \
        python -m pytest tests/test_writer_priority_readers_writers.py -q
"""

from collections.abc import Callable


class WriterPriorityReadersWriters:
    """Starter solution for the writer-priority readers-writers exercise."""

    def __init__(self) -> None:
        # Put semaphores, locks, counters, lightswitches, or gates here.
        pass

    def reader(self, read: Callable[[], None]) -> None:
        # TODO: enter as a writer-priority reader, call read once, then leave.
        read()

    def writer(self, write: Callable[[], None]) -> None:
        # TODO: enter as a writer-priority writer, call write once, then leave.
        write()
