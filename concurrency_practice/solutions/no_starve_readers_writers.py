"""No-starve readers-writers exercise.

Based on The Little Book of Semaphores, readers-writers sections 4.2.4 and
4.2.5.

Start from the basic readers-writers rules:
- Multiple readers may read at the same time.
- A writer must be alone.
- No reader may enter while a writer is active.
- No writer may enter while any reader is active.

Additional no-starve rule:
- If a writer arrives while readers are active, later readers must queue behind
  that writer. When the current readers leave, at least one waiting writer must
  enter before those later readers.

The Little Book hint is to add a turnstile. Writers hold the turnstile while
waiting for the room to become empty, which prevents later readers from
continually extending the reader group.

Run:
    python -m pytest tests/test_no_starve_readers_writers.py -q

Stress:
    NO_STARVE_READERS_WRITERS_STRESS_RUNS=100 \
        python -m pytest tests/test_no_starve_readers_writers.py -q
"""

from collections.abc import Callable
import threading

class NoStarveReadersWriters:
    """Starter solution for the no-starve readers-writers exercise."""

    def __init__(self) -> None:
        self.lock=threading.Lock()
        self.lock2=threading.Lock()
        self.room_empty=threading.Lock()
        self.readers=0
        pass

    def reader(self, read: Callable[[], None]) -> None:
        with self.lock:
            pass

        with self.lock2:
            self.readers+=1
            if self.readers==1:
                self.room_empty.acquire()

        try:
            read()
        finally:
            with self.lock2:
                self.readers-=1
                if self.readers==0:
                    self.room_empty.release()


    def writer(self, write: Callable[[], None]) -> None:
        with self.lock:
            self.room_empty.acquire()
            try:
                write()
            finally:
                self.room_empty.release()
