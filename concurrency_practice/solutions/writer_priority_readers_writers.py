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
from threading import Lock

class WriterPriorityReadersWriters:
    """Starter solution for the writer-priority readers-writers exercise."""

    def __init__(self) -> None:
        self.lock1=Lock()
        self.lock2=Lock()
        self.room=Lock()
        self.readers=0
        self.writers_queued=0
        pass

    def reader(self, read: Callable[[], None]) -> None:
        self.lock2.acquire()
        self.lock2.release()
        
        with self.lock1:
            self.readers+=1
            if self.readers==1:
                self.room.acquire()

        try:
            read()
        finally:
            with self.lock1:
                self.readers-=1
                if self.readers==0:
                    self.room.release()


    def writer(self, write: Callable[[], None]) -> None:
        with self.lock1:
            self.writers_queued+=1
            if self.writers_queued==1:
                self.lock2.acquire()
        self.room.acquire()
        try:        
            write()
        finally:
            self.room.release()
            self.writers_queued-=1
            if self.writers_queued==0:
                self.lock2.release()    
