"""Readers-writers exercise.

Implement `ReadersWriters.reader()` and `ReadersWriters.writer()`.

The verifier owns the critical sections and gives you callbacks:
- `read()` represents the reader critical section.
- `write()` represents the writer critical section.

Verifier invariants:
- Multiple readers are allowed to read at the same time.
- A writer must be alone.
- No reader may enter while a writer is active.
- No writer may enter while any reader is active.

This is the basic readers-writers problem from The Little Book of Semaphores.
Writer-priority and no-starve variants are separate extensions and are not
required by this verifier.

Run:
    python -m pytest tests/test_readers_writers.py -q

Stress:
    READERS_WRITERS_STRESS_RUNS=200 python -m pytest tests/test_readers_writers.py -q
"""

from collections.abc import Callable
from threading import Semaphore, Lock, Condition


class ReadersWriters:
    """Starter solution for the basic readers-writers exercise."""

    def __init__(self) -> None:
        # Put semaphores, locks, counters, or lightswitch state here.
        self.reader_active=0
        self.writer_active=0
        self.cond = Condition()


    def reader(self, read: Callable[[], None]) -> None:
        
        with self.cond:
            while self.writer_active!=0:
                self.cond.wait()
            self.reader_active+=1    
        try:
            read()
        finally:    
            with self.cond:
                self.reader_active-=1
                if self.reader_active==0:
                    self.cond.notify_all()


    def writer(self, write: Callable[[], None]) -> None:

        with self.cond:
            while self.reader_active!=0 or self.writer_active!=0:
                self.cond.wait()
            self.writer_active=1
       
        try:
            write()
        finally:    
            with self.cond:
                self.writer_active=0
                self.cond.notify_all()
