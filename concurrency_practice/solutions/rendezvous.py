"""Rendezvous exercise.

Implement `Rendezvous.a()` and `Rendezvous.b()`.

Single-pair invariants:
- A1 happens before A2.
- B1 happens before B2.
- Both A1 and B1 happen before either A2 or B2.

Shared-instance stress invariant:
- The tests also run many A and B threads against the same object.
- Every A2 must have a prior unmatched B1.
- Every B2 must have a prior unmatched A1.
- This is not a barrier; one pair may complete before later threads arrive.

Run:
    python -m pytest tests/test_rendezvous.py -q

Stress:
    RENDEZVOUS_CONCURRENT_PAIRS=64 CONCURRENCY_STRESS_RUNS=500 \
        python -m pytest tests/test_rendezvous.py -q
"""

from collections.abc import Callable

import threading 

class Rendezvous:
    """Starter solution for the rendezvous exercise.

    Replace the TODO sections with synchronization. The verifier supplies
    callbacks that record the visible events, so call each callback exactly once.
    """

    def __init__(self) -> None:
        # Put semaphores, events, locks, or other shared state here.
        self.a1Done=threading.Semaphore(0)
        self.b1Done=threading.Semaphore(0)


    def a(self, a1: Callable[[], None], a2: Callable[[], None]) -> None:
        a1()
        self.a1Done.release()
        self.b1Done.acquire()
        a2()

    def b(self, b1: Callable[[], None], b2: Callable[[], None]) -> None:
        b1()
        self.b1Done.release()
        self.a1Done.acquire()
        b2()
