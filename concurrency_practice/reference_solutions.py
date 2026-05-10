from collections.abc import Callable
import threading


class RendezvousReference:
    """Known-good rendezvous implementation using two semaphores."""

    def __init__(self) -> None:
        self._a_done = threading.Semaphore(0)
        self._b_done = threading.Semaphore(0)

    def a(self, a1: Callable[[], None], a2: Callable[[], None]) -> None:
        a1()
        self._a_done.release()
        self._b_done.acquire()
        a2()

    def b(self, b1: Callable[[], None], b2: Callable[[], None]) -> None:
        b1()
        self._b_done.release()
        self._a_done.acquire()
        b2()


class ReusableBarrierReference:
    """Known-good two-turnstile reusable barrier."""

    def __init__(self, parties: int) -> None:
        if parties <= 0:
            raise ValueError("parties must be positive")
        self.parties = parties
        self._count = 0
        self._mutex = threading.Semaphore(1)
        self._turnstile = threading.Semaphore(0)
        self._turnstile2 = threading.Semaphore(1)

    def wait(self) -> None:
        self._phase1()
        self._phase2()

    def _phase1(self) -> None:
        self._mutex.acquire()
        self._count += 1
        if self._count == self.parties:
            self._turnstile2.acquire()
            self._turnstile.release()
        self._mutex.release()

        self._turnstile.acquire()
        self._turnstile.release()

    def _phase2(self) -> None:
        self._mutex.acquire()
        self._count -= 1
        if self._count == 0:
            self._turnstile.acquire()
            self._turnstile2.release()
        self._mutex.release()

        self._turnstile2.acquire()
        self._turnstile2.release()
