from collections.abc import Callable
import threading


class BoundedBlockingQueueReference:
    """Known-good bounded FIFO blocking queue implementation."""

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self._queue: list[int] = []
        self._mutex = threading.Semaphore(1)
        self._items = threading.Semaphore(0)
        self._spaces = threading.Semaphore(capacity)

    def enqueue(self, item: int) -> None:
        self._spaces.acquire()
        self._mutex.acquire()
        try:
            self._queue.append(item)
        finally:
            self._mutex.release()
        self._items.release()

    def dequeue(self) -> int:
        self._items.acquire()
        self._mutex.acquire()
        try:
            item = self._queue.pop(0)
        finally:
            self._mutex.release()
        self._spaces.release()
        return item

    def size(self) -> int:
        self._mutex.acquire()
        try:
            return len(self._queue)
        finally:
            self._mutex.release()


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


class ProducerConsumerReference:
    """Known-good finite-buffer producer-consumer implementation."""

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self._mutex = threading.Semaphore(1)
        self._items = threading.Semaphore(0)
        self._spaces = threading.Semaphore(capacity)

    def produce(self, item: int, put: Callable[[int], None]) -> None:
        self._spaces.acquire()
        self._mutex.acquire()
        try:
            put(item)
        finally:
            self._mutex.release()
        self._items.release()

    def consume(self, get: Callable[[], int]) -> int:
        self._items.acquire()
        self._mutex.acquire()
        try:
            item = get()
        finally:
            self._mutex.release()
        self._spaces.release()
        return item


class ReadersWritersReference:
    """Known-good basic readers-writers implementation."""

    def __init__(self) -> None:
        self._readers = 0
        self._mutex = threading.Semaphore(1)
        self._room_empty = threading.Semaphore(1)

    def reader(self, read: Callable[[], None]) -> None:
        self._mutex.acquire()
        self._readers += 1
        if self._readers == 1:
            self._room_empty.acquire()
        self._mutex.release()

        try:
            read()
        finally:
            self._mutex.acquire()
            self._readers -= 1
            if self._readers == 0:
                self._room_empty.release()
            self._mutex.release()

    def writer(self, write: Callable[[], None]) -> None:
        self._room_empty.acquire()
        try:
            write()
        finally:
            self._room_empty.release()


class _LightSwitch:
    def __init__(self) -> None:
        self._counter = 0
        self._mutex = threading.Semaphore(1)

    def lock(self, semaphore: threading.Semaphore) -> None:
        self._mutex.acquire()
        self._counter += 1
        if self._counter == 1:
            semaphore.acquire()
        self._mutex.release()

    def unlock(self, semaphore: threading.Semaphore) -> None:
        self._mutex.acquire()
        self._counter -= 1
        if self._counter == 0:
            semaphore.release()
        self._mutex.release()


class NoStarveReadersWritersReference:
    """Known-good no-starve readers-writers implementation."""

    def __init__(self) -> None:
        self._read_switch = _LightSwitch()
        self._room_empty = threading.Semaphore(1)
        self._turnstile = threading.Semaphore(1)

    def reader(self, read: Callable[[], None]) -> None:
        self._turnstile.acquire()
        self._turnstile.release()

        self._read_switch.lock(self._room_empty)
        try:
            read()
        finally:
            self._read_switch.unlock(self._room_empty)

    def writer(self, write: Callable[[], None]) -> None:
        self._turnstile.acquire()
        self._room_empty.acquire()
        try:
            write()
        finally:
            self._turnstile.release()
            self._room_empty.release()


class WriterPriorityReadersWritersReference:
    """Known-good writer-priority readers-writers implementation."""

    def __init__(self) -> None:
        self._read_switch = _LightSwitch()
        self._write_switch = _LightSwitch()
        self._no_readers = threading.Semaphore(1)
        self._no_writers = threading.Semaphore(1)

    def reader(self, read: Callable[[], None]) -> None:
        self._no_readers.acquire()
        self._read_switch.lock(self._no_writers)
        self._no_readers.release()

        try:
            read()
        finally:
            self._read_switch.unlock(self._no_writers)

    def writer(self, write: Callable[[], None]) -> None:
        self._write_switch.lock(self._no_readers)
        self._no_writers.acquire()
        try:
            write()
        finally:
            self._no_writers.release()
            self._write_switch.unlock(self._no_readers)
