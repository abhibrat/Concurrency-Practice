from __future__ import annotations

from collections.abc import Callable
import random
import threading
import time
from typing import Protocol

from concurrency_practice.verifiers.common import (
    EventLog,
    VerificationError,
    run_threads,
    sleep_random,
)


class ReadersWritersSolution(Protocol):
    def reader(self, read: Callable[[], None]) -> None:
        ...

    def writer(self, write: Callable[[], None]) -> None:
        ...


class ReadersWritersVerifier:
    def __init__(self, *, seed: int = 0, max_jitter: float = 0.001) -> None:
        self.log = EventLog()
        self._active_readers = 0
        self._active_writers = 0
        self._max_active_readers = 0
        self._read_count = 0
        self._write_count = 0
        self._state = threading.Condition()
        self._rng = random.Random(seed)
        self._max_jitter = max_jitter

    @property
    def max_active_readers(self) -> int:
        with self._state:
            return self._max_active_readers

    def read(
        self,
        label: str,
        *,
        hold_gate: threading.Event | None = None,
        overlap_gate: threading.Event | None = None,
    ) -> Callable[[], None]:
        return lambda: self._read(label, hold_gate, overlap_gate)

    def write(self, label: str) -> Callable[[], None]:
        return lambda: self._write(label)

    def wait_for_reader_overlap(self, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        with self._state:
            while self._max_active_readers < 2:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._state.wait(remaining)
            return True

    def assert_counts(self, *, reads: int, writes: int) -> None:
        with self._state:
            if self._read_count != reads or self._write_count != writes:
                raise VerificationError(
                    f"critical-section counts mismatch; expected "
                    f"reads={reads}, writes={writes}; actual "
                    f"reads={self._read_count}, writes={self._write_count}; "
                    f"log=[{self.log.compact()}]"
                )
            if self._active_readers or self._active_writers:
                raise VerificationError(
                    f"critical section still active; readers={self._active_readers}, "
                    f"writers={self._active_writers}, log=[{self.log.compact()}]"
                )

    def _read(
        self,
        label: str,
        hold_gate: threading.Event | None,
        overlap_gate: threading.Event | None,
    ) -> None:
        self._jitter()
        with self._state:
            if self._active_writers:
                raise VerificationError(
                    f"reader {label} entered while a writer was active; "
                    f"log=[{self.log.compact()}]"
                )
            self._active_readers += 1
            self._read_count += 1
            self._max_active_readers = max(
                self._max_active_readers,
                self._active_readers,
            )
            self.log.record(f"read_enter#{label}")
            if self._active_readers >= 2 and overlap_gate is not None:
                overlap_gate.set()
            self._state.notify_all()

        try:
            if hold_gate is not None:
                if not hold_gate.wait(2.0):
                    raise VerificationError(
                        f"reader {label} was not released from the read callback"
                    )
            else:
                self._jitter()
        finally:
            with self._state:
                self._active_readers -= 1
                self.log.record(f"read_exit#{label}")
                self._state.notify_all()
        self._jitter()

    def _write(self, label: str) -> None:
        self._jitter()
        with self._state:
            if self._active_writers or self._active_readers:
                raise VerificationError(
                    f"writer {label} entered while the room was not empty; "
                    f"active_readers={self._active_readers}, "
                    f"active_writers={self._active_writers}, "
                    f"log=[{self.log.compact()}]"
                )
            self._active_writers += 1
            self._write_count += 1
            self.log.record(f"write_enter#{label}")
            self._state.notify_all()

        try:
            self._jitter()
        finally:
            with self._state:
                self._active_writers -= 1
                self.log.record(f"write_exit#{label}")
                self._state.notify_all()
        self._jitter()

    def _jitter(self) -> None:
        sleep_random(self._rng, self._max_jitter)


def verify_readers_writers(
    solution_factory: Callable[[], ReadersWritersSolution],
    *,
    reader_threads: int = 6,
    writer_threads: int = 3,
    operations_per_thread: int = 8,
    runs: int = 50,
    timeout: float = 3.0,
    max_jitter: float = 0.001,
) -> None:
    """Verify the basic readers-writers safety and reader-sharing contract."""

    _assert_readers_can_overlap(
        solution_factory,
        reader_threads=max(2, reader_threads),
        timeout=timeout,
        max_jitter=max_jitter,
    )

    for run in range(runs):
        solution = solution_factory()
        verifier = ReadersWritersVerifier(seed=run, max_jitter=max_jitter)

        def reader_thread(thread_id: int) -> None:
            rng = random.Random((run + 1) * 10_000 + thread_id)
            for op in range(operations_per_thread):
                sleep_random(rng, max_jitter)
                _reader_once(
                    solution,
                    verifier.read(f"R{thread_id}.{op}"),
                    f"R{thread_id}.{op}",
                )

        def writer_thread(thread_id: int) -> None:
            rng = random.Random((run + 1) * 20_000 + thread_id)
            for op in range(operations_per_thread):
                sleep_random(rng, max_jitter)
                _writer_once(
                    solution,
                    verifier.write(f"W{thread_id}.{op}"),
                    f"W{thread_id}.{op}",
                )

        run_threads(
            {
                **{
                    f"R{thread_id}": (
                        lambda thread_id=thread_id: reader_thread(thread_id)
                    )
                    for thread_id in range(reader_threads)
                },
                **{
                    f"W{thread_id}": (
                        lambda thread_id=thread_id: writer_thread(thread_id)
                    )
                    for thread_id in range(writer_threads)
                },
            },
            timeout=timeout,
        )
        verifier.assert_counts(
            reads=reader_threads * operations_per_thread,
            writes=writer_threads * operations_per_thread,
        )


def _assert_readers_can_overlap(
    solution_factory: Callable[[], ReadersWritersSolution],
    *,
    reader_threads: int,
    timeout: float,
    max_jitter: float,
) -> None:
    solution = solution_factory()
    verifier = ReadersWritersVerifier(seed=80_000, max_jitter=max_jitter)
    hold_gate = threading.Event()
    overlap_gate = threading.Event()

    def reader(thread_id: int) -> None:
        _reader_once(
            solution,
            verifier.read(
                f"overlap-{thread_id}",
                hold_gate=hold_gate,
                overlap_gate=overlap_gate,
            ),
            f"overlap-{thread_id}",
        )

    def after_start() -> None:
        try:
            if not overlap_gate.wait(timeout / 2):
                raise VerificationError(
                    "readers did not overlap; the basic readers-writers problem "
                    "allows multiple readers in the critical section together"
                )
        finally:
            hold_gate.set()

    run_threads(
        {
            f"R{thread_id}": lambda thread_id=thread_id: reader(thread_id)
            for thread_id in range(reader_threads)
        },
        timeout=timeout,
        after_start=after_start,
    )


def _reader_once(
    solution: ReadersWritersSolution,
    read: Callable[[], None],
    label: str,
) -> None:
    called = False

    def read_once() -> None:
        nonlocal called
        if called:
            raise VerificationError(f"reader callback called more than once: {label}")
        called = True
        read()

    solution.reader(read_once)
    if not called:
        raise VerificationError(f"reader({label}) returned without calling read")


def _writer_once(
    solution: ReadersWritersSolution,
    write: Callable[[], None],
    label: str,
) -> None:
    called = False

    def write_once() -> None:
        nonlocal called
        if called:
            raise VerificationError(f"writer callback called more than once: {label}")
        called = True
        write()

    solution.writer(write_once)
    if not called:
        raise VerificationError(f"writer({label}) returned without calling write")
