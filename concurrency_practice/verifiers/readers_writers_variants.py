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
)
from concurrency_practice.verifiers.readers_writers import verify_readers_writers


class ReadersWritersVariantSolution(Protocol):
    def reader(self, read: Callable[[], None]) -> None:
        ...

    def writer(self, write: Callable[[], None]) -> None:
        ...


class ReadersWritersVariantProbe:
    def __init__(
        self,
        *,
        seed: int = 0,
        max_jitter: float = 0.001,
        callback_timeout: float = 2.0,
    ) -> None:
        self.log = EventLog()
        self._active_readers = 0
        self._active_writers = 0
        self._read_count = 0
        self._write_count = 0
        self._state = threading.Condition()
        self._entered: dict[str, threading.Event] = {}
        self._rng = random.Random(seed)
        self._rng_lock = threading.Lock()
        self._max_jitter = max_jitter
        self._callback_timeout = callback_timeout

    def read(
        self,
        label: str,
        *,
        hold_gate: threading.Event | None = None,
    ) -> Callable[[], None]:
        return lambda: self._read(label, hold_gate)

    def write(
        self,
        label: str,
        *,
        hold_gate: threading.Event | None = None,
    ) -> Callable[[], None]:
        return lambda: self._write(label, hold_gate)

    def wait_for_enter(self, event_name: str, timeout: float) -> bool:
        with self._state:
            event = self._entered.setdefault(event_name, threading.Event())
        return event.wait(timeout)

    def entry_index(self, event_name: str) -> int:
        for event in self.log.snapshot():
            if event.name == event_name:
                return event.index
        raise VerificationError(
            f"{event_name} never entered; log=[{self.log.compact()}]"
        )

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

    def _read(self, label: str, hold_gate: threading.Event | None) -> None:
        self._jitter()
        with self._state:
            if self._active_writers:
                raise VerificationError(
                    f"reader {label} entered while a writer was active; "
                    f"log=[{self.log.compact()}]"
                )
            self._active_readers += 1
            self._read_count += 1
            self._record_enter_locked(f"read_enter#{label}")

        try:
            self._wait_or_jitter(hold_gate, f"reader {label}")
        finally:
            with self._state:
                self._active_readers -= 1
                self.log.record(f"read_exit#{label}")
                self._state.notify_all()
        self._jitter()

    def _write(self, label: str, hold_gate: threading.Event | None) -> None:
        self._jitter()
        with self._state:
            if self._active_readers or self._active_writers:
                raise VerificationError(
                    f"writer {label} entered while the room was not empty; "
                    f"active_readers={self._active_readers}, "
                    f"active_writers={self._active_writers}, "
                    f"log=[{self.log.compact()}]"
                )
            self._active_writers += 1
            self._write_count += 1
            self._record_enter_locked(f"write_enter#{label}")

        try:
            self._wait_or_jitter(hold_gate, f"writer {label}")
        finally:
            with self._state:
                self._active_writers -= 1
                self.log.record(f"write_exit#{label}")
                self._state.notify_all()
        self._jitter()

    def _record_enter_locked(self, event_name: str) -> None:
        self.log.record(event_name)
        event = self._entered.setdefault(event_name, threading.Event())
        event.set()
        self._state.notify_all()

    def _wait_or_jitter(
        self,
        hold_gate: threading.Event | None,
        label: str,
    ) -> None:
        if hold_gate is None:
            self._jitter()
            return
        if not hold_gate.wait(self._callback_timeout):
            raise VerificationError(f"{label} was not released by the verifier")

    def _jitter(self) -> None:
        if self._max_jitter <= 0:
            return
        with self._rng_lock:
            delay = self._rng.uniform(0, self._max_jitter)
        time.sleep(delay)


def verify_no_starve_readers_writers(
    solution_factory: Callable[[], ReadersWritersVariantSolution],
    *,
    runs: int = 25,
    timeout: float = 3.0,
    max_jitter: float = 0.001,
) -> None:
    """Verify basic safety plus the Little Book no-starve turnstile behavior."""

    verify_readers_writers(
        solution_factory,
        reader_threads=6,
        writer_threads=3,
        operations_per_thread=4,
        runs=max(1, min(runs, 10)),
        timeout=timeout,
        max_jitter=max_jitter,
    )

    for run in range(runs):
        _assert_waiting_writer_beats_late_reader(
            solution_factory,
            seed=100_000 + run,
            timeout=timeout,
            max_jitter=max_jitter,
        )


def verify_writer_priority_readers_writers(
    solution_factory: Callable[[], ReadersWritersVariantSolution],
    *,
    runs: int = 25,
    queued_writers: int = 3,
    timeout: float = 3.0,
    max_jitter: float = 0.001,
) -> None:
    """Verify basic safety plus writer-priority queue-draining behavior."""

    if queued_writers <= 0:
        raise ValueError("queued_writers must be positive")

    verify_readers_writers(
        solution_factory,
        reader_threads=6,
        writer_threads=3,
        operations_per_thread=4,
        runs=max(1, min(runs, 10)),
        timeout=timeout,
        max_jitter=max_jitter,
    )

    for run in range(runs):
        _assert_queued_writers_beat_late_reader(
            solution_factory,
            queued_writers=queued_writers,
            seed=200_000 + run,
            timeout=timeout,
            max_jitter=max_jitter,
        )


def _assert_waiting_writer_beats_late_reader(
    solution_factory: Callable[[], ReadersWritersVariantSolution],
    *,
    seed: int,
    timeout: float,
    max_jitter: float,
) -> None:
    solution = solution_factory()
    probe = ReadersWritersVariantProbe(seed=seed, max_jitter=max_jitter)
    early_reader_hold = threading.Event()
    writer_start = threading.Event()
    writer_started = threading.Event()
    late_reader_start = threading.Event()

    def early_reader() -> None:
        _reader_once(
            solution,
            probe.read("early", hold_gate=early_reader_hold),
            "early",
        )

    def writer() -> None:
        _wait_for_gate(writer_start, timeout, "writer")
        writer_started.set()
        _writer_once(solution, probe.write("waiting-writer"), "waiting-writer")

    def late_reader() -> None:
        _wait_for_gate(late_reader_start, timeout, "late reader")
        _reader_once(solution, probe.read("late-reader"), "late-reader")

    def after_start() -> None:
        try:
            _require_enter(probe, "read_enter#early", timeout / 2)
            writer_start.set()
            _wait_for_gate(writer_started, timeout / 2, "writer start")
            time.sleep(max(0.02, max_jitter * 20))
            late_reader_start.set()
            time.sleep(max(0.02, max_jitter * 20))
        finally:
            early_reader_hold.set()

    run_threads(
        {
            "early-reader": early_reader,
            "waiting-writer": writer,
            "late-reader": late_reader,
        },
        timeout=timeout,
        after_start=after_start,
    )
    probe.assert_counts(reads=2, writes=1)

    writer_index = probe.entry_index("write_enter#waiting-writer")
    late_reader_index = probe.entry_index("read_enter#late-reader")
    if late_reader_index < writer_index:
        raise VerificationError(
            "late reader entered before a writer that was already waiting; "
            f"log=[{probe.log.compact()}]"
        )


def _assert_queued_writers_beat_late_reader(
    solution_factory: Callable[[], ReadersWritersVariantSolution],
    *,
    queued_writers: int,
    seed: int,
    timeout: float,
    max_jitter: float,
) -> None:
    solution = solution_factory()
    probe = ReadersWritersVariantProbe(seed=seed, max_jitter=max_jitter)
    early_reader_hold = threading.Event()
    writers_start = threading.Event()
    writers_started = [
        threading.Event()
        for _ in range(queued_writers)
    ]
    late_reader_start = threading.Event()

    def early_reader() -> None:
        _reader_once(
            solution,
            probe.read("early", hold_gate=early_reader_hold),
            "early",
        )

    def writer(writer_id: int) -> None:
        _wait_for_gate(writers_start, timeout, f"writer {writer_id}")
        writers_started[writer_id].set()
        _writer_once(
            solution,
            probe.write(f"queued-writer-{writer_id}"),
            f"queued-writer-{writer_id}",
        )

    def late_reader() -> None:
        _wait_for_gate(late_reader_start, timeout, "late reader")
        _reader_once(solution, probe.read("late-reader"), "late-reader")

    def after_start() -> None:
        try:
            _require_enter(probe, "read_enter#early", timeout / 2)
            writers_start.set()
            for writer_id, started in enumerate(writers_started):
                _wait_for_gate(started, timeout / 2, f"writer {writer_id} start")
            time.sleep(max(0.03, max_jitter * 30))
            late_reader_start.set()
            time.sleep(max(0.02, max_jitter * 20))
        finally:
            early_reader_hold.set()

    run_threads(
        {
            "early-reader": early_reader,
            **{
                f"queued-writer-{writer_id}": (
                    lambda writer_id=writer_id: writer(writer_id)
                )
                for writer_id in range(queued_writers)
            },
            "late-reader": late_reader,
        },
        timeout=timeout,
        after_start=after_start,
    )
    probe.assert_counts(reads=2, writes=queued_writers)

    late_reader_index = probe.entry_index("read_enter#late-reader")
    late_writers = []
    for writer_id in range(queued_writers):
        event_name = f"write_enter#queued-writer-{writer_id}"
        if probe.entry_index(event_name) > late_reader_index:
            late_writers.append(event_name)

    if late_writers:
        raise VerificationError(
            "late reader entered before queued writer(s), violating "
            f"writer-priority; late_writers={late_writers}, "
            f"log=[{probe.log.compact()}]"
        )


def _reader_once(
    solution: ReadersWritersVariantSolution,
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
    solution: ReadersWritersVariantSolution,
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


def _require_enter(
    probe: ReadersWritersVariantProbe,
    event_name: str,
    timeout: float,
) -> None:
    if not probe.wait_for_enter(event_name, timeout):
        raise VerificationError(
            f"{event_name} did not enter before timeout; log=[{probe.log.compact()}]"
        )


def _wait_for_gate(gate: threading.Event, timeout: float, name: str) -> None:
    if not gate.wait(timeout):
        raise VerificationError(f"{name} was never released by the verifier")
