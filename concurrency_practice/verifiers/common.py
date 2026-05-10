from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import random
import threading
import time
import traceback
from typing import Any


class VerificationError(AssertionError):
    """Raised when a concurrent execution violates a problem invariant."""


@dataclass(frozen=True)
class Event:
    index: int
    name: str
    thread_name: str
    generation: int | None = None


class EventLog:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._events: list[Event] = []

    def record(self, name: str, *, generation: int | None = None) -> Event:
        with self._lock:
            event = Event(
                index=len(self._events),
                name=name,
                thread_name=threading.current_thread().name,
                generation=generation,
            )
            self._events.append(event)
            return event

    def snapshot(self) -> list[Event]:
        with self._lock:
            return list(self._events)

    def names(self) -> list[str]:
        return [event.name for event in self.snapshot()]

    def compact(self) -> str:
        pieces = []
        for event in self.snapshot():
            if event.generation is None:
                pieces.append(f"{event.index}:{event.name}@{event.thread_name}")
            else:
                pieces.append(
                    f"{event.index}:{event.name}[g={event.generation}]"
                    f"@{event.thread_name}"
                )
        return ", ".join(pieces)


def sleep_random(rng: random.Random, max_seconds: float) -> None:
    if max_seconds <= 0:
        return
    time.sleep(rng.uniform(0, max_seconds))


@dataclass(frozen=True)
class _ThreadFailure:
    name: str
    error: BaseException
    formatted_traceback: str


def run_threads(
    tasks: Mapping[str, Callable[[], Any]],
    *,
    timeout: float,
    after_start: Callable[[], Any] | None = None,
) -> None:
    """Run named callables in threads and surface failures in the test thread."""

    if not tasks:
        raise ValueError("at least one task is required")

    failures: list[_ThreadFailure] = []
    failure_lock = threading.Lock()
    start = threading.Barrier(len(tasks) + 1)

    def runner(name: str, task: Callable[[], Any]) -> None:
        try:
            start.wait(timeout=timeout)
            task()
        except BaseException as exc:
            with failure_lock:
                failures.append(
                    _ThreadFailure(
                        name=name,
                        error=exc,
                        formatted_traceback=traceback.format_exc(),
                    )
                )

    threads = [
        threading.Thread(target=runner, name=name, args=(name, task), daemon=True)
        for name, task in tasks.items()
    ]

    for thread in threads:
        thread.start()

    after_start_failure: _ThreadFailure | None = None
    try:
        start.wait(timeout=timeout)
        if after_start is not None:
            after_start()
    except BaseException as exc:
        after_start_failure = _ThreadFailure(
            name="test-driver",
            error=exc,
            formatted_traceback=traceback.format_exc(),
        )

    deadline = time.monotonic() + timeout
    for thread in threads:
        remaining = max(0.0, deadline - time.monotonic())
        thread.join(remaining)

    alive = [thread.name for thread in threads if thread.is_alive()]
    if after_start_failure is not None:
        failures.insert(0, after_start_failure)

    if alive:
        raise VerificationError(
            "timed out waiting for threads to finish: "
            + ", ".join(alive)
            + ". This usually means the solution deadlocked."
        )

    if failures:
        details = "\n\n".join(
            f"[{failure.name}] {failure.error!r}\n{failure.formatted_traceback}"
            for failure in failures
        )
        raise VerificationError(details)
