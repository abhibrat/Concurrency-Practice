from __future__ import annotations

from collections import Counter
from collections.abc import Callable
import random
import threading
import time
from typing import Protocol

from concurrency_practice.verifiers.common import EventLog, VerificationError, run_threads


class RendezvousSolution(Protocol):
    def a(self, a1: Callable[[], None], a2: Callable[[], None]) -> None:
        ...

    def b(self, b1: Callable[[], None], b2: Callable[[], None]) -> None:
        ...


class RendezvousVerifier:
    def __init__(self, *, seed: int = 0, max_jitter: float = 0.001) -> None:
        self.log = EventLog()
        self._max_jitter = max_jitter
        self._rng = random.Random(seed)
        self._rng_lock = threading.Lock()
        self._lock = threading.Lock()
        self._seen: set[str] = set()
        self._events = {name: threading.Event() for name in ("A1", "A2", "B1", "B2")}

    def a1(self) -> None:
        self._record("A1")

    def a2(self) -> None:
        self._record("A2")

    def b1(self) -> None:
        self._record("B1")

    def b2(self) -> None:
        self._record("B2")

    def wait_for(self, name: str, timeout: float) -> bool:
        return self._events[name].wait(timeout)

    def assert_complete(self) -> None:
        names = self.log.names()
        counts = Counter(names)
        missing = [name for name in ("A1", "A2", "B1", "B2") if counts[name] == 0]
        duplicates = [name for name, count in counts.items() if count > 1]

        if missing or duplicates:
            raise VerificationError(
                f"expected exactly one A1, A2, B1, and B2; "
                f"missing={missing}, duplicates={duplicates}, log=[{self.log.compact()}]"
            )

        positions = {name: names.index(name) for name in names}
        for second_event in ("A2", "B2"):
            for first_event in ("A1", "B1"):
                if positions[first_event] > positions[second_event]:
                    raise VerificationError(
                        f"{second_event} happened before {first_event}; "
                        f"log=[{self.log.compact()}]"
                    )

    def _record(self, name: str) -> None:
        self._jitter()
        with self._lock:
            if name in self._seen:
                raise VerificationError(
                    f"{name} was called more than once; log=[{self.log.compact()}]"
                )
            if name in {"A2", "B2"} and not {"A1", "B1"}.issubset(self._seen):
                raise VerificationError(
                    f"{name} ran before both A1 and B1 completed; "
                    f"seen={sorted(self._seen)}, log=[{self.log.compact()}]"
                )
            self._seen.add(name)
            self.log.record(name)
            self._events[name].set()
        self._jitter()

    def _jitter(self) -> None:
        if self._max_jitter <= 0:
            return
        with self._rng_lock:
            delay = self._rng.uniform(0, self._max_jitter)
        time.sleep(delay)


class MultiRendezvousVerifier:
    """Verifier for many A/B callers sharing one rendezvous object.

    This checks the reusable/counting interpretation of rendezvous:
    each A2 must consume one prior B1, and each B2 must consume one prior A1.
    It does not require every thread's first action to happen before any second
    action, because that would be a barrier.
    """

    def __init__(
        self,
        *,
        pairs: int,
        seed: int = 0,
        max_jitter: float = 0.001,
    ) -> None:
        if pairs <= 0:
            raise ValueError("pairs must be positive")
        self.pairs = pairs
        self.log = EventLog()
        self._max_jitter = max_jitter
        self._rng = random.Random(seed)
        self._rng_lock = threading.Lock()
        self._condition = threading.Condition()
        self._seen: set[tuple[str, int, int]] = set()
        self._counts = Counter()

    def a1(self, thread_id: int) -> Callable[[], None]:
        return lambda: self._record("A", 1, thread_id)

    def a2(self, thread_id: int) -> Callable[[], None]:
        return lambda: self._record("A", 2, thread_id)

    def b1(self, thread_id: int) -> Callable[[], None]:
        return lambda: self._record("B", 1, thread_id)

    def b2(self, thread_id: int) -> Callable[[], None]:
        return lambda: self._record("B", 2, thread_id)

    def wait_for_count(self, event_name: str, expected: int, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        with self._condition:
            while self._counts[event_name] < expected:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
            return True

    def assert_complete(self) -> None:
        with self._condition:
            expected_counts = {
                "A1": self.pairs,
                "A2": self.pairs,
                "B1": self.pairs,
                "B2": self.pairs,
            }
            actual_counts = {
                name: self._counts[name]
                for name in ("A1", "A2", "B1", "B2")
            }
            if actual_counts != expected_counts:
                raise VerificationError(
                    f"expected counts {expected_counts}, got {actual_counts}; "
                    f"log=[{self.log.compact()}]"
                )

            missing = []
            for role in ("A", "B"):
                for phase in (1, 2):
                    for thread_id in range(self.pairs):
                        if (role, phase, thread_id) not in self._seen:
                            missing.append(f"{role}{phase}#{thread_id}")

            if missing:
                raise VerificationError(
                    f"missing callbacks: {missing}; log=[{self.log.compact()}]"
                )

    def _record(self, role: str, phase: int, thread_id: int) -> None:
        self._jitter()
        event_name = f"{role}{phase}"
        event_key = (role, phase, thread_id)

        with self._condition:
            if event_key in self._seen:
                raise VerificationError(
                    f"{event_name} for thread {thread_id} was called more than once; "
                    f"log=[{self.log.compact()}]"
                )

            if phase == 2:
                first_key = (role, 1, thread_id)
                if first_key not in self._seen:
                    raise VerificationError(
                        f"{event_name} for thread {thread_id} ran before its "
                        f"{role}1; log=[{self.log.compact()}]"
                    )

                if role == "A" and self._counts["B1"] <= self._counts["A2"]:
                    raise VerificationError(
                        f"A2 for thread {thread_id} ran without an unmatched B1; "
                        f"counts={dict(self._counts)}, log=[{self.log.compact()}]"
                    )
                if role == "B" and self._counts["A1"] <= self._counts["B2"]:
                    raise VerificationError(
                        f"B2 for thread {thread_id} ran without an unmatched A1; "
                        f"counts={dict(self._counts)}, log=[{self.log.compact()}]"
                    )

            self._seen.add(event_key)
            self._counts[event_name] += 1
            self.log.record(f"{event_name}#{thread_id}")
            self._condition.notify_all()

        self._jitter()

    def _jitter(self) -> None:
        if self._max_jitter <= 0:
            return
        with self._rng_lock:
            delay = self._rng.uniform(0, self._max_jitter)
        time.sleep(delay)


def verify_rendezvous(
    solution_factory: Callable[[], RendezvousSolution],
    *,
    runs: int = 100,
    concurrent_pairs: int = 16,
    timeout: float = 1.0,
    max_jitter: float = 0.001,
) -> None:
    """Verify a rendezvous solution under adversarial and random schedules."""

    _run_with_leader(
        solution_factory,
        leader="A",
        seed=10_000,
        timeout=timeout,
        max_jitter=max_jitter,
    )
    _run_with_leader(
        solution_factory,
        leader="B",
        seed=20_000,
        timeout=timeout,
        max_jitter=max_jitter,
    )

    for run in range(runs):
        solution = solution_factory()
        verifier = RendezvousVerifier(seed=run, max_jitter=max_jitter)
        run_threads(
            {
                "A": lambda: solution.a(verifier.a1, verifier.a2),
                "B": lambda: solution.b(verifier.b1, verifier.b2),
            },
            timeout=timeout,
        )
        verifier.assert_complete()

    _run_multi_with_leader(
        solution_factory,
        leader="A",
        pairs=concurrent_pairs,
        seed=30_000,
        timeout=timeout,
        max_jitter=max_jitter,
    )
    _run_multi_with_leader(
        solution_factory,
        leader="B",
        pairs=concurrent_pairs,
        seed=40_000,
        timeout=timeout,
        max_jitter=max_jitter,
    )

    for run in range(runs):
        solution = solution_factory()
        verifier = MultiRendezvousVerifier(
            pairs=concurrent_pairs,
            seed=50_000 + run,
            max_jitter=max_jitter,
        )
        run_threads(
            {
                **{
                    f"A{thread_id}": (
                        lambda thread_id=thread_id: solution.a(
                            verifier.a1(thread_id),
                            verifier.a2(thread_id),
                        )
                    )
                    for thread_id in range(concurrent_pairs)
                },
                **{
                    f"B{thread_id}": (
                        lambda thread_id=thread_id: solution.b(
                            verifier.b1(thread_id),
                            verifier.b2(thread_id),
                        )
                    )
                    for thread_id in range(concurrent_pairs)
                },
            },
            timeout=timeout,
        )
        verifier.assert_complete()


def _run_with_leader(
    solution_factory: Callable[[], RendezvousSolution],
    *,
    leader: str,
    seed: int,
    timeout: float,
    max_jitter: float,
) -> None:
    solution = solution_factory()
    verifier = RendezvousVerifier(seed=seed, max_jitter=max_jitter)
    follower_gate = threading.Event()

    def run_a() -> None:
        if leader == "B":
            _wait_for_gate(follower_gate, timeout, "A")
        solution.a(verifier.a1, verifier.a2)

    def run_b() -> None:
        if leader == "A":
            _wait_for_gate(follower_gate, timeout, "B")
        solution.b(verifier.b1, verifier.b2)

    def after_start() -> None:
        first_event = f"{leader}1"
        try:
            if not verifier.wait_for(first_event, timeout / 2):
                raise VerificationError(
                    f"{leader} did not call {first_event} before timeout; "
                    f"log=[{verifier.log.compact()}]"
                )
            time.sleep(max(0.005, max_jitter * 4))
        finally:
            follower_gate.set()

    run_threads(
        {"A": run_a, "B": run_b},
        timeout=timeout,
        after_start=after_start,
    )
    verifier.assert_complete()


def _run_multi_with_leader(
    solution_factory: Callable[[], RendezvousSolution],
    *,
    leader: str,
    pairs: int,
    seed: int,
    timeout: float,
    max_jitter: float,
) -> None:
    solution = solution_factory()
    verifier = MultiRendezvousVerifier(
        pairs=pairs,
        seed=seed,
        max_jitter=max_jitter,
    )
    follower_gate = threading.Event()

    def run_a(thread_id: int) -> None:
        if leader == "B":
            _wait_for_gate(follower_gate, timeout, f"A{thread_id}")
        solution.a(verifier.a1(thread_id), verifier.a2(thread_id))

    def run_b(thread_id: int) -> None:
        if leader == "A":
            _wait_for_gate(follower_gate, timeout, f"B{thread_id}")
        solution.b(verifier.b1(thread_id), verifier.b2(thread_id))

    def after_start() -> None:
        first_event = f"{leader}1"
        try:
            if not verifier.wait_for_count(first_event, pairs, timeout / 2):
                raise VerificationError(
                    f"only saw {verifier._counts[first_event]} of {pairs} "
                    f"{first_event} calls before timeout; log=[{verifier.log.compact()}]"
                )
            time.sleep(max(0.005, max_jitter * 4))
        finally:
            follower_gate.set()

    run_threads(
        {
            **{
                f"A{thread_id}": (
                    lambda thread_id=thread_id: run_a(thread_id)
                )
                for thread_id in range(pairs)
            },
            **{
                f"B{thread_id}": (
                    lambda thread_id=thread_id: run_b(thread_id)
                )
                for thread_id in range(pairs)
            },
        },
        timeout=timeout,
        after_start=after_start,
    )
    verifier.assert_complete()


def _wait_for_gate(gate: threading.Event, timeout: float, thread_name: str) -> None:
    if not gate.wait(timeout):
        raise VerificationError(f"{thread_name} never received its start signal")
