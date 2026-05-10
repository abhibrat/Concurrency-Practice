from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
import random
import threading
from typing import Protocol

from concurrency_practice.verifiers.common import (
    EventLog,
    VerificationError,
    run_threads,
    sleep_random,
)


class ReusableBarrierSolution(Protocol):
    def wait(self) -> None:
        ...


class ReusableBarrierVerifier:
    def __init__(self, parties: int) -> None:
        if parties <= 0:
            raise ValueError("parties must be positive")
        self.parties = parties
        self.log = EventLog()
        self._lock = threading.Lock()
        self._arrived: dict[int, set[int]] = defaultdict(set)
        self._crossed: dict[int, set[int]] = defaultdict(set)

    def arrive(self, generation: int, thread_id: int) -> None:
        with self._lock:
            if thread_id in self._arrived[generation]:
                raise VerificationError(
                    f"thread {thread_id} arrived twice in generation {generation}; "
                    f"log=[{self.log.compact()}]"
                )
            self._arrived[generation].add(thread_id)
            self.log.record("arrive", generation=generation)

    def cross(self, generation: int, thread_id: int) -> None:
        with self._lock:
            arrived = self._arrived[generation]
            if len(arrived) != self.parties:
                missing = sorted(set(range(self.parties)) - arrived)
                raise VerificationError(
                    f"thread {thread_id} crossed generation {generation} before "
                    f"all parties arrived; missing={missing}, log=[{self.log.compact()}]"
                )

            if generation > 0:
                previous_crossed = self._crossed[generation - 1]
                if len(previous_crossed) != self.parties:
                    missing = sorted(set(range(self.parties)) - previous_crossed)
                    raise VerificationError(
                        f"thread {thread_id} crossed generation {generation} before "
                        f"generation {generation - 1} fully completed; "
                        f"missing_previous_crosses={missing}, log=[{self.log.compact()}]"
                    )

            if thread_id in self._crossed[generation]:
                raise VerificationError(
                    f"thread {thread_id} crossed twice in generation {generation}; "
                    f"log=[{self.log.compact()}]"
                )

            self._crossed[generation].add(thread_id)
            self.log.record("cross", generation=generation)

    def assert_complete(self, generations: int) -> None:
        expected = set(range(self.parties))
        for generation in range(generations):
            arrived = self._arrived[generation]
            crossed = self._crossed[generation]
            if arrived != expected or crossed != expected:
                raise VerificationError(
                    f"generation {generation} incomplete; "
                    f"arrived={sorted(arrived)}, crossed={sorted(crossed)}, "
                    f"log=[{self.log.compact()}]"
                )


def verify_reusable_barrier(
    barrier_factory: Callable[[int], ReusableBarrierSolution],
    *,
    parties: int = 5,
    generations: int = 25,
    runs: int = 20,
    timeout: float = 3.0,
    max_jitter: float = 0.001,
) -> None:
    """Verify a reusable barrier under repeated randomized schedules."""

    for run in range(runs):
        barrier = barrier_factory(parties)
        verifier = ReusableBarrierVerifier(parties)

        def worker(thread_id: int) -> None:
            rng = random.Random((run + 1) * 10_000 + thread_id)
            for generation in range(generations):
                sleep_random(rng, max_jitter)
                verifier.arrive(generation, thread_id)
                sleep_random(rng, max_jitter)
                barrier.wait()
                verifier.cross(generation, thread_id)
                sleep_random(rng, max_jitter)

        run_threads(
            {
                f"T{thread_id}": lambda thread_id=thread_id: worker(thread_id)
                for thread_id in range(parties)
            },
            timeout=timeout,
        )
        verifier.assert_complete(generations)
