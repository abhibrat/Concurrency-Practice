from concurrency_practice.reference_solutions import (
    RendezvousReference,
    ReusableBarrierReference,
)
from concurrency_practice.verifiers import verify_rendezvous, verify_reusable_barrier


def test_reference_rendezvous() -> None:
    verify_rendezvous(RendezvousReference, runs=25)


def test_reference_reusable_barrier() -> None:
    verify_reusable_barrier(
        ReusableBarrierReference,
        parties=5,
        generations=15,
        runs=5,
    )
