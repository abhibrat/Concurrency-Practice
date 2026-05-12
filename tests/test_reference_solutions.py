from concurrency_practice.reference_solutions import (
    BoundedBlockingQueueReference,
    NoStarveReadersWritersReference,
    ProducerConsumerReference,
    ReadersWritersReference,
    RendezvousReference,
    ReusableBarrierReference,
    WriterPriorityReadersWritersReference,
)
from concurrency_practice.verifiers import (
    verify_bounded_blocking_queue,
    verify_no_starve_readers_writers,
    verify_producer_consumer,
    verify_readers_writers,
    verify_rendezvous,
    verify_reusable_barrier,
    verify_writer_priority_readers_writers,
)


def test_reference_bounded_blocking_queue() -> None:
    verify_bounded_blocking_queue(
        BoundedBlockingQueueReference,
        capacity=3,
        runs=10,
    )


def test_reference_rendezvous() -> None:
    verify_rendezvous(RendezvousReference, runs=25)


def test_reference_reusable_barrier() -> None:
    verify_reusable_barrier(
        ReusableBarrierReference,
        parties=5,
        generations=15,
        runs=5,
    )


def test_reference_producer_consumer() -> None:
    verify_producer_consumer(
        ProducerConsumerReference,
        capacity=3,
        runs=10,
    )


def test_reference_readers_writers() -> None:
    verify_readers_writers(
        ReadersWritersReference,
        reader_threads=6,
        writer_threads=3,
        operations_per_thread=4,
        runs=10,
    )


def test_reference_no_starve_readers_writers() -> None:
    verify_no_starve_readers_writers(
        NoStarveReadersWritersReference,
        runs=10,
    )


def test_reference_writer_priority_readers_writers() -> None:
    verify_writer_priority_readers_writers(
        WriterPriorityReadersWritersReference,
        runs=10,
        queued_writers=3,
    )
