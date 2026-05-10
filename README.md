# Concurrency Practice

This repo is a small LeetCode-style harness for practicing synchronization
problems from The Little Book of Semaphores.

You edit the solution files:

- `concurrency_practice/solutions/rendezvous.py`
- `concurrency_practice/solutions/reusable_barrier.py`
- `concurrency_practice/solutions/producer_consumer.py`
- `concurrency_practice/solutions/readers_writers.py`

Then run pytest:

```bash
python -m pytest -q
```

The starter solutions are intentionally wrong. The tests are the verifier.

## Rendezvous

Implement `Rendezvous.a()` and `Rendezvous.b()` so these invariants always
hold for one `A` thread and one `B` thread:

- `A1` happens before `A2`.
- `B1` happens before `B2`.
- Both `A1` and `B1` happen before either `A2` or `B2`.

The verifier also stress-tests many `A` and `B` threads calling the same
`Rendezvous` instance. For that reusable/counting version:

- Every `A2` must have a prior unmatched `B1`.
- Every `B2` must have a prior unmatched `A1`.
- This is not a barrier; one pair may complete before later threads arrive.

The verifier passes callback functions into your methods:

```python
def a(self, a1, a2):
    a1()
    # synchronize here
    a2()

def b(self, b1, b2):
    b1()
    # synchronize here
    b2()
```

## Reusable Barrier

Implement `ReusableBarrier.wait()` for `parties` threads. Each call blocks
until all parties have arrived for that generation. The verifier checks:

- No thread crosses a generation until all parties have arrived there.
- No thread crosses generation `k + 1` before all parties have crossed
  generation `k`.
- Every thread crosses every generation exactly once.

## Producer-Consumer

Implement `ProducerConsumer.produce()` and `ProducerConsumer.consume()` for a
finite buffer. The verifier owns the actual buffer and passes callbacks into
your methods:

```python
def produce(self, item, put):
    # synchronize here
    put(item)

def consume(self, get):
    # synchronize here
    return get()
```

The verifier checks:

- Buffer access is exclusive.
- Buffer size never goes below 0 or above `capacity`.
- Consumers block while the buffer is empty.
- Producers block while the buffer is full.
- Every consumed item was produced.
- No item is consumed more than once.

## Readers-Writers

Implement `ReadersWriters.reader()` and `ReadersWriters.writer()` for the
basic readers-writers problem. The verifier passes callbacks into your methods:

```python
def reader(self, read):
    # enter as reader
    read()
    # leave as reader

def writer(self, write):
    # enter as writer
    write()
    # leave as writer
```

The verifier checks:

- Multiple readers are allowed to read at the same time.
- A writer must be alone.
- No reader may enter while a writer is active.
- No writer may enter while any reader is active.

Writer-priority and no-starve variants are separate extensions and are not
required by this verifier.

## Useful Commands

Run just one problem:

```bash
python -m pytest tests/test_rendezvous.py -q
python -m pytest tests/test_reusable_barrier.py -q
python -m pytest tests/test_producer_consumer.py -q
python -m pytest tests/test_readers_writers.py -q
```

Increase stress:

```bash
CONCURRENCY_STRESS_RUNS=500 python -m pytest tests/test_rendezvous.py -q
RENDEZVOUS_CONCURRENT_PAIRS=64 CONCURRENCY_STRESS_RUNS=200 python -m pytest tests/test_rendezvous.py -q
BARRIER_STRESS_RUNS=100 BARRIER_GENERATIONS=50 python -m pytest tests/test_reusable_barrier.py -q
PRODUCER_CONSUMER_STRESS_RUNS=200 PRODUCER_CONSUMER_CAPACITY=3 python -m pytest tests/test_producer_consumer.py -q
READERS_WRITERS_STRESS_RUNS=200 python -m pytest tests/test_readers_writers.py -q
```

Verify the harness itself against known-good reference implementations:

```bash
python -m pytest tests/test_reference_solutions.py -q
```
