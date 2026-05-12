# Concurrency Practice

This repo is a small LeetCode-style harness for practicing synchronization
problems from The Little Book of Semaphores.

You edit the solution files:

- `concurrency_practice/solutions/rendezvous.py`
- `concurrency_practice/solutions/reusable_barrier.py`
- `concurrency_practice/solutions/bounded_blocking_queue.py`
- `concurrency_practice/solutions/producer_consumer.py`
- `concurrency_practice/solutions/readers_writers.py`
- `concurrency_practice/solutions/no_starve_readers_writers.py`
- `concurrency_practice/solutions/writer_priority_readers_writers.py`

Install the test dependency into your active Python once:

```bash
python -m pip install -e '.[dev]'
```

Then run pytest. Poetry is not required for this repo:

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

## Bounded Blocking Queue

Implement `BoundedBlockingQueue.enqueue()`,
`BoundedBlockingQueue.dequeue()`, and `BoundedBlockingQueue.size()`.

This is the Little Book finite-buffer producer-consumer variation from
sections 4.1.4 through 4.1.6, packaged like a queue object. It also uses the
section 3.8 idea that semaphores can act as queues of waiting threads.

```python
queue = BoundedBlockingQueue(capacity=3)
queue.enqueue(10)
item = queue.dequeue()
current = queue.size()
```

The verifier checks:

- Items come out in FIFO order.
- `dequeue()` blocks while the queue is empty.
- `enqueue(item)` blocks while the queue is full.
- Many producers and consumers can use the queue concurrently.
- Every enqueued item is dequeued exactly once.
- `size()` reports the current number of queued items and ends at zero.

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

## No-Starve Readers-Writers

Implement `NoStarveReadersWriters.reader()` and
`NoStarveReadersWriters.writer()`.

This is the Little Book sections 4.2.4 and 4.2.5 variant. It starts with the
basic readers-writers safety rules and adds:

- If a writer arrives while readers are active, later readers must queue behind
  that writer.
- When the current readers leave, at least one waiting writer must enter before
  those later readers.

The book's hint is to add a `turnstile`. Writers hold the turnstile while
waiting for the room to become empty, which prevents later readers from
continually extending the reader group.

## Writer-Priority Readers-Writers

Implement `WriterPriorityReadersWriters.reader()` and
`WriterPriorityReadersWriters.writer()`.

This is the Little Book sections 4.2.6 and 4.2.7 variant. It starts with the
basic readers-writers safety rules and adds:

- Once a writer is queued, later readers must wait.
- If several writers are queued, readers should not enter until the queued
  writer group has drained.

The book's solution uses reader and writer lightswitches plus `noReaders` and
`noWriters` semaphores. The tradeoff is that readers can starve or face long
delays while writers keep arriving.

## Useful Commands

Run just one problem:

```bash
python -m pytest tests/test_rendezvous.py -q
python -m pytest tests/test_reusable_barrier.py -q
python -m pytest tests/test_bounded_blocking_queue.py -q
python -m pytest tests/test_producer_consumer.py -q
python -m pytest tests/test_readers_writers.py -q
python -m pytest tests/test_no_starve_readers_writers.py -q
python -m pytest tests/test_writer_priority_readers_writers.py -q
```

Increase stress:

```bash
CONCURRENCY_STRESS_RUNS=500 python -m pytest tests/test_rendezvous.py -q
RENDEZVOUS_CONCURRENT_PAIRS=64 CONCURRENCY_STRESS_RUNS=200 python -m pytest tests/test_rendezvous.py -q
BARRIER_STRESS_RUNS=100 BARRIER_GENERATIONS=50 python -m pytest tests/test_reusable_barrier.py -q
BOUNDED_BLOCKING_QUEUE_STRESS_RUNS=200 BOUNDED_BLOCKING_QUEUE_CAPACITY=3 python -m pytest tests/test_bounded_blocking_queue.py -q
PRODUCER_CONSUMER_STRESS_RUNS=200 PRODUCER_CONSUMER_CAPACITY=3 python -m pytest tests/test_producer_consumer.py -q
READERS_WRITERS_STRESS_RUNS=200 python -m pytest tests/test_readers_writers.py -q
NO_STARVE_READERS_WRITERS_STRESS_RUNS=100 python -m pytest tests/test_no_starve_readers_writers.py -q
WRITER_PRIORITY_READERS_WRITERS_STRESS_RUNS=100 WRITER_PRIORITY_QUEUED_WRITERS=5 python -m pytest tests/test_writer_priority_readers_writers.py -q
```

Verify the harness itself against known-good reference implementations:

```bash
python -m pytest tests/test_reference_solutions.py -q
```
