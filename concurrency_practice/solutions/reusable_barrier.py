import threading

"""Reusable barrier exercise.

Implement `ReusableBarrier.wait()` for `parties` threads.

Each call blocks until all parties have arrived for the current generation,
then the barrier resets correctly for the next generation.

Verifier invariants:
- No thread crosses a generation until all parties have arrived there.
- No thread crosses generation k + 1 before all threads from generation k
  have crossed.
- Every thread crosses every generation exactly once.

Run:
    python -m pytest tests/test_reusable_barrier.py -q

Stress:
    BARRIER_STRESS_RUNS=100 BARRIER_GENERATIONS=50 \
        python -m pytest tests/test_reusable_barrier.py -q
"""

class ReusableBarrier:
    """Starter solution for a reusable barrier.

    `wait()` must block until `parties` threads have called it for the current
    generation, then it must reset correctly for the next generation.
    """

    def __init__(self, parties: int) -> None:
        if parties <= 0:
            raise ValueError("parties must be positive")
        self.parties = parties
        self.lock = threading.Lock()
        self.t1=threading.Semaphore(0)
        self.t2=threading.Semaphore(0)
        self.c=0
       

    def wait(self) -> None:

        with self.lock:
            self.c+=1
            if self.c==self.parties:
                for i in range(self.parties):
                    self.t1.release()
        self.t1.acquire()            


        with self.lock:
            self.c-=1
            if self.c==0:
                for i in range(self.parties):
                    self.t2.release()
        self.t2.acquire()            

        # while self.cond:
        #     self.c+=1
        #     gen=self.gen
        #     if self.c==self.parties:
        #         self.c=0
        #         self.gen+=1
        #         self.cond.notify_all()
        #     else:
        #         while gen==self.gen:
        #             self.cond.wait()    
