import heapq
from functools import wraps
from itertools import count
import random

class Registry:
    def __init__(self, limit = 1000):
        # Start allows to start doing some prep before time = 0
        self.time = 0
        self.queue = []
        self.limit = limit
        self.has_run = False
        self.count  = 0

    def call_in(self, delay, fn, *args, priority = 0, **kwargs):
        """This will register the call in `delay` time from now, ties broken by priority, then first to register"""
        #if self.has_run:
            #assert delay >= 0, "Event <%s> has to be in the future, not %f" % (fn, delay)
        # Not threadsafe
        self.count += 1
        heapq.heappush(self.queue, (self.time+delay, priority, self.count, fn, args, kwargs))
        return count

    def stop(self):
        self.running = False

    def run_next(self):
        """This function will only return when we're done with all events.
        This should probably never happen"""

        self.has_run = True
        self.running = True
        while self.running:
            # Could be moved in while statement, but this prints message...
            if len(self.queue) == 0:
                print("@%.3f: no more events in registry" % self.time)
                break
            if self.time >= self.limit:
                print("reached past simulation time limit")
                break

            # Also not threadsafe
            self.time, _, _, fn, args, kwargs = heapq.heappop(self.queue)
            #print(" %.2f> #%d %s %s, %s" % (self.time, count, fn.__name__, args, kwargs))
            fn(*args, **kwargs) # Call fn (it may register more events!)

#@dataclass if python3 > 3.7
class Delay:
    """Decorator to force anyone calling this function to incure a delay of `delay`
    Optionally adds a random uniform delay of +/- `jitter`"""
    #delay_t: float
    #max_jitter: float = 0
    def __init__(self, delay, jitter=0, priority=0):
        assert delay >= 0, "Delay must be non-negative"
        assert jitter >= 0, "Jitter must be non-negative"
        assert jitter <= delay, "Jitter must be smaller than delay"

        self.delay    = delay
        self.jitter   = jitter
        self.priority = priority

    def __call__(self, fn):
        if self.jitter == 0:
            @wraps(fn)
            def called_fn(*args, **kwargs):
                R.call_in(self.delay, fn, *args, priority = self.priority, **kwargs)
            return called_fn
        else:
            @wraps(fn)
            def called_fn(*args, **kwargs):
                jitter = random.uniform(-self.jitter, self.jitter)
                R.call_in(self.delay+jitter, fn, *args, priority = self.priority, **kwargs)
            return called_fn

def stop_simulation(r):
    r.stop()


R = Registry()

if __name__ == "__main__":

    # Toy function to play with
    @Delay(5)
    def hello(name = ""):
        """Says hello, a lot"""
        print("hello 1 %s @%.2f" % (name, R.time))
        # (un)comment for a "recursive" call every .5
        #hello(name = name)

    # Olivia will get called @.5: hello() incurs a .5 delay
    hello("Olivia")

    # Amir   will get called @.6: at .1 call hello(), which incurs a .5 delay
    R.call_in(delay = 0.5, fn = hello, name = "Amir  ")

    # Start the simulation
    R.run_next()

