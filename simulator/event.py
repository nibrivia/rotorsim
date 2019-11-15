import heapq
from functools import wraps

class Registry:
    def __init__(self, limit = 1000):
        # Start allows to start doing some prep before time = 0
        self.time = 0
        self.queue = []
        self.limit = limit
        self.has_run = False

    def call_in(self, delay, fn, *args, **kwargs):
        """This will register the call in `delay` time from now"""
        if self.has_run:
            assert delay > 0, "Event <%s> has to be in the future, not %f" % (fn, delay)
        # Not threadsafe
        heapq.heappush(self.queue, (self.time+delay, fn, args, kwargs))

    def run_next(self):
        """This function will only return when we're done with all events.
        This should probably never happen"""

        self.has_run = True
        while True:
            # Could be moved in while statement, but this prints message...
            if len(self.queue) == 0:
                print("@%.2f: no more events in registry" % self.time)
                break
            if self.time > self.limit:
                print("reached past simulation time limit")
                break

            # Also not threadsafe
            self.time, fn, args, kwargs = heapq.heappop(self.queue)
            #print(" %.2f> %s %s, %s" % (self.time, fn.__name__, args, kwargs))
            fn(*args, **kwargs) # Call fn (it may register more events!)

def delay(registry, delay_t):
    """Decorator to force anyone calling this function to incure a delay of `delay`"""
    def decorator_with_delay(fn):
        @wraps(fn)
        def called_fn(*args, **kwargs):
            registry.call_in(delay_t, fn, *args, **kwargs)
        return called_fn
    return decorator_with_delay



if __name__ == "__main__":

    r = Registry(limit = 10)

    # Toy function to play with
    @delay(r, .5)
    def hello(name = ""):
        """Says hello, a lot"""
        print("hello %s @%.2f" % (name, r.time))
        # (un)comment for a "recursive" call every .5
        hello(name = name)

    # Olivia will get called @.5: hello() incurs a .5 delay
    hello("Olivia")

    # Amir   will get called @.6: at .1 call hello(), which incurs a .5 delay
    r.call_in(delay = .1, fn = hello, name = "Amir  ")

    # Start the simulation
    r.run_next()

