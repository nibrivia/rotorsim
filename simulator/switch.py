from params import PARAMS
from functools import partial

class Empty:
    def __str__(self):
        return self.name

class Switch:
    def __init__(self, id):
        self.id = id
        self._disable()

        self.dests = [None for _ in range(PARAMS.n_tor)]
        self.n_packets = [0 for _ in range(PARAMS.n_tor)]

    def start(self):
        raise NotImplementedError

    def _enable(self):
        self.enabled = True
    def _disable(self):
        self.enabled = False

    def connect_tors(self, tors):
        """'Physically' connect the switch to its ports"""
        self.tors = tors
        for t_id, tor in enumerate(tors):
            # This handle thing is essentially giving the illusion that
            # each port has its own .recv function. That's annoying to
            # do in practice, so we just give out an object with a partial
            handle = Empty()
            handle.recv = partial(self.recv, tor)
            handle.name = str(self)
            handle.id   = self.id
            tor.connect_queue(port_id = self.id, switch = self, queue = handle)

    def recv(self, tor, packet):
        if not self.enabled:
            assert False,\
                    "@%.3f%s: Dropping packets from tor %s" % (R.time, self, tor)

        # Get destination
        dst = self.dests[tor.id]

        #TODO track utilization

        # Send to destination
        dst.recv(packet)

