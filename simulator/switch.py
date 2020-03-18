from params import PARAMS
from collections import deque

class QueueLink:
    def __init__(self,
            dst_recv,
            name  = "",
            delay = 0,
            ms_per_byte    = 0,
            max_size_bytes = None,
            ):
        # ID
        self.name = name

        # Internal state
        self._queue   = deque()
        self._enabled = False

        self.queue_size_bytes = 0
        self.queue_size_max   = max_size_bytes
        self.ms_per_byte      = ms_per_byte

        # Destination values
        self.dst_recv = dst_recv

        # Link params
        self.delay = delay

    def enq(self, packet):
        self._queue.appendleft(packet)
        self.send()

    def _enable(self):
        self._enabled = True
        self._send()

    def send(self):
        # Currently sending something, or no packets to send
        if not self._enabled or len(self._queue) == 0:
            return

        # Disable
        self._enabled = False

        # Check if there are packet
        pkt = self._queue.pop()
        self.dst_recv(pkt)

        # Re-enable after a delay
        delay = self.delay + pkt.size * ms_per_byte
        R.call_in(delay, self._enable)

    def __str__(self):
        return self.name

class Switch:
    def __init__(self, id):
        self.id = id
        self._disable()

        # in and out links
        self.rx    = [None for _ in range(PARAMS.n_tor)]
        self.tx    = [None for _ in range(PARAMS.n_tor)]

        # Mapping from in->out
        self.dests = [None for _ in range(PARAMS.n_tor)]

        # Create the in-links now
        for tor_id in range(PARAMS.n_tor):
            recv = self.make_recv(tor_id)
            name = "%s:%-2d" % (self, tor_id)
            handle = QueueLink(recv, name = name, delay = 0)

            self.rx[tor_id] = handle


    def start(self):
        raise NotImplementedError

    def _enable(self):
        self.enabled = True
    def _disable(self):
        self.enabled = False

    def connect_tors(self, tors):
        """'Physically' connect the switch to its ports"""
        for tor_id, tor in enumerate(tors):
            # Tell the ToR about us
            tor_rx = tor.connect_backbone(port_id = self.id, switch = self, queue = self.rx[tor_id])

            # Give us a way to talk to the ToR
            self.tx[tor_id] = tor_rx

    def make_recv(self, port_id):
        """Makes a dedicated function for this incoming port"""

        def recv(packet):
            """Actually receives packets for `port_id`"""
            if not self.enabled:
                assert False,\
                        "@%.3f%s: Dropping packets from tor %s" % (R.time, self, tor)

            # Forward to destination
            dst_id = self.dests[port_id]
            self.out[dst_id].recv(packet)

        return recv

