from helpers import vprint, color_str_
from params import PARAMS
from collections import deque
from event import R, Delay

class QueueLink:
    def __init__(self,
            dst_recv,
            name  = "",
            delay = 0,
            bandwidth_Bms = None,
            max_size_bytes = None,
            ):
        # ID
        self.name = name
        if name is None:
            self.name = "{}"

        # Internal state
        self._queue   = deque()
        self._enabled = True

        self.q_size_B = 0
        self.queue_size_max   = max_size_bytes
        if bandwidth_Bms is None:
            self.ms_per_byte = 0
        else:
            self.ms_per_byte = 1/bandwidth_Bms

        # Destination values
        self.dst_recv = dst_recv

        # Link params
        self.prop_delay = delay

    def enq(self, packet):
        if packet.flow_id == 0:
            vprint("queue: %s enq  %s" % (packet, self))
        if self.queue_size_max is not None and \
                self.q_size_B + packet.size_B > self.queue_size_max:
            if packet.flow_id == 0:
                vprint("%s drop %s" % (packet, self))
            return
        self._queue.appendleft(packet)
        self.q_size_B += packet.size_B
        # Having a delay of 0 makes it so that even if we can send
        # immediately, it waits until the caller is done, making 
        # the behavior of enq consistent regardless of current queue
        # size
        R.call_in(0, self._send)

    def _enable(self):
        self._enabled = True
        self._send()

    def _send(self):
        # Currently sending something, or no packets to send
        if not self._enabled or len(self._queue) == 0:
            return

        # Disable
        self._enabled = False

        # Get packet and compute tx time
        pkt = self._queue.pop()
        self.q_size_B -= pkt.size_B
        if pkt.flow_id == 0:
            vprint("queue: %s sent %s" % (pkt, self))
        tx_delay = pkt.size_B * self.ms_per_byte
        #vprint("tx_delay", pkt, tx_delay)

        R.call_in(tx_delay, self._enable)
        R.call_in(self.prop_delay + tx_delay, self.dst_recv, pkt)

    @color_str_
    def __str__(self):
        if self.queue_size_max is None:
            frac_full = self.q_size_B
        else:
            frac_full = self.q_size_B / self.queue_size_max
        return "%s [%2d%%]" % (self.name, frac_full*100)

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

    def connect_tors(self, links):
        """'Physically' connect the switch to its ports"""
        self.tx = links
        #for tor_id, link in enumerate(links):
            ## Give us a way to talk to the ToR
            #self.tx[tor_id] = tor.recv

    def make_recv(self, port_id):
        """Makes a dedicated function for this incoming port"""

        def recv(packet):
            """Actually receives packets for `port_id`"""
            if packet.flow_id == PARAMS.flow_print:
                vprint("swtch: %s recv %s" % (packet, self))

            if not self.enabled:
                assert False,\
                        "@%.3f%s: Dropping packets from tor %s" % (R.time, self, tor)

            # Forward to destination
            dst_id = self.dests[port_id]
            self.tx[dst_id].enq(packet)

        return recv

