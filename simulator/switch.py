from helpers import vprint, color_str_
from params import PARAMS
from collections import deque
from event import R, Delay
from nic import NIC


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
            handle = NIC(recv, name = name, delay = 0)

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
                        "@%.3f%s: %s drop from :%s" % (R.time, self, packet, port_id)

            # Forward to destination
            dst_id = self.dests[port_id]
            self.tx[dst_id].enq(packet)

        return recv

