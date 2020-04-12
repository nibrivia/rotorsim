from helpers import vprint, color_str_
from params import PARAMS
from collections import deque
from event import R, Delay
from nic import NIC
from debuglog import DebugLog


class Switch(DebugLog):
    def __init__(self, id):
        self.id = id
        self._disable()

        # in and out links
        self.tx    = [None for _ in range(PARAMS.n_tor)]

        # Mapping from in->out
        self.dests = [None for _ in range(PARAMS.n_tor)]

        # Statistics
        self.packets_by_port = [0 for _ in range(PARAMS.n_tor)]


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

            if not self.enabled:
                assert False,\
                        "@%.3f%s: %s drop from :%s" % (R.time, self, packet, port_id)


            # Forward to destination
            dst_id = self.dests[port_id]

            if packet.flow_id == PARAMS.flow_print:
                vprint("sw % 2d: %s recv %s -> %s" % (self.id, packet, self, dst_id))

            self.tx[dst_id].enq(packet)

            self.packets_by_port[port_id] += 1

        return recv

