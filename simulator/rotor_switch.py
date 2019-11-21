from helpers import *
from functools import partial

class Empty:
    pass

class RotorSwitch:
    def __init__(self, id, n_ports):
        self.id   = id
        self.dests = [None for _ in range(n_ports)]
        # Tor id #1 is at port #1 for simplicity, wlog on a rotornet
        self.disable()

    def disable(self):
        self.enabled = False
    def enable(self):
        self.enabled = True

    def install_matchings(self, matchings):
        self.disable()
        for src, dst in matchings:
            self.dests[src.id] = dst
        # Wait for reconfiguration time
        #Delay(delay_t = .001)(self.enable)()
        self.enable()

    def connect_tors(self, tors):
        self.disable()
        self.tors = tors
        for t_id, tor in enumerate(tors):
            handle = Empty()
            handle.src = tor
            handle.recv = partial(self.recv, tor)

            tor.connect_rotor(self, handle)

    def recv(self, tor, packets, flow):
        if self.enabled:
            self.dests[tor.id].recv(flow=flow, packets=packets)
        else:
            # Could assert false, but just drop
            print("%s: Dropping packets from tor %s" % (self, tor))

    def __str__(self):
        return "Rotor %s" % self.id

