from helpers import *
from functools import partial
from event import R, Delay

class Empty:
    def __str__(self):
        return self.name

class RotorSwitch:
    def __init__(self,
            id, n_ports,
            slot_duration, clock_jitter):
        # About me
        self.id   = id
        self.dests = [None for _ in range(n_ports)]

        # About time
        self.slot_duration = slot_duration
        self.clock_jitter  = clock_jitter
        self.slot_t        = -1


        self._disable()

    def add_matchings(self, matchings_by_slot):
        self.matchings_by_slot = matchings_by_slot

    def new_slot(self):
        self.slot_t += 1
        n_slots = len(self.matchings_by_slot)

        current_matchings = self.matchings_by_slot[self.slot_t % n_slots]
        self.install_matchings(current_matchings)
        pass


    def _disable(self):
        self.enabled = False
    def _enable(self):
        self.enabled = True

    def install_matchings(self, matchings):
        self._disable()
        for src, dst in matchings:
            self.dests[src.id] = dst
        # Wait for reconfiguration time
        Delay(delay = .001, jitter = self.clock_jitter)(self._enable)()
        #self._enable()

    def connect_tors(self, tors):
        self._disable()
        self.tors = tors
        for t_id, tor in enumerate(tors):
            handle = Empty()
            handle.recv = partial(self.recv, tor)
            handle.name = str(self)
            tor.connect_rotor(self, handle)

    @Delay(0)
    def recv(self, tor, packets):
        if self.enabled:
            self.dests[tor.id].recv(self.id, packets)
        else:
            # Could assert false, but just drop
            print("%s: Dropping packets from tor %s" % (self, tor))

    def __str__(self):
        return "Rotor %s" % self.id

