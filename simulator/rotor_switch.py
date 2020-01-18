from helpers import *
from functools import partial
from event import R, Delay

class Empty:
    def __str__(self):
        return self.name

class RotorSwitch:
    def __init__(self,
            id, n_ports,
            verbose, logger = None):
        # About me
        self.id   = id
        self.dests = [None for _ in range(n_ports)]
        # dests[1] is the destination of a packet arriving in on port 1

        # About time
        self.slice_t              = -1

        # About IO
        self.verbose = verbose
        self.logger  = logger

        self._disable()

    def add_matchings(self, matchings_by_slot, n_rotor):
        self.matchings_by_slot = matchings_by_slot
        self.n_rotor = n_rotor

    def start(self, slice_duration):
        self._enable()
        self.install_matchings(self.matchings_by_slot[0])

        # Create a recursive call
        if slice_duration > 0:
            self.new_slice = Delay(slice_duration)(self._new_slice)
        else:
            self.new_slice = lambda: None

        self._new_slice()

    def _new_slice(self):
        self.slice_t += 1
        n_slots = len(self.matchings_by_slot)

        # Skip if it's not our turn
        if self.slice_t % self.n_rotor != self.id:
            self.new_slice() # This passes through, it has a delay on it
            return

        # Compute our new matching
        slot_t = self.slice_t // self.n_rotor
        current_matchings = self.matchings_by_slot[slot_t % n_slots]
        self.install_matchings(current_matchings)

        # Re-call ourselves
        self.new_slice()



    def _disable(self):
        self.enabled = False
    def _enable(self):
        self.enabled = True

    def install_matchings(self, matchings):
        self._disable()
        for src, dst in matchings:
            self.dests[src.id] = dst
        # Wait for reconfiguration time, high priority so that reconf 0 has no down time
        self._enable()

    def connect_tors(self, tors):
        assert not self.enabled
        self.tors = tors
        for t_id, tor in enumerate(tors):
            # This handle thing is essentially giving the illusion that
            # each port has its own .recv function. That's annoying to
            # do in practice, so we just give out an object with a partial
            handle = Empty()
            handle.recv = partial(self.recv, tor)
            handle.name = str(self)
            handle.id   = self.id
            tor.connect_queue(self.id, handle)

    #@Delay(0)
    def recv(self, tor, packet):
        if self.enabled:
            dst = self.dests[tor.id]
            #if self.verbose:
                #print("@%.2f                 %s to \033[01m%s\033[00m\033[00m"
                        #% (R.time, self, dst))
            if self.logger is not None:
                self.logger.log(src = tor, dst = dst, rotor = self, packet = packet)

            self.dests[tor.id].recv(packet)

        else:
            # Could assert false, but just drop
            print("%s: Dropping packets from tor %s" % (self, tor))

    def __str__(self):
        return "Rot %s" % self.id

