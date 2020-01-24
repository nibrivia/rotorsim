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

        # for cache
        self.available_up = [True for _ in range(n_ports)]
        self.available_dn = [True for _ in range(n_ports)]

        # About time
        self.slice_t              = -1

        # About IO
        self.verbose = verbose
        self.logger  = logger

        self._disable()

    def add_matchings(self, matchings_by_slot, n_rotor):
        self.matchings_by_slot = matchings_by_slot
        self.n_rotor = n_rotor

    def start(self, slice_duration, is_rotor = True):
        self._enable()
        self.install_matchings(self.matchings_by_slot[0])

        # Create a recursive call
        if slice_duration is not None and slice_duration > 0:
            self.new_slice = Delay(slice_duration, priority = -1000)(self._new_slice)
            self.is_rotor = is_rotor
            self.slice_duration = slice_duration
        else:
            self.new_slice = lambda: None

        self._new_slice()

    # Returns True/False if the connection can be established
    def request_matching(self, tor, dst_id):
        #print("%s: %s requesting matching to %s" % (self, tor, dst_id))
        # You should know better than that
        assert self.available_up[tor.id]

        # Make sure the connection can be established
        if not self.available_dn[dst_id]:
            return False

        # True it
        self.available_up[tor.id] = False
        self.available_dn[dst_id] = False

        self.dests[tor.id] = self.tors[dst_id]

        return True


    def release_matching(self, tor):
        #print("%s releasing matching from %s" % (self, tor))
        self.available_up[tor.id] = True
        dst = self.dests[tor.id]
        self.available_dn[dst.id] = True

        self.dests[tor.id] = None

        # TODO notify
        for tor in self.tors:
            tor._send(self.id)
            # tor.cache_free(dst.id)
            pass

    @property
    def slot_t(self):
        return round(R.time / self.slice_duration)

    def _new_slice(self):
        n_slots = len(self.matchings_by_slot)

        # Skip if it's not our turn
        if not self.is_rotor and self.slice_t % self.n_rotor != self.id:
            assert False
            self.new_slice() # This passes through, it has a delay on it
            return

        # Compute our new matching
        current_matchings = self.matchings_by_slot[self.slot_t % n_slots]
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
            tor.connect_queue(port_id = self.id, switch = self, queue = handle)

    @Delay(0, priority = -100)
    def recv(self, tor, packet):
        if self.enabled:
            dst = self.dests[tor.id]
            if self.verbose:
                p = packet
                print("@%.3f         %d  ->%d %3d[%s->%s]#%d\033[00m"
                        % (R.time, tor.id, dst.id,
                           p.flow_id, p.src_id, p.dst_id, p.seq_num))
            if self.logger is not None:
                self.logger.log(src = tor, dst = dst, rotor = self, packet = packet)

            self.dests[tor.id].recv(packet)

        else:
            # Could assert false, but just drop
            print("%s: Dropping packets from tor %s" % (self, tor))

    def __str__(self):
        return "Rot %s" % self.id

