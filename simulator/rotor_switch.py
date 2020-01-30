from helpers import *
from logger import LOG
from functools import partial
from event import R, Delay

class Empty:
    def __str__(self):
        return self.name

class RotorSwitch:
    def __init__(self,
            id, n_ports,
            verbose):
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

        self._disable()

    def add_matchings(self, matchings_by_slot, n_rotor):
        self.matchings_by_slot = matchings_by_slot
        self.n_rotor = n_rotor

    def start(self, slice_duration, reconf_time = 0, is_rotor = True):
        self.reconf_time = reconf_time # TODO

        self._disable()
        self.install_matchings(self.matchings_by_slot[0])

        # Create a recursive call
        if slice_duration is not None and slice_duration > 0:
            self.new_slice = Delay(slice_duration + reconf_time, priority = 2)(self._new_slice)
            self.is_rotor = is_rotor
            self.slice_duration = slice_duration
        else:
            self.new_slice = lambda: None

        R.call_in(slice_duration, priority = 1, fn = self._disable)
        self._new_slice()

    # Returns True/False if the connection can be established
    def request_matching(self, tor, dst_id):
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
        return round(R.time / (self.slice_duration+self.reconf_time))

    def _new_slice(self):
        n_slots = len(self.matchings_by_slot)

        # Skip if it's not our turn
        if not self.is_rotor and self.slice_t % self.n_rotor != self.id:
            self._enable()
            return

        slot_id = self.slot_t % n_slots
        if self.verbose:
            print("%.6f %s switch %d" % (R.time, self, slot_id))
        # Compute our new matching
        current_matchings = self.matchings_by_slot[self.slot_t % n_slots]
        self.install_matchings(current_matchings)

        # Re-call ourselves
        self._enable()
        R.call_in(self.slice_duration, self._disable)
        self.new_slice()


    def _disable(self):
        self.enabled = False
    def _enable(self):
        self.enabled = True

    def install_matchings(self, matchings):
        assert not self.enabled, "@%.3f" % R.time
        for src, dst in matchings:
            self.dests[src.id] = dst
        # Wait for reconfiguration time, high priority so that reconf 0 has no down time

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

    def recv(self, tor, packet):
        if not self.enabled:
            assert False,\
                    "@%.3f%s: Dropping packets from tor %s" % (R.time, self, tor)

        # Get destination
        dst = self.dests[tor.id]

        # Some checking for rotors
        if self.is_rotor:
            intended_dst, port_id, slot_t, lump = packet
            assert intended_dst == dst.id, \
                    "%.3f %s %d:%d->(%d) actual %d. Tor slot %d Rot slot %d\n%s" % (
                        R.time,
                        self,
                        tor.id, port_id,
                        intended_dst, dst.id,
                        slot_t, self.slot_t,
                        self.matchings_by_slot[self.slot_t][tor.id][1].id)
            dst.rx_rotor(lump)
            return

        # Print
        if self.verbose:
            p = packet
            print("@%s %.3f (%2d)    %d  ->%d %s\033[00m"
                    % (self.enabled, R.time, self.id, tor.id, dst.id, p))
            assert p.intended_dest == dst.id

        # Send non-rotor packet
        self.dests[tor.id].recv(packet)

    def __str__(self):
        return "Rot %s" % self.id

