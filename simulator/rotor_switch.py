from helpers import vprint
from logger import LOG
from event import R, Delay
from switch import Switch
from params import PARAMS


class RotorSwitch(Switch):
    def __init__(self, id):
        """A rotor switch"""
        super().__init__(id)

        self.slice_t   = -1


    def add_matchings(self, matchings_by_slot, n_rotor):
        self.matchings_by_slot = matchings_by_slot

    def start(self):
        self._disable()
        self.install_matchings(self.matchings_by_slot[0])

        # Create a recursive call
        self.new_slice = Delay(PARAMS.slot_duration + PARAMS.reconfiguration_time,
                priority = 2)(self._new_slice)
        R.call_in(PARAMS.slot_duration, priority = 1, fn = self._disable)

        self._new_slice()

    @property
    def slot_t(self):
        return round(R.time / (PARAMS.slot_duration+PARAMS.reconfiguration_time))

    def _new_slice(self):
        n_slots = len(self.matchings_by_slot)
        slot_id = self.slot_t % n_slots
        #vprint("%.6f %s              slot_id %d" % (R.time, self, slot_id))


        #vprint("%.6f %s switching to slot_id %d" % (R.time, self, slot_id))
        # Compute our new matching
        current_matchings = self.matchings_by_slot[self.slot_t % n_slots]
        self.install_matchings(current_matchings)

        # Re-call ourselves
        self._enable()
        R.call_in(PARAMS.slot_duration, self._disable)
        self.new_slice()


    def install_matchings(self, matchings):
        assert not self.enabled, "@%.3f" % R.time
        for src, dst in matchings:
            self.dests[src.id] = dst

    def recv(self, tor, packet):
        if not self.enabled:
            assert False,\
                    "@%.3f%s: Dropping packets from tor %s" % (R.time, self, tor)

        # Get destination
        dst = self.dests[tor.id]

        # Some checking for rotors
        intended_dst, port_id, slot_t, lumps = packet
        if len(lumps) == 0:
            return

        assert intended_dst == dst.id, \
                "%.3f %s %d:%d->(%d) actual %d. Tor slot %d Rot slot %d\n%s" % (
                    R.time,
                    self,
                    tor.id, port_id,
                    intended_dst, dst.id,
                    slot_t, PARAMS.slot_t,
                    self.matchings_by_slot[self.slot_t][tor.id][1].id)
        for _, _, n in lumps:
            self.n_packets[tor.id] += n

        dst.rx_rotor(lumps)

    def __str__(self):
        return "Switch %s (rotor)" % (self.id)

