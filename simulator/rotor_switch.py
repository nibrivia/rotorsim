from helpers import *
from logger import LOG
from event import R, Delay
from switch import Switch


class RotorSwitch(Switch):
    def __init__(self,
            id,
            tag,
            ):
        """"""
        # About me
        self.id   = id
        self.dests = [None for _ in range(PARAMS.n_tor)]
        self.tag   = tag
        # dests[1] is the destination of a packet arriving in on port 1

        # for cache
        self.available_up = [True for _ in range(PARAMS.n_tor)]
        self.available_dn = [True for _ in range(PARAMS.n_tor)]

        # About time
        self.slice_t   = -1
        self.n_packets = [0 for _ in range(PARAMS.n_tor)]
        self.starts    = [0 for _ in range(PARAMS.n_tor)]

        # About IO
        self._disable()

    def add_matchings(self, matchings_by_slot, n_rotor):
        self.matchings_by_slot = matchings_by_slot

    def start(self):
        self._disable()
        self.install_matchings(self.matchings_by_slot[0])

        # Create a recursive call
        if self.tag == "rotor":
            self.new_slice = Delay(PARAMS.slot_duration + PARAMS.reconfiguration_time, priority = 2)(self._new_slice)
            R.call_in(PARAMS.slot_duration, priority = 1, fn = self._disable)
        else:
            self.new_slice = lambda: None
            self._enable()

        self._new_slice()

    # Returns True/False if the connection can be established
    def request_matching(self, tor, dst_id):
        assert self.available_up[tor.id], "%s %s %s %s" % (
                self, tor, dst_id, tor.active_flow[tor.debug])

        # Make sure the connection can be established
        if not self.available_dn[dst_id]:
            return False

        # True it
        self.available_up[tor.id] = False
        self.available_dn[dst_id] = False

        self.dests[ tor.id] = self.tors[dst_id]
        self.starts[tor.id] = R.time + 15#...

        return True

    def release_matching(self, tor):
        self.available_up[tor.id] = True
        dst = self.dests[tor.id]
        self.available_dn[dst.id] = True

        self.dests[tor.id] = None

        self.n_packets[tor.id] += max(0, R.time - self.starts[tor.id])

        # TODO notify
        for tor in self.tors:
            tor._send(self.id)
            # tor.cache_free(dst.id)
            pass

    @property
    def slot_t(self):
        return round(R.time / (PARAMS.slot_duration+PARAMS.reconfiguration_time))

    def _new_slice(self):
        n_slots = len(self.matchings_by_slot)
        slot_id = self.slot_t % n_slots
        vprint("%.6f %s              slot_id %d" % (R.time, self, slot_id))

        # Skip if it's not our turn
        if self.tag != "rotor": #and self.slice_t % PARAMS.n_rotor != self.id:
            self._enable()
            return

        vprint("%.6f %s switching to slot_id %d" % (R.time, self, slot_id))
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
        # Wait for reconfiguration time, high priority so that reconf 0 has no down time

    def recv(self, tor, packet):
        if not self.enabled:
            assert False,\
                    "@%.3f%s: Dropping packets from tor %s" % (R.time, self, tor)

        # Get destination
        dst = self.dests[tor.id]

        # Some checking for rotors
        if self.tag == "rotor":
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
            return

        # Print
        if PARAMS.verbose:
            p = packet
            print("@%.3f (%2d)    %d  ->%d %s\033[00m"
                    % (R.time, self.id, tor.id, dst.id, p))
            assert p.intended_dest == dst.id

        # Send non-rotor packet
        self.n_packets[tor.id] += 1
        self.dests[tor.id].recv(packet)

    def __str__(self):
        return "Switch %s (%s)" % (self.id, self.tag)

