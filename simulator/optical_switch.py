from switch import Switch
from params import PARAMS

class OpticalSwitch(Switch):
    def __init__(self, id):
        """Optical switch"""
        super().__init__(id)

        # Link status
        self.available_up = [True for _ in range(PARAMS.n_tor)]
        self.available_dn = [True for _ in range(PARAMS.n_tor)]

    def start(self):
        self._enable()

    def add_matchings(self):
        assert not self.enabled
        for src, dst in matchings:
            self.request_matching(srd, dst.id)

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

    """
    def recv(self, tor, packet):
        if not self.enabled:
            assert False,\
                    "@%.3f%s: Dropping packets from tor %s" % (R.time, self, tor)

        # Get destination
        dst = self.dests[tor.id]

        # Print
        if PARAMS.verbose:
            p = packet
            print("@%.3f (%2d)    %d  ->%d %s\033[00m"
                    % (R.time, self.id, tor.id, dst.id, p))
            assert p.intended_dest == dst.id

        # Send non-rotor packet
        self.n_packets[tor.id] += 1
        self.dests[tor.id].recv(packet)
        """

    def __str__(self):
        return "Switch %s (optical)" % (self.id)

