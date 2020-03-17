from switch import Switch
from params import PARAMS
from event import R

class OpticalSwitch(Switch):
    def __init__(self, id):
        """Optical switch"""
        super().__init__(id)

        # Link status
        self.available_up = [True for _ in range(PARAMS.n_tor)]
        self.available_dn = [True for _ in range(PARAMS.n_tor)]

        self.starts = [None for _ in range(PARAMS.n_tor)]

    def start(self):
        self._enable()

    def add_matchings(self, matchings):
        assert not self.enabled
        for src, dst in matchings:
            self.request_matching(src, dst.id)

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


    def __str__(self):
        return "Switch %s (optical)" % (self.id)

