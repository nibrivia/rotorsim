
class EndPoint:
    def __init__(self):
        self.to_send = []
        self.received = []

class ToRSwitch:
    def __init__(self, name = "", n_tor = 0):
        # Index by who to send to
        self.buffer_dir    =  [[0] for dst in range(n_tor)]
        # Index by who to send to
        self.received_from =  [[0] for dst in range(n_tor)]

        # self.ind[orig][dest]
        self.buffer_ind    = [[ [0] for d in range(n_tor)] for s in range(n_tor)]

        self.name = name

    def available(self, dst):
        # Initially full capacity
        available = 1

        # Remove old indirect traffic
        for src_buffer_ind in self.buffer_ind:
            available -= src_buffer_ind[dst]

        # Remove direct traffic
        available -= self.buffer_dir[dst]

        return available

    def __str__(self):
        return "ToR %s" % self.name


def send(src, dst, amount):
    """
    Will actually modify src and dst, using array for pointer properties...
    """
    src[0] -= amount
    dst[0] += amount

class RotorSwitch:
    def __init__(self, name = ""):
        self.name = name

    def init_slot(self, matchings):
        # Reset link availabilities
        self.remaining = {(src, dst): 1 for src, dst in matchings}
        self.matchings = matchings

    def send_old_indirect(self):
        # For each matching, look through our buffer, deliver what we have stored
        for ind, dst in enumerate(matchings):
            for dta_src, ind_src_buffer in enumerate(ind.buffer_ind):
                # Try to send what we have
                to_send = src.buffer_ind[dta_src][dst]

                if to_send > 0:
                    data_sent = min(max(0, to_send), remaining[src][dst])
                    if data_sent > 0 and verbose:
                        print("        (%2d->)%-2d->%-2d: sending %3d" %
                                (dta_src+1, src+1, dst+1, round(100*data_sent)))
                    new_buffer[src][dta_src][dst] -= data_sent
                    remaining[src][dst] -= data_sent
                    #run_delivered += data_sent
                    #slot_sent_ind += data_sent

    def __str__(self):
        return "Rotor %s" % self.name
