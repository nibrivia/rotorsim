import random

VERBOSE = True

class Buffer():
    def __init__(self, name = ""):
        self.amount = 0
        self.name = name

    def send(self, to, amount):
        assert self.amount >= amount
        to.amount += amount
        self.amount -= amount
        if VERBOSE and amount > 0:
            print("        %s -> %s: %.2f" % (self, to, amount))

    def add(self, val):
        self.amount += val

    def __float__(self):
        return float(self.amount)

    def __str__(self):
        return "%s" % self.name

class EndPoint:
    def __init__(self):
        self.to_send = []
        self.received = []

class ToRSwitch:
    def __init__(self, name = "", n_tor = 0):
        # Index by who to send to
        self.outgoing =  [Buffer(name = "%s.t%s" %(name, dst+1))
                for dst in range(n_tor)]
        # Index by who to send to
        self.incoming =  [Buffer(name = "%s.r%s" % (name, src+1))
                for src in range(n_tor)]

        # self.ind[orig][dest]
        self.indirect = [[ Buffer(name = "%s.(s%s->d%s)" % (name, src+1, dst+1))
            for dst in range(n_tor)] for src in range(n_tor)]

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
    src.send(dst, amount)
    if VERBOSE:
        print("%s -> %s: %.2f" % (src, dst, amount))

def bound(lo, val, hi):
    return max(float(lo), min(float(val), float(hi)))

class RotorSwitch:
    def __init__(self, tors, name = ""):
        self.name = name
        self.tors = tors

    def init_slot(self, matchings):
        # Reset link availabilities
        self.remaining = {link: 1 for link in matchings}
        self.matchings = matchings

    def send_old_indirect(self):
        print("      %s" % self)
        # For each matching, look through our buffer, deliver old stuff
        for link in self.matchings:
            ind_i, dst_i = link
            ind,   dst   = self.tors[ind_i], self.tors[dst_i]

            # For this link, find all old indirect traffic who wants to go
            for dta_src, ind_buffer in enumerate(ind.indirect):
                # How much can we send?
                amount = bound(0, ind_buffer[dst_i], self.remaining[link])

                # Actually send
                ind_buffer[dst_i].send(dst.incoming[dta_src], amount)
                self.remaining[link] -= amount

    def send_direct(self):
        print("      %s" % self)

        for link in self.matchings:
            src_i, dst_i = link
            src,   dst   = self.tors[src_i], self.tors[dst_i]

            # How much to send?
            amount = bound(0, src.outgoing[dst_i], self.remaining[link])

            # Actually send
            src.outgoing[dst_i].send(dst.incoming[src_i], amount)
            self.remaining[link] -= amount


    def send_new_indirect(self):
        print("      %s" % self)
        # Go through matchings randomly, would be better if fair
        for link in sorted(self.matchings, key = lambda k: random.random()):
            src_i, ind_i = link
            src,   ind   = self.tors[src_i], self.tors[ind_i]

            # If we still have demand, indirect it somewhere
            for dst_i, src_buffer in enumerate(src.outgoing):
                available = min(self.remaining[link], 1)
                amount = bound(0, src_buffer, available)
                #max(min(demand[src][dst],
                                    #remaining[src][ind],
                                    #available(dst, all_tor_buffers[ind], demand[ind][dst])),
                        #0)

                src.outgoing[dst_i].send(ind.indirect[src_i][dst_i], amount)
                self.remaining[link] -= amount

    def __str__(self):
        return "Rotor %s" % self.name
