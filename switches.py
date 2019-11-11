import sys
from buffer import *
from timetracker import *
from helpers import *


PACKETS_PER_SLOT = 4

class ToRSwitch:
    def __init__(self, name, n_tor, logger, verbose):
        # Index by who to send to
        self.id = int(name)

        """
        self.outgoing =  [Buffer(
                            name = "%s.%s->%s" %(name, name, dst+1),
                            logger = logger,
                            verbose = verbose)
                for dst in range(n_tor)]
        # Index by who send to me
        self.incoming =  [Buffer(
                            name = "%s.%s->%s" % (name, src+1, name),
                            logger = logger,
                            verbose = verbose)
                for src in range(n_tor)]

        # self.indirect[dst][src]
        self.indirect = [[ Buffer(
                            name = "%s.%s->%s" % (name, src+1, dst+1),
                            logger = logger,
                            verbose = verbose)
            for src in range(n_tor)] for dst in range(n_tor)]
        """

        self.buffers = { (src, dst) : Buffer(
            name = "%s.%s->%s" % (self.id, src, dst),
            logger = logger,
            verbose = verbose) for src in range(n_tor) for dst in range(n_tor) }

        self.disconnect_all()


    def add_demand_to(self, dst, amount):
        self.buffers[(self.id, dst)].add_n(amount)

    def send(self, to, flow, n_packets):
        assert self.link_remaining[to] >= n_packets, "Link capacity violation"

        self.buffers[flow].send_to(to.buffers[flow], n_packets)
        self.link_remaining[to] -= n_packets

    def connect_to(self, tor_id, tor):
        self.connected_tors.add((tor_id, tor))
        self.link_remaining[tor] = PACKETS_PER_SLOT

    def disconnect_all(self):
        self.connected_tors = set()
        self.link_remaining = dict()

    def send_direct(self):
        for dst_id, dst in self.connected_tors:
            flow = (self.id, dst_id)
            n_sending = bound(0, self.buffers[flow].size, PACKETS_PER_SLOT)
            self.send(dst, flow, n_sending)



    def available_to(self, dst):
        # Initially full capacity, w/out direct traffic
        available = PACKETS_PER_SLOT - self.outgoing[dst].size

        # Remove old indirect traffic
        available -= sum(b.size for b in self.indirect[dst])

        return max(0, available)

    def __str__(self):
        return "ToR %s" % self.id


"""
class RotorSwitch:
    def __init__(self, tors, name):
        self.name = name
        self.tors = tors

    def init_slot(self, matchings):
        raise Error
        # Reset link availabilities
        self.remaining = {link: PACKETS_PER_SLOT for link in matchings}
        for src, dst in matchings:
            self.tors[src-1].connect_to(dst)
        self.matchings = matchings

    def send_old_indirect(self):
        return

        vprint("      %s" % self)
        # For each matching, look through our buffer, deliver old stuff
        for link in self.matchings:
            ind_i, dst_i = link
            ind,   dst   = self.tors[ind_i], self.tors[dst_i]
            s = sum(b.size for b in ind.indirect[dst_i])
            assert s <= PACKETS_PER_SLOT, "More old indirect than can send %s" % self

            # For this link, find all old indirect traffic who wants to go
            for dta_src, ind_buffer in enumerate(ind.indirect[dst_i]):
                # How much can we send?
                amount = bound(0, ind_buffer.size, self.remaining[link])

                # Actually send
                ind_buffer.send(dst.incoming[dta_src], amount)
                self.remaining[link] -= amount

    def send_direct(self):
        raise Error
        vprint("      %s" % self)

        for link in self.matchings:
            src_i, dst_i = link
            src,   dst   = self.tors[src_i], self.tors[dst_i]

            # How much to send?
            amount = bound(0, src.outgoing[dst_i].size, self.remaining[link])

            # Actually send
            src.outgoing[dst_i].send(dst.incoming[src_i], amount)
            self.remaining[link] -= amount


    def send_new_indirect(self):
        return

        vprint("      %s" % self)

        # Because sending traffic indirectly then allows us to receive more,
        # we need to keep iterating until we can't send any more
        sent = 1
        while sent > 0:
            sent = 0
            # Go through matchings randomly, would be better if fair
            for link in shuffle(self.matchings):
                src_i, ind_i = link
                src,   ind   = self.tors[src_i], self.tors[ind_i]

                # If we still have demand, indirect it somewhere
                for dst_i, src_buffer in shuffle(enumerate(src.outgoing)):
                    available = src.available_to(dst_i)
                    available = min(self.remaining[link], available)
                    amount = bound(0, src_buffer.size, available)

                    src.outgoing[dst_i].send(ind.indirect[dst_i][src_i], amount)
                    self.remaining[link] -= amount
                    sent += amount

    def __str__(self):
        return "Rotor %s" % self.name
"""
