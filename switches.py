import random
import math
import sys
import collections
from logger import *

N_TOR   = 17
N_ROTOR = 4
N_MATCHINGS = N_TOR - 1 #don't link back to yourself
N_SLOTS = math.ceil(N_MATCHINGS / N_ROTOR)
N_CYCLES = 5

VERBOSE = False

LOG = Log()

DO_LOG = False

# TODO use logger process
def log(src, dst, packets):
    if not DO_LOG:
        return

    t = T.T
    if isinstance(src, str):
        for p in packets:
            LOG.log("%.3f, %s, 0, %s, %s, %d\n" %
                    (t, src, dst.owner, dst.q_name, p))
            t += 1/(PACKETS_PER_SLOT*N_SLOTS)
    else:
        for p in packets:
            LOG.log("%.3f, %s, %s, %s, %s, %d\n" %
                    (t, src.owner, src.q_name, dst.owner, dst.q_name, p))
            t += 1/(PACKETS_PER_SLOT*2)

def close_log():
    LOG.close_log()

class Time:
    def __init__(self):
        self.T = 0

    def add(self, inc):
        self.T += inc

    def __str__(self):
        return str(self.T)
    def __int__(self):
        return int(self.T)
    def __float__(self):
        return float(self.T)

global T
T = Time()

PACKETS_PER_SLOT = 100
class Buffer():
    def __init__(self, name = ""):
        self.packets = collections.deque()
        self.name = str(name)
        self.owner, self.q_name = str(name).split(".")
        self.count = 0

        # Cached, this is used a lot
        self.size = 0

    def send(self, to, num_packets):
        assert len(self.packets) >= num_packets, "Sending more packets than in queue %s" % self

        moving_packets = [self.packets.popleft() for _ in range(num_packets)]
        to.recv(moving_packets)

        self.size = len(self.packets)
        log(self, to, moving_packets)

        if VERBOSE and num_packets > 0:
            print("        \033[01m%s -> %s: %2d\033[00m"
                    % (self, to, num_packets))

    def recv(self, packets):
        self.packets.extend(packets)
        self.size = len(self.packets)

    def add(self, val):
        new_packets = [self.count+i for i in range(val)]
        self.count += val
        self.packets.extend(new_packets)
        self.size = len(self.packets)

        log("demand", self, new_packets)


    def __str__(self):
        return "%s" % self.name

class EndPoint:
    def __init__(self):
        self.to_send = []
        self.received = []

class ToRSwitch:
    def __init__(self, name = "", n_tor = 0):
        # Index by who to send to
        self.outgoing =  [Buffer(name = "%s.dst%s" %(name, dst+1))
                for dst in range(n_tor)]
        # Index by who send to me
        self.incoming =  [Buffer(name = "%s.src%s" % (name, src+1))
                for src in range(n_tor)]

        # self.ind[dst][src]
        self.indirect = [[ Buffer(name = "%s.(src%s->dst%s)"
            % (name, src+1, dst+1))
            for src in range(n_tor)] for dst in range(n_tor)]

        self.name = name

    def available_to(self, dst):
        # Initially full capacity, w/out direct traffic
        available = PACKETS_PER_SLOT - self.outgoing[dst].size

        # Remove old indirect traffic
        available -= sum(b.size for b in self.indirect[dst])

        return max(available, 0)

    def __str__(self):
        return "ToR %s" % self.name


def bound(lo, val, hi):
    return max(lo, min(val, hi))

def shuffle(generator):
    return sorted(generator, key = lambda k: random.random())
    #return generator


class RotorSwitch:
    def __init__(self, tors, name = ""):
        self.name = name
        self.tors = tors

    def init_slot(self, matchings):
        # Reset link availabilities
        self.remaining = {link: PACKETS_PER_SLOT for link in matchings}
        self.matchings = matchings

    def send_old_indirect(self):
        if VERBOSE:
            print("      %s" % self)
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
        if VERBOSE:
            print("      %s" % self)

        for link in self.matchings:
            src_i, dst_i = link
            src,   dst   = self.tors[src_i], self.tors[dst_i]

            # How much to send?
            amount = bound(0, src.outgoing[dst_i].size, self.remaining[link])

            # Actually send
            src.outgoing[dst_i].send(dst.incoming[src_i], amount)
            self.remaining[link] -= amount


    def send_new_indirect(self):
        # TODO bug where things that are about to be indirected count
        # against other indirect traffic
        if VERBOSE:
            print("      %s" % self)

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
