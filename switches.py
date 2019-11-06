import random
import math
import sys
import collections
from logger import *

N_TOR   = 5
N_ROTOR = 2
N_MATCHINGS = N_TOR - 1 #don't link back to yourself
N_SLOTS = math.ceil(N_MATCHINGS / N_ROTOR)

VERBOSE = True

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

    def send(self, to, num_packets):
        #assert len(self.packets) >= num_packets

        moving_packets = [self.packets.popleft() for _ in range(num_packets)]
        to.recv(moving_packets)

        log(self, to, moving_packets)

        if VERBOSE and num_packets > 0:
            print("        \033[01m%s -> %s: %2d\033[00m"
                    % (self, to, num_packets))

    def recv(self, packets):
        self.packets.extend(packets)

    def add(self, val):
        new_packets = [self.count+i for i in range(val)]
        self.count += val
        self.packets.extend(new_packets)

        log("demand", self, new_packets)


    @property
    def size(self):
        return len(self.packets)

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
        #raise Error
        # Initially full capacity
        available = PACKETS_PER_SLOT

        # Remove old indirect traffic
        available -= sum(b.size for b in self.indirect[dst])

        # Remove direct traffic
        available -= self.outgoing[dst].size

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
            if s > PACKETS_PER_SLOT:
                print(str(self) + str(s))
                raise E

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
        # Go through matchings randomly, would be better if fair
        for link in shuffle(self.matchings):
            src_i, ind_i = link
            src,   ind   = self.tors[src_i], self.tors[ind_i]
            print("         %s->%s    %d" % (src_i+1, ind_i+1, self.remaining[link]))

            # If we still have demand, indirect it somewhere
            for dst_i, src_buffer in shuffle(enumerate(src.outgoing)):
                available = src.available_to(dst_i)
                print("            %s->%d %d" % (ind_i+1, dst_i+1, available))
                available = min(self.remaining[link], available)
                amount = bound(0, src_buffer.size, available)

                src.outgoing[dst_i].send(ind.indirect[dst_i][src_i], amount)
                self.remaining[link] -= amount

    def __str__(self):
        return "Rotor %s" % self.name
