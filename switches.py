import random
import sys

VERBOSE = True

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

def init_log():
    with open("data/out.csv", "w") as f:
        print("time, src, src_queue, dst, dst_queue, packet",
                file = f)

# TODO use logger
def log(src, dst, packets):
    with open("data/out.csv", "a") as f:
        if isinstance(src, str):
            for p in packets:
                print("%.3f, %s, %s, %s, %s, %d" %
                        (T, src, "0", dst.owner, dst.q_name, p),
                    file = f)
        else:
            for p in packets:
                print("%.3f, %s, %s, %s, %s, %d" %
                        (T, src.owner, src.q_name, dst.owner, dst.q_name, p),
                    file = f)


PACKETS_PER_SLOT = 10
class Buffer():
    def __init__(self, name = ""):
        self.packets = []
        self.name = str(name)
        self.owner, self.q_name = str(name).split(".")
        self.count = 0

    def send(self, to, num_packets):
        assert len(self.packets) >= num_packets
        if VERBOSE and num_packets > 0:
            print("        \033[01m%s -> %s: %2d\033[00m"
                    % (self, to, num_packets))
            log(self, to, self.packets[0:num_packets])

        to.recv(self.packets[0:num_packets])
        self.packets = self.packets[num_packets:]

    def recv(self, packets):
        self.packets.extend(packets)
        if len(packets) > 0 and False:
            print("%s: received [%s]" %
                    (self.name, ", ".join([str(p) for p in packets])))
            print(self.packets)

    def add(self, val):
        self.packets.extend([self.count+i for i in range(val)])
        log("demand", self, self.packets[-val:])
        #self.packets.extend([self.name + "-" + str(self.count + i)
        #    for i in range(val)])
        self.count += val
        #self.amount += val

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

        # self.ind[orig][dest]
        self.indirect = [[ Buffer(name = "%s.(src%s->dst%s)"
            % (name, src+1, dst+1))
            for dst in range(n_tor)] for src in range(n_tor)]

        self.name = name

    def available(self, dst):
        raise Error
        # Initially full capacity
        available = PACKETS_PER_SLOT

        # Remove old indirect traffic
        for src_buffer_ind in self.buffer_ind:
            available -= src_buffer_ind[dst]

        # Remove direct traffic
        available -= self.incoming[dst]

        return available

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
        print("      %s" % self)
        # For each matching, look through our buffer, deliver old stuff
        for link in self.matchings:
            ind_i, dst_i = link
            ind,   dst   = self.tors[ind_i], self.tors[dst_i]

            # For this link, find all old indirect traffic who wants to go
            for dta_src, ind_buffer in enumerate(ind.indirect):
                # How much can we send?
                amount = bound(0, ind_buffer[dst_i].size, self.remaining[link])

                # Actually send
                ind_buffer[dst_i].send(dst.incoming[dta_src], amount)
                self.remaining[link] -= amount

    def send_direct(self):
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
        print("      %s" % self)
        # Go through matchings randomly, would be better if fair
        for link in shuffle(self.matchings):
            src_i, ind_i = link
            src,   ind   = self.tors[src_i], self.tors[ind_i]

            # If we still have demand, indirect it somewhere
            for dst_i, src_buffer in shuffle(enumerate(src.outgoing)):
                available = min(self.remaining[link], PACKETS_PER_SLOT)
                amount = bound(0, src_buffer.size, available)

                src.outgoing[dst_i].send(ind.indirect[src_i][dst_i], amount)
                self.remaining[link] -= amount

    def __str__(self):
        return "Rotor %s" % self.name
