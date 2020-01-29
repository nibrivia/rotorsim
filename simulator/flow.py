import math
from logger import LOG
from flow_generator import BYTES_PER_PACKET, N_DONE, N_FLOWS, FLOWS
from event import R

class Packet:
    def __init__(self, src_id, dst_id, seq_num, tag, flow_id, is_last):
        self.src_id  = src_id
        self.dst_id  = dst_id
        self.seq_num = seq_num
        self.tag     = tag
        self.flow_id = flow_id
        self.is_last = is_last

        self.intended_dest = None

    def __str__(self):
        return "%3d[%s->%s]#%d >%s" % (
                self.flow_id, self.src_id, self.dst_id, self.seq_num, self.intended_dest)

class Flow:
    def __init__(self, arrival, flow_id, size, src, dst):
        self.arrival = arrival
        self.id      = flow_id
        self.size    = size
        self.src     = src
        self.dst     = dst

        self.remaining_packets = math.ceil(size/(BYTES_PER_PACKET*8))
        self.size_packets      = self.remaining_packets
        self.n_sent = 0
        self.n_recv = 0

        self.end = float("nan")

        if size < 1e6:
            self.tag = "xpand"
        elif size < 1e9:
            self.tag = "rotor"
        else:
            self.tag = "cache"

    def pop_lump(self, n=1):
        assert self.remaining_packets >= n, \
                "Flow %d does not have %d packets to send" % (self.id, n)

        self.remaining_packets -= n
        self.n_sent += n

        return (self.id, self.dst, n)

    def pop(self, n = 1):
        assert self.remaining_packets >= n, \
                "Flow %d does not have %d packets to send" % (self.id, n)

        p = Packet(self.src, self.dst, self.n_sent,
                self.tag, self.id, self.remaining_packets == 1)

        self.remaining_packets -= 1
        self.n_sent += 1

        return p

    def rx(self, n=1, t = None):
        self.n_recv += n
        assert self.n_recv <= self.n_sent, "%s recv/sent/size %d/%d/%d" % (self, self.n_recv, self.n_sent, self.size_packets)
        assert self.n_recv <= self.size_packets

        if self.n_recv == self.size_packets:
            if t is None:
                self.end = R.time
            else:
                self.end = t

            if self.tag == "rotor" and False:
                print(self, "done")

            LOG.log_flow_done(self)
            global FLOWS, N_DONE
            N_DONE[0] += 1
            del FLOWS[self.id]

            if len(FLOWS) == 0:
                R.stop()

    def send(self, n_packets):
        n_packets = min(n_packets, self.remaining_packets)

    def __str__(self):
        return "%s %4d[%3d->%3d]\033[00m" % (self.tag, self.id, self.src, self.dst)