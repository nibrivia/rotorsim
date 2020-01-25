import collections 

from logger import LOG
from event import R
from packet import Packet

class Buffer():
    def __init__(self, parent = None, name = "",
            src = None, dst = None,
            verbose = False):
        self.packets = collections.deque()
        self.parent  = parent
        self.name    = name
        self.src, self.dst = (src, dst)
        self.count = 0

        self.verbose = verbose

        # Cached, this is used a lot
        self.size = 0

    def pop(self):
        p = self.packets.popleft()
        self.size -= 1
        return p


    def recv(self, packet):
        #for p in packets:
        #    assert p.dst == self.dst
        self.packets.append(packet)
        self.size += 1

    def recv_many(self, packets):
        self.packets.extend(packets)
        self.size += len(packets)

    def empty(self):
        self.size = 0
        ps = self.packets
        self.packets = []
        return ps

    def add_n(self, amount, src = None, dst = None):
        if src is None:
            src = self.src
        if dst is None:
            dst = self.dst

        new_packets = [Packet(src, dst, self.count+i) for i in range(amount)]
        self.count += amount
        self.recv(new_packets)

    def __str__(self):
        return "%s.%s;%d" % (str(self.parent), self.name, self.size)

