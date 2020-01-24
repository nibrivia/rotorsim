import collections 

from logger import Log
from event import R
from packet import Packet

class Buffer():
    def __init__(self, parent = None, name = "",
            src = None, dst = None,
            logger = None, verbose = False):
        self.packets = collections.deque()
        self.parent  = parent
        self.name    = name
        self.src, self.dst = (src, dst)
        self.count = 0

        self.logger = logger
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


DEMAND_NODE = Buffer(None, None, None, None, verbose = False)

if __name__ == "__main__":
    l = Log()
    sn = Buffer("1.1->2", None, True)
    hn = Buffer("3.1->2", None, True)
    rn = Buffer("2.1->2", None, True)

    sn.add_n(3)
    print((sn.packets, hn.packets, rn.packets))

    sn.send_to(hn, 2, 1)
    print((sn.packets, hn.packets, rn.packets))
    hn.send_to(rn, 1, 1)
    print((sn.packets, hn.packets, rn.packets))

    sn.send_to(hn, 1, 1)
    print((sn.packets, hn.packets, rn.packets))
    hn.send_to(rn, 1, 1)
    print((sn.packets, hn.packets, rn.packets))

    sn.add_n(3)
    print((sn.packets, hn.packets, rn.packets))
    sn.send_to(hn, 3, 1)
    print((sn.packets, hn.packets, rn.packets))
    hn.send_to(rn, 3, 1)
    print((sn.packets, hn.packets, rn.packets))

