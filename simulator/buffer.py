import collections 
from logger import Log
from event import R

Packet = collections.namedtuple('Packet', 'src dst seq_num')
p = Packet(0, 0, 0)
print(p)

class Buffer():
    def __init__(self, parent, src, dst, logger, verbose):
        self.packets = collections.deque()
        self.parent  = parent
        self.src, self.dst = (src, dst)
        self.count = 0

        self.logger = logger
        self.verbose = verbose

        # Cached, this is used a lot
        self.size = 0

    def send_to(self, to, num_packets):
        if num_packets > 0 and self.verbose:
            print("@%.2f        \033[01m%s\033[00m to %s   [%s->%s]: %2d pkts\033[00m"
                    % (R.time, self.parent, to, self.src, self.dst, num_packets))

        assert len(self.packets) >= num_packets, "Sending more packets than inqueue %s" % self

        moving_packets = [self.packets.popleft() for _ in range(num_packets)]
        self.size -= num_packets

        to.recv(moving_packets)

        if not self.logger is None:
            self.logger.log(
                    src = self.parent, dst = to,
                    packets = moving_packets)

    def recv(self, packets):
        #for p in packets:
        #    assert p.dst == self.dst
        self.packets.extend(packets)
        self.size = len(self.packets)

    def add_n(self, amount, src = None, dst = None):
        if src is None:
            src = self.src
        if dst is None:
            dst = self.dst

        new_packets = [Packet(src, dst, self.count+i) for i in range(amount)]
        self.count += amount
        self.recv(new_packets)


DEMAND_NODE = Buffer(None, None, None, None, verbose = False)

if __name__ == "__main__":
    l = Log()
    sn = SourceBuffer("1.1->2", None, True)
    hn = Buffer("3.1->2", None, True)
    rn = DestBuffer("2.1->2", None, True)

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

