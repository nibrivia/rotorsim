import collections 
from logger import Log

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

    def send_to(self, to, num_packets, rotor_id):
        if num_packets > 0 and self.verbose:
            print("        \033[01m%s to %s\033[00m, [%s->%s] via %s: %2d pkts\033[00m"
                    % (self.parent, to, self.src, self.dst, rotor_id, num_packets))

        assert len(self.packets) >= num_packets, "Sending more packets than inqueue %s" % self

        moving_packets = [self.packets.popleft() for _ in range(num_packets)]
        to.recv(moving_packets)

        self.size -= num_packets

        if not self.logger is None:
            self.logger.log(
                    src = self.src, dst = to.src, flow = self.flow,
                    rotor_id = rotor_id,
                    packets = moving_packets)

    def recv(self, packets):
        for p in packets:
            assert p.dst == self.dst
        self.packets.extend(packets)
        self.size = len(self.packets)

class SourceBuffer:
    def __init__(self, parent, src, dst, logger, verbose):
        self.parent = parent
        self.src, self.dst = (src, dst)
        self.count = 0
        self.sent  = 0
        self.size  = 0

        self.logger = logger
        self.verbose = verbose

    def recv(self, packets, flow):
        raise Error

    def add_n(self, amount):
        new_packets = [Packet(self.src, self.dst, self.count+i) for i in range(amount)]

        self.count += amount
        self.size  += amount

        if not self.logger is None:
            self.logger.log(
                    src = DEMAND_NODE.src, dst = self.src, flow = self.flow,
                    rotor_id = -1,
                    packets = new_packets)

    def send_to(self, to, amount, rotor_id):
        assert amount <= self.size
        packets = [Packet(self.src, self.dst, self.count-self.size+i) for i in range(amount)]

        if amount > 0 and self.verbose:
            print("        \033[01m%s to %s\033[00m, [%s->%s] via %s: %2d pkts\033[00m"
                    % (self.parent, to, self.src, self.dst, rotor_id, amount))

        to.recv(packets)
        self.size -= amount

        if not self.logger is None:
            self.logger.log(
                    src = self.src, dst = to.src, flow = self.flow,
                    rotor_id = rotor_id,
                    packets = packets)


class DestBuffer:
    def __init__(self, src, dst, logger, verbose):
        self.src, self.dst = (src, dst)
        self.size = 0

    def recv(self, packets):
        self.size += len(packets)

    def add_n(self, amount):
        raise Error

    def send_to(self, to, amount, rotor_id):
        raise Error



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

