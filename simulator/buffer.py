import collections 
from logger import Log


class Buffer():
    def __init__(self, name, logger, verbose):
        self.packets = collections.deque()
        self.src, self.flow = str(name).split(".")
        self.count = 0

        self.logger = logger
        self.verbose = verbose

        # Cached, this is used a lot
        self.size = 0

    def vprint(self, s = ""):
        if self.verbose:
            print(s)


    def send_to(self, to, num_packets, rotor_id):
        if num_packets > 0:
            self.vprint("        \033[01m%s to %s\033[00m, [%s] via %s: %2d pkts\033[00m"
                    % (self.src, to.src, self.flow, rotor_id, num_packets))

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
        self.packets.extend(packets)
        self.size = len(self.packets)

    def add_n(self, val):
        new_packets = [self.count+i for i in range(val)]
        self.count += val
        self.packets.extend(new_packets)
        self.size += val

        if not self.logger is None:
            self.logger.log(
                    src = DEMAND_NODE.src, dst = self.src, flow = self.flow,
                    rotor_id = -1,
                    packets = new_packets)

class SourceBuffer(Buffer):
    def __init__(self, *args, **kwargs):
        super(SourceBuffer, self).__init__(*args, **kwargs)

    def recv(self, packets):
        raise Error

    def add_n(self, amount):
        new_packets = [self.count+i for i in range(amount)]

        self.count += amount
        self.size  += amount

        if not self.logger is None:
            self.logger.log(
                    src = DEMAND_NODE.src, dst = self.src, flow = self.flow,
                    rotor_id = -1,
                    packets = new_packets)

    def send_to(self, to, amount, rotor_id):
        assert amount <= self.size
        self.packets.extend([self.count-self.size+i for i in range(amount)])
        super(SourceBuffer, self).send_to(to, amount, rotor_id)


class DestBuffer(Buffer):
    def __init__(self, *args, **kwargs):
        super(DestBuffer, self).__init__(*args, **kwargs)

    def recv(self, packets):
        self.size += len(packets)

    def add_n(self, amount):
        raise Error

    def send_to(self, to, amount, rotor_id):
        raise Error



DEMAND_NODE = Buffer("demand.0", None, verbose = False)

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

