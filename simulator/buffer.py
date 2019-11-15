import collections 
from logger import Log


class Buffer():
    def __init__(self, name, logger, verbose):
        self.packets = collections.deque()
        self.name = str(name)
        self.src, self.flow = str(name).split(".")
        self.count = 0
        self.packet_num = 0

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

        self.size = len(self.packets)
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
        self.size = len(self.packets)

        self.logger.log(
                src = DEMAND_NODE.src, dst = self.src, flow = self.flow,
                rotor_id = -1,
                packets = new_packets)


    def __str__(self):
        return "%s" % self.name

DEMAND_NODE = Buffer("demand.0", None, verbose = False)