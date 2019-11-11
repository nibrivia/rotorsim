import collections 
from logger import Log

LOG = Log()
DO_LOG = False
VERBOSE = False



class Buffer():
    def __init__(self, name, logger, verbose):
        self.packets = collections.deque()
        self.name = str(name)
        self.owner, self.q_name = str(name).split(".")
        self.count = 0
        self.packet_num = 0
        self.logger = logger
        self.verbose = verbose

        # Cached, this is used a lot
        self.size = 0

    def vprint(self, s = ""):
        if self.verbose:
            print(s)


    def send(self, to, num_packets):
        assert len(self.packets) >= num_packets, "Sending more packets than inqueue %s" % self

        moving_packets = [self.packets.popleft() for _ in range(num_packets)]
        to.recv(moving_packets)

        self.size = len(self.packets)
        self.logger.log(t = 0,
                src = self, dst = to,
                packets = moving_packets)

        if num_packets > 0:
            self.vprint("        \033[01m%s -> %s: %2d\033[00m"
                    % (self, to, num_packets))

    def recv(self, packets):
        self.packets.extend(packets)
        self.size = len(self.packets)

    def add_n(self, val):
        new_packets = [self.count+i for i in range(val)]
        self.count += val
        self.packets.extend(new_packets)
        self.size = len(self.packets)

        self.logger.log(t = 0,
                src = DEMAND_NODE, dst = self,
                packets = new_packets)


    def __str__(self):
        return "%s" % self.name

DEMAND_NODE = Buffer("demand.0", None, verbose = False)
