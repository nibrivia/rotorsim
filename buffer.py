import collections 
from logger import Log

LOG = Log()
DO_LOG = False
VERBOSE = False

class Buffer():
    def __init__(self, name = ""):
        self.packets = collections.deque()
        self.name = str(name)
        self.owner, self.q_name = str(name).split(".")
        self.count = 0

        # Cached, this is used a lot
        self.size = 0

    def send(self, to, num_packets):
        assert len(self.packets) >= num_packets, "Sending more packets than in queue %s" % self

        moving_packets = [self.packets.popleft() for _ in range(num_packets)]
        to.recv(moving_packets)

        self.size = len(self.packets)
        log(self, to, moving_packets)

        if VERBOSE and num_packets > 0:
            print("        \033[01m%s -> %s: %2d\033[00m"
                    % (self, to, num_packets))

    def recv(self, packets):
        self.packets.extend(packets)
        self.size = len(self.packets)

    def add(self, val):
        new_packets = [self.count+i for i in range(val)]
        self.count += val
        self.packets.extend(new_packets)
        self.size = len(self.packets)

        log("demand", self, new_packets)


    def __str__(self):
        return "%s" % self.name

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
