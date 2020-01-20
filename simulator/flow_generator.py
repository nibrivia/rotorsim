import numpy as np
import math
import csv
from itertools import product
import collections
import numpy as np

from workloads.websearch import websearch_distribution, websearch_size
from workloads.chen import chen_distribution, chen_size
from workloads.uniform import log_uniform_distribution, log_uniform_size

from collections import defaultdict
from helpers import *


# HELPERS ========================================================

BYTES_PER_PACKET = 1500

Packet = collections.namedtuple('Packet', 'src_id dst_id seq_num tag flow_id is_last')

class Flow:
    def __init__(self, arrival, flow_id, size, src, dst):
        self.arrival = arrival
        self.id      = flow_id
        self.size    = size
        self.src     = src
        self.dst     = dst

        self.started = False

        self.remaining_packets = math.ceil(size/(BYTES_PER_PACKET*8))
        self.n_sent = 0

        if size < 1e6:
            self.tag = "xpand"
        elif size < 1e9:
            self.tag = "rotor"
        else:
            self.tag = "cache"

    def pop(self):
        assert self.remaining_packets > 0, "Flow %d has no more packets to send" % self.id

        if False and not self.started:
            if self.tag == "xpand":
                print("\033[0;31m", end = "")
            if self.tag == "rotor":
                print("\033[0;32m", end = "")
            if self.tag == "cache":
                print("\033[0;33m", end = "")
            print("flow %d start (%s)\033[00m" % (self.id, self.tag))
            self.start = R.time
            self.started = True

        p = Packet(self.src, self.dst, self.n_sent,
                self.tag, self.id, self.remaining_packets == 1)

        self.remaining_packets -= 1
        self.n_sent += 1

        return p

    def send(self, n_packets):
        n_packets = min(n_packets, self.remaining_packets)

    def __str__(self):
        return "%s %3d[%s->%s]\033[00m" % (self.tag, self.id, self.src, self.dst)

class FlowDistribution:
    def __init__(self, cdf):
        self.cdf = [(0,0)] + cdf
        self.pdf = [(p-self.cdf[i-1][0], size) for i, (p, size) in enumerate(self.cdf) if i >= 1]
        self.probs, self.sizes = zip(*self.pdf)
        self.size = sum(p*size for p, size in self.pdf) # in bits

    def get_flows(self, n=1):
        return np.random.choice(self.sizes, size = n, p = self.probs)

websearch_cdf = [(1,1)]
simple_cdf = [(0.049, 10e3), (0.99, 1e6), (1, 1e9)]
xpand_cdf = [(1, 10e3)]
rotor_cdf = [(1, 10e6)]
cache_cdf = [(1, 1e9)]
WORKLOAD_FNS = defaultdict(
        websearch   = FlowDistribution(websearch_cdf),
        chen        = FlowDistribution(simple_cdf),
        xpand       = FlowDistribution(xpand_cdf),
        rotor       = FlowDistribution(rotor_cdf),
        cache       = FlowDistribution(cache_cdf),
        #log_uniform = FlowDistribution(log_uniform_distribution, log_uniform_size),
        default   = lambda _: Exception('Unrecognized workload {}'.format(workload)))


# MAIN ===========================================================

def generate_flows(
    load, bandwidth,
    time_limit,
    num_tors,
    num_switches,
    workload_name,
    results_file='flows.csv',
):
    # csv header
    fields = [
        'id',
        'arrival',
        'size_bytes'
    ]

    # get workload generator
    workload = WORKLOAD_FNS[workload_name]

    # construct tor pairs
    tors = set(range(num_tors))
    tor_pairs = np.array(list(set(product(tors, tors)) - { (i, i) for i in tors }))

    # Find interflow arrival rate
    # bits / Mbits/s / load * 1000 = 1000*s / load = ms
    capacity  = num_tors*num_switches*bandwidth*1e6*time_limit/1e3 # Gb
    n_flows   = int(capacity/workload.size*load)
    iflow_wait = time_limit/n_flows

    # arrivals
    #n_flows = flow_arrivals_per_ms*time_limit
    offered_load = workload.size*n_flows
    print("load      %d flows x %.3fMb/flow = %dGb" %
            (n_flows, workload.size/1e6, offered_load/1e9))
    print("net load %.3fGb / %.3fGb = %.2f%%" % (offered_load/1e9, capacity/1e9, 100*offered_load/capacity))
    print("iflow all %.3fus" % (iflow_wait*1000))
    # np.poisson returns int, so in ns, then convert back to ms
    waits = np.random.poisson(lam=iflow_wait*1e6, size=n_flows)

    # Convert waits into times
    t = 0
    arrivals = [0 for _ in waits]
    for i, w in enumerate(waits):
        t += w
        arrivals[i] = t/1e6

    # pairs
    pairs_idx = np.random.choice(len(tor_pairs), size = n_flows)
    pairs = tor_pairs[pairs_idx]

    # sizes
    sizes = workload.get_flows(n = n_flows)

    # start, id, size, src, dst
    flows = zip(arrivals, [i for i in range(n_flows)], sizes, *zip(*pairs))
    flows = [Flow(*f) for f in flows]

    # write flows out to csv in increasing arrival order
    with open(results_file, 'w') as csv_file:
        # write csv header
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(fields)
        # write flows
        csv_writer.writerows((f.id, f.arrival, f.size) for f in flows)

    return flows
