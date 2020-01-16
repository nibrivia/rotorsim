import numpy as np
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

Flow = collections.namedtuple('Flow', 'arrival id size src dst remaining')

class FlowDistribution:
    def __init__(self, cdf):
        self.cdf = [(0,0)] + cdf
        self.pdf = [(p-self.cdf[i-1][0], size) for i, (p, size) in enumerate(self.cdf) if i >= 1]
        self.probs, self.sizes = zip(*self.pdf)
        self.size = sum(p*size for p, size in self.pdf) # in bits

    def get_flows(self, n=1):
        return np.random.choice(self.sizes, size = n, p = self.probs)

websearch_cdf = [(1,1)]
simple_cdf = [(0.049, 10e3), (0.999, 1e6), (1, 1e9)]
WORKLOAD_FNS = defaultdict(
        websearch   = FlowDistribution(websearch_cdf),
        chen        = FlowDistribution(simple_cdf),
        #log_uniform = FlowDistribution(log_uniform_distribution, log_uniform_size),
        default   = lambda _: Exception('Unrecognized workload {}'.format(workload)))


# MAIN ===========================================================

def generate_flows(
    load, bandwidth,
    time_limit,
    num_tors,
    num_rotors,
    workload_name,
    results_file='flows.csv',
):
    # csv header
    fields = [
        'arrival',
        'id',
        'size_bytes',
        'src',
        'dst',
    ]

    # get workload generator
    workload = WORKLOAD_FNS[workload_name]

    # construct tor pairs
    tors = set(range(num_tors))
    tor_pairs = np.array(list(set(product(tors, tors)) - { (i, i) for i in tors }))

    # Find interflow arrival rate
    # bits / Mbits/s / load * 1000 = 1000*s / load = ms
    capacity  = num_tors*num_rotors*bandwidth*1e6*time_limit/1e3 # Gb
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
    flows = zip(arrivals, [i for i in range(n_flows)], sizes, *zip(*pairs), sizes)
    flows = [Flow(*f) for f in flows]
    for f in flows:
        pass
        #print(f)


    # write flows out to csv in increasing arrival order
    with open(results_file, 'w') as csv_file:
        # write csv header
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(fields)
        # write flows
        csv_writer.writerows(flows)

    return flows
