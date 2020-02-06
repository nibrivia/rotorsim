import numpy as np
import math
from itertools import product
import collections
import numpy as np

from workloads.websearch import websearch_distribution, websearch_size
from workloads.chen import chen_distribution, chen_size
from workloads.uniform import log_uniform_distribution, log_uniform_size

BYTES_PER_PACKET = 10000
global FLOWS, N_FLOWS, N_DONE
FLOWS = dict()
N_FLOWS = [0]
N_DONE  = [0]

import csv
from collections import defaultdict
from helpers import *
from flow import *
from logger import LOG


# HELPERS ========================================================



class FlowDistribution:
    def __init__(self, cdf):
        self.cdf = [(0,0)] + cdf
        self.pdf = [(p-self.cdf[i-1][0], size) for i, (p, size) in enumerate(self.cdf) if i >= 1]
        self.probs, self.sizes = zip(*self.pdf)
        self.size = sum(p*size for p, size in self.pdf) # in bits

    def get_flows(self, n=1):
        return np.random.choice(self.sizes, size = n, p = self.probs)

def dist_from_file(filename):
    with open(filename) as f:
        reader = csv.reader(f)
        next(reader) # skip header
        cdf    = [(float(prob), int(size)*8) for size, prob in reader]
    return FlowDistribution(cdf)

def weights_to_cdf(weights):
    w_sum = sum(w for w, s in simple_weights)
    c = 0
    cdf = []
    for w, s in weights:
        c += w
        cdf.append((c / w_sum, s))
    return cdf


simple_weights = [
        ( 4.9, 10e3),
        (95.0,  1e6),
        ( 0.1,  1e9)]
simple_cdf = weights_to_cdf(simple_weights)

xpand_cdf = [(1, 10e3)]
rotor_cdf = [(1, 10e6)]
cache_cdf = [(1, 1e9)]
WORKLOAD_FNS = defaultdict(
        #websearch   = FlowDistribution(websearch_cdf),
        datamining  = dist_from_file("workloads/datamining.csv"),
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
    arrive_at_start = False,
    results_file='flows.csv',
    skewed = False,
):

    # get workload generator
    workload = WORKLOAD_FNS[workload_name]
    if workload_name == "datamining" and num_tors == 108:
        num_switches = 6

    # construct tor pairs
    n_active = num_tors
    if skewed:
        n_active = round(num_tors * load)
        load     = 1

    tors = set(range(n_active))
    tor_pairs = np.array(list(set(product(tors, tors)) - { (i, i) for i in tors }))

    # Find interflow arrival rate
    # bits / Mbits/s / load * 1000 = 1000*s / load = ms
    n_pairs  = len(tor_pairs)
    n_links  = n_active*num_switches
    capacity = n_links*bandwidth*1e6*time_limit/1e3 # Gb
    n_flows  = int(capacity/workload.size*load)
    iflow_wait = time_limit/n_flows

    # arrivals
    #n_flows = flow_arrivals_per_ms*time_limit
    offered_load = workload.size*n_flows
    print("load      %d flows x %.3fMb/flow = %dGb" %
            (n_flows, workload.size/1e6, offered_load/1e9))
    print("net load %.3fGb / %.3fGb = %.2f%%" % (offered_load/1e9, capacity/1e9, 100*offered_load/capacity))
    print("iflow all %.3fus" % (iflow_wait*1000))
    # np.poisson returns int, so in ns, then convert back to ms

    if arrive_at_start and workload_name == "chen":
        flows_per_pair = n_flows/n_pairs
        rotor_per_pair = flows_per_pair * workload.probs[0]
        cache_per_pair = flows_per_pair * workload.probs[1]

        print(n_flows, flows_per_pair, rotor_per_pair, cache_per_pair)
        assert cache_per_pair + rotor_per_pair == flows_per_pair


        flow_id = 0
        for src in range(num_tors):
            for dst in range(num_tors):
                if dst == src:
                    continue

                rotor_per_pair = np.random.binomial(n=flows_per_pair, p = workload.probs[0])
                if rotor_per_pair > 0:
                    f = Flow(0, flow_id, rotor_per_pair*workload.sizes[0], src, dst)
                    f.tag = "rotor"
                    FLOWS[flow_id] = f
                    yield (0, f)
                    flow_id += 1

                cache_per_pair = np.random.binomial(n=flows_per_pair, p = workload.probs[1])
                if cache_per_pair > 0:
                    f = Flow(0, flow_id, cache_per_pair*workload.sizes[1], src, dst)
                    f.tag = "cache"
                    FLOWS[flow_id] = f
                    yield (0, f)
                    flow_id += 1
        return

    else:
        flow_id = -1
        for _ in range(n_flows):
            # Stop backlogging if we're doing skewed -> less memory
            if skewed and len(FLOWS) > n_links * 10: # Each link has 10 flows waiting on average
                yield (iflow_wait, None)

            flow_id += 1
            if arrive_at_start:
                wait = 0
            else:
                wait = np.random.poisson(lam=iflow_wait*1e6, size=1)[0]/1e6

            # pairs
            #print("pairs")
            pair_id = np.random.choice(len(tor_pairs), size = 1)[0]
            pair = tor_pairs[pair_id]
            src, dst = pair
            # sizes
            #print("sizes")
            size = workload.get_flows(n = 1)[0]

            # start, id, size, src, dst
            #print("Flow gen...", end = "")
            flow = Flow(R.time + wait, flow_id, size, pair[0], pair[1])

            # write flows out to csv in increasing arrival order

            FLOWS[flow_id] = flow
            N_FLOWS[0] += 1
            yield (wait, flow)
