#import numpy as np
import math
from itertools import product
import collections

from workloads.websearch import websearch_distribution, websearch_size
from workloads.chen import chen_distribution, chen_size
from workloads.uniform import log_uniform_distribution, log_uniform_size

BYTES_PER_PACKET = 10000
global FLOWS, N_FLOWS, N_DONE
FLOWS = dict()
ML_JOBS = []
ML_QUEUE = []
N_FLOWS = [0]
N_DONE  = [0]

import random
import csv
from collections import defaultdict
from helpers import *
from flow import Flow, TCPFlow
from logger import LOG
from params import PARAMS


# HELPERS ========================================================



class SizeDistribution:
    def __init__(self, cdf):
        self.cdf = [(0,0)] + cdf
        self.pdf = [(p-self.cdf[i-1][0], size) for i, (p, size) in enumerate(self.cdf) if i >= 1]
        self.probs, self.sizes = zip(*self.pdf)
        self.size = sum(p*size for p, size in self.pdf) # in bits

    def get_flows(self, n=1):
        return random.choices(self.sizes, k = n, weights = self.probs)

    def gen_sizes(self):
        while True:
            yield self.get_flows(1)[0]

def dist_from_file(filename):
    with open(filename) as f:
        reader = csv.reader(f)
        next(reader) # skip header
        cdf    = [(float(prob), int(size)*8) for size, prob in reader]
    return SizeDistribution(cdf)

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
        chen        = SizeDistribution(simple_cdf),
        xpand       = SizeDistribution(xpand_cdf),
        rotor       = SizeDistribution(rotor_cdf),
        cache       = SizeDistribution(cache_cdf),
        #log_uniform = FlowDistribution(log_uniform_distribution, log_uniform_size),
        default   = lambda _: Exception('Unrecognized workload {}'.format(workload)))


# MAIN ===========================================================

# TIME --------------------------------
def time_uniform(iflow_wait):
    t = 0
    while True:
        t += random.expovariate(lambd=1/iflow_wait)
        yield t

def time_arrive_at_start():
    while True:
        yield 0

# PAIR --------------------------------
def pair_uniform(n_servers):
    all_pairs = [(src, dst)
            for src in range(n_servers)
            for dst in range(n_servers) if src != dst]
    while True:
        yield random.choice(all_pairs)


def generate_flows(
    load,
    bandwidth,
    time_limit,
    n_tor,
    n_switches,
    workload_name,
    arrive_at_start,
    skewed,
    #make_flow
    #is_ml,
    ):

    # How many active links are there?
    # How much should active links be loaded?
    n_servers = n_tor * PARAMS.servers_per_rack
    n_pairs = n_tor * (n_tor - 1)
    n_links = n_tor * n_switches

    if skewed:
        # Load servers are active
        link_load = 1
        n_active = round(n_tor * load)
    else:
        link_load = load
        n_active = n_tor

    n_active_links = n_active * n_switches
    effective_load = n_active_links * link_load

    # SIZE
    workload = WORKLOAD_FNS[workload_name]
    size_dist = workload.gen_sizes()

    # PAIRS
    pair_dist = pair_uniform(n_active)

    # TIME
    # bits / Mbits/s / load * 1000 = 1000*s / load = ms
    full_capacity = n_links*bandwidth*1e6*time_limit/1e3 # Gb
    n_flows = effective_load*full_capacity/workload.size
    if arrive_at_start:
        time_dist = time_arrive_at_start
    else:
        full_capacity = n_links*bandwidth*1e6*time_limit/1e3 # Gb
        n_flows = effective_load*full_capacity/workload.size
        iflow_wait = time_limit/n_flows
        time_dist = time_uniform(iflow_wait)



    # Actual generator loop
    for flow_id, (t, (src, dst), size) in enumerate(zip(time_dist, pair_dist, size_dist)):
        #print(t, src, dst, size)
        yield TCPFlow(flow_id = flow_id,
                arrival = t,
                size_bits = size,
                src = src, dst = dst)


def _generate_flows(
    load, bandwidth,
    time_limit,
    num_tors,
    num_switches,
    workload_name,
    arrive_at_start,
    skewed,
    is_ml
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
    tor_pairs = list(set(product(tors, tors)) - { (i, i) for i in tors })

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
        xpand_per_pair = flows_per_pair * workload.probs[0]
        rotor_per_pair = flows_per_pair * workload.probs[1]
        cache_per_pair = flows_per_pair * workload.probs[2]

        print(n_flows, flows_per_pair, rotor_per_pair+ cache_per_pair+xpand_per_pair)


        flow_id = 0
        for src in range(num_tors):
            for dst in range(num_tors):
                if dst == src:
                    continue

                assert False, "Binomial doesn't exist here"
                #xpand_per_pair = np.random.binomial(n=flows_per_pair, p = workload.probs[0])
                if xpand_per_pair > 0:
                    f = Flow(0, flow_id, xpand_per_pair*workload.sizes[0], src, dst)
                    f.tag = "xpand"
                    FLOWS[flow_id] = f
                    yield (0, f)
                    flow_id += 1

                rotor_per_pair = np.random.binomial(n=flows_per_pair, p = workload.probs[1])
                if rotor_per_pair > 0:
                    f = Flow(0, flow_id, rotor_per_pair*workload.sizes[1], src, dst)
                    f.tag = "rotor"
                    FLOWS[flow_id] = f
                    yield (0, f)
                    flow_id += 1

                cache_per_pair = np.random.binomial(n=flows_per_pair, p = workload.probs[2])
                if cache_per_pair > 0:
                    f = Flow(0, flow_id, cache_per_pair*workload.sizes[2], src, dst)
                    f.tag = "cache"
                    FLOWS[flow_id] = f
                    yield (0, f)
                    flow_id += 1
        return
    else:
        flow_id = -1
        if is_ml and workload_name == "datamining":
            resnet_med =  180e6
            vgg_med    = 1080e6
            gpt2_med   = 5300e6
            sizes = [resnet_med, vgg_med, gpt2_med]

            if load == .2:
                n_job = [1, 2, 4]
            elif load == .7:
                n_job = [3, 6, 12]
            else:
                assert False, "please no"
            job_desc = zip(sizes, n_job)

            global ML_JOBS, ML_QUEUE
            for size, n in job_desc:
                for _ in range(n):
                    nodes = random.sample(tors, 4)
                    pairs = [p for p in zip(nodes[-1:]+nodes[:-1], nodes)]

                    job = (size, pairs, 0)
                    ML_JOBS.append(job)
                    ML_QUEUE.append(len(ML_QUEUE))
                    print(job)
        for _ in range(n_flows*2): # Cap #flows at twice what it should be
            if is_ml:
                for i in ML_QUEUE:
                    size, pairs, n_flows = ML_JOBS[i]
                    if n_flows == 0:
                        for src, dst in pairs:
                            flow_id += 1
                            f = Flow(R.time, flow_id, size, src, dst, ml_id = i)
                            FLOWS[flow_id] = f
                            N_FLOWS[0] += 1
                            yield (0, f)
                        print("more flows %d %d" % (i, size))
                        ML_JOBS[i] = (size, pairs, 4)
                for _ in range(len(ML_QUEUE)):
                    ML_QUEUE.pop()

            # Stop backlogging if we're doing skewed -> less memory
            if skewed and len(FLOWS) > n_links * 10: # Each link has 10 flows waiting on average
                yield (iflow_wait, None)
                continue

            flow_id += 1
            if arrive_at_start:
                wait = 0
            else:
                wait = random.expovariate(lambd=1/iflow_wait)

            # pairs
            #print("pairs")
            pair = random.choice(tor_pairs)
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
