#import numpy as np
import math
import heapq
from itertools import product
import collections

from workloads.websearch import websearch_distribution, websearch_size
from workloads.chen import chen_distribution, chen_size
from workloads.uniform import log_uniform_distribution, log_uniform_size

# Ugly but it works
BYTES_PER_PACKET = 1500
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
from event import R


# HELPERS ========================================================



class SizeDistribution:
    def __init__(self, cdf):
        self.cdf = [(0,0)] + cdf
        self.pdf = [(p-self.cdf[i-1][0], size) for i, (p, size) in enumerate(self.cdf) if i >= 1]
        self.probs, self.sizes = zip(*self.pdf)
        self.size_B = sum(p*size for p, size in self.pdf) # in bits

    def get_flows(self, n=1):
        return random.choices(self.sizes, k = n, weights = self.probs)

    def gen_sizes(self):
        while True:
            yield self.get_flows(1)[0]

def dist_from_file(filename):
    with open(filename) as f:
        reader = csv.reader(f)
        next(reader) # skip header
        cdf    = [(float(prob), int(size)) for size, prob in reader]
    return SizeDistribution(cdf)

def weights_to_cdf(weights):
    w_sum = sum(w for w, s in weights)
    c = 0
    cdf = []
    for w, s in weights:
        c += w
        cdf.append((c / w_sum, s))
    return cdf


# in bytes
flat_log = [
        (1, 1e2),
        (1, 1e3),
        (1, 1e4),
        (1, 1e5),
        (1, 1e6),
        (1, 1e7),
        (1, 1e8),
        (1, 1e9),
        ]
flat_log_cdf = weights_to_cdf(flat_log)
simple_weights = [
        ( 4.9, 10e3),
        (95.0,  1e6),
        ( 0.1,  1e9)]
simple_cdf = weights_to_cdf(simple_weights)

xpand_cdf = [(1, 10e3)]
rotor_cdf = [(1, 1e6)]
cache_cdf = [(1, 1e9)]
WORKLOAD_FNS = defaultdict(
        #websearch   = FlowDistribution(websearch_cdf),
        datamining  = dist_from_file("workloads/datamining.csv"),
        chen        = SizeDistribution(simple_cdf),
        olivia      = SizeDistribution(flat_log_cdf),
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

def time_arrive_at_start(n_flows):
    for i in range(n_flows):
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
        n_active_tor = round(n_tor * load)
    else:
        link_load = load
        n_active_tor = n_tor

    n_active_links = n_active_tor * n_switches
    effective_load = n_active_links * link_load

    # SIZE
    workload = WORKLOAD_FNS[workload_name]
    size_dist = workload.gen_sizes()


    # PAIRS
    pair_dist = pair_uniform(n_active_tor * PARAMS.servers_per_rack)

    # TIME
    # bits / Mbits/s / load * 1000 = 1000*s / load = ms
    full_capacity = n_links*bandwidth*1e6*time_limit/1e3 # b
    n_flows = effective_load*bandwidth*1e6*time_limit/1e3/(workload.size_B*8)
    if arrive_at_start:
        time_dist = time_arrive_at_start(round(n_flows))
    else:
        #full_capacity = n_links*bandwidth*1e6*time_limit/1e3 # Gb
        #n_flows = effective_load*full_capacity/(workload.size_B*8)
        iflow_wait = time_limit/n_flows
        time_dist = time_uniform(iflow_wait)


    print("%d links @%dGb/s for %dms -> %.3fGb full capacity" % (n_links, bandwidth/1e3, time_limit, full_capacity/1e9))
    #print("%.3fGb at %.d%% load -> %dGb generated traffic" % (full_capacity/1e9, 100*link_load, effective_load))
    #print("%dGb at %.1fMb/flow -> %d flows" % (effective_load*bandwidth/1e3*time_limit, workload.size_B*8/1e6, n_flows))
    print("%d flows of %dMb -> %.3fGb traffic" % (n_flows, workload.size_B*8/1e6, n_flows*workload.size_B*8/1e9))

    # Actual generator loop
    for flow_id, (t, (src, dst), size_B) in enumerate(zip(time_dist, pair_dist, size_dist)):
        #print(t, src, dst, size)
        flow =  TCPFlow(flow_id = flow_id,
                arrival = t,
                size_bits = size_B*8,
                src = src, dst = dst)
        #print(flow, flow.size_bits, flow.size_packets)
        yield flow

def next_or_None(gen):
    try:
        return next(gen)
    except StopIteration:
        return None

def gen_constant(val):
    while True:
        yield val

def ml_size_generator(model_name):
    model_sizes = dict(
            resnet =  180e6,
            vgg    = 1080e6,
            gpt2   = 5300e6,
            )
    return gen_constant(model_sizes[model_name])

def start_job(network, servers, size_dist):
    pairs = [p for p in zip(servers[-1:]+servers[:-1], servers)]
    n_waiting = 0
    iter_done = -1

    def flow_done(flow_id):
        nonlocal n_waiting
        n_waiting -= 1
        vprint("ML flow %s done %s waiting" % (flow_id, n_waiting))
        if n_waiting == 0:
            ml_flow_gen()

    def ml_flow_gen():
        nonlocal n_waiting, iter_done
        iter_done += 1
        vprint("Iteration #%s!" % (iter_done), pairs)
        start_t = R.time

        for src, dst in pairs:
            vprint("New iteration %s->%s" % (src, dst))
            size = next(size_dist)
            flow = TCPFlow(flow_id = 1,
                    arrival = start_t,
                    size_bits = size, # TODO use size distribution
                    src = src, dst = dst)
            flow.add_callback_done(flow_done)
            network.open_connection(flow, use_gen = False)

        n_waiting = len(pairs)

    return ml_flow_gen()


def ml_generator(network, n_jobs, servers_per_ring, model_name):
    # establish rings
    servers = [i for i in range(PARAMS.n_tor * PARAMS.servers_per_rack)]
    size_dist = ml_size_generator(model_name)
    for job_id in range(n_jobs):
        ring = random.sample(servers, servers_per_ring)
        start_job(network, ring, size_dist)


def flow_combiner(gen_a, gen_b):
    """Combines two flow generators in one"""
    flow_a = next_or_None(gen_a)
    flow_b = next_or_None(gen_b)

    while True:
        if flow_a is None and flow_b is None:
            print("done")
            return

        to_yield = None
        if flow_b is None or flow_a.arrival <= flow_b.arrival:
            to_yield = flow_a
            flow_a = next_or_None(gen_a)
        elif flow_a is None or flow_b.arrival <= flow_a.arrival:
            to_yield = flow_b
            flow_b = next_or_None(gen_b)

        print("yield", to_yield, flow_a, flow_b)
        yield to_yield
        print("yielded", to_yield, flow_a, flow_b)

