import numpy as np
import math
from itertools import product
import collections
import numpy as np

from workloads.websearch import websearch_distribution, websearch_size
from workloads.chen import chen_distribution, chen_size
from workloads.uniform import log_uniform_distribution, log_uniform_size

from collections import defaultdict
from helpers import *
from logger import LOG


# HELPERS ========================================================

BYTES_PER_PACKET = 1500

class Packet:
    def __init__(self, src_id, dst_id, seq_num, tag, flow_id, is_last):
        self.src_id  = src_id
        self.dst_id  = dst_id
        self.seq_num = seq_num
        self.tag     = tag
        self.flow_id = flow_id
        self.is_last = is_last

        self.intended_dest = None

    def __str__(self):
        return "%3d[%s->%s]#%d >%s" % (
                self.flow_id, self.src_id, self.dst_id, self.seq_num, self.intended_dest)

class Flow:
    def __init__(self, arrival, flow_id, size, src, dst):
        self.arrival = arrival
        self.id      = flow_id
        self.size    = size
        self.src     = src
        self.dst     = dst

        self.remaining_packets = math.ceil(size/(BYTES_PER_PACKET*8))
        self.size_packets      = self.remaining_packets
        self.n_sent = 0
        self.n_recv = 0


        if size < 1e6:
            self.tag = "xpand"
        elif size < 1e9:
            self.tag = "rotor"
        else:
            self.tag = "cache"

    def pop_lump(self, n=1):
        assert self.remaining_packets >= n, \
                "Flow %d does not have %d packets to send" % (self.id, n)

        self.remaining_packets -= n
        self.n_sent += n

        return (self.id, self.dst, n)

    def pop(self, n = 1):
        assert self.remaining_packets >= n, \
                "Flow %d does not have %d packets to send" % (self.id, n)

        p = Packet(self.src, self.dst, self.n_sent,
                self.tag, self.id, self.remaining_packets == 1)

        self.remaining_packets -= 1
        self.n_sent += 1

        return p

    def rx(self, n=1, t = None):
        self.n_recv += n
        assert self.n_recv <= self.n_sent, "%s recv/sent/size %d/%d/%d" % (self, self.n_recv, self.n_sent, self.size_packets)
        assert self.n_recv <= self.size_packets

        if self.n_recv == self.size_packets:
            if t is None:
                self.end = R.time
            else:
                self.end = t

            if self.tag == "rotor" and False:
                print(self, "done")

            LOG.log_flow_done(self)
            global FLOWS, N_DONE
            N_DONE[0] += 1
            del FLOWS[self.id]

            if len(FLOWS) == 0:
                R.stop()

    def send(self, n_packets):
        n_packets = min(n_packets, self.remaining_packets)

    def __str__(self):
        return "%s %4d[%3d->%3d]\033[00m" % (self.tag, self.id, self.src, self.dst)

class FlowDistribution:
    def __init__(self, cdf):
        self.cdf = [(0,0)] + cdf
        self.pdf = [(p-self.cdf[i-1][0], size) for i, (p, size) in enumerate(self.cdf) if i >= 1]
        self.probs, self.sizes = zip(*self.pdf)
        self.size = sum(p*size for p, size in self.pdf) # in bits

    def get_flows(self, n=1):
        return np.random.choice(self.sizes, size = n, p = self.probs)


def weights_to_cdf(weights):
    w_sum = sum(w for w, s in simple_weights)
    c = 0
    cdf = []
    for w, s in weights:
        c += w
        cdf.append((c / w_sum, s))
    return cdf


websearch_cdf = [(1,1)]
simple_weights = [
        #( 4.9, 10e3),
        (95.0,  1e6),
        ( 0.1,  1e9)]
simple_cdf = weights_to_cdf(simple_weights)

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
global FLOWS, N_FLOWS, N_DONE
FLOWS = dict()
N_FLOWS = [0]
N_DONE  = [0]

def generate_flows(
    load, bandwidth,
    time_limit,
    num_tors,
    num_switches,
    workload_name,
    arrive_at_start = False,
    results_file='flows.csv',
):

    # get workload generator
    workload = WORKLOAD_FNS[workload_name]

    # construct tor pairs
    tors = set(range(num_tors))
    tor_pairs = np.array(list(set(product(tors, tors)) - { (i, i) for i in tors }))

    # Find interflow arrival rate
    # bits / Mbits/s / load * 1000 = 1000*s / load = ms
    n_pairs = (num_tors*(num_tors-1))
    n_links = num_tors*num_switches
    capacity  = n_links*bandwidth*1e6*time_limit/1e3 # Gb
    n_flows_o = int(capacity/workload.size*load)
    n_flows   = round(n_flows_o/n_pairs)*n_pairs
    print("diff: %d" % (n_flows_o-n_flows))
    iflow_wait = time_limit/n_flows

    # arrivals
    #n_flows = flow_arrivals_per_ms*time_limit
    offered_load = workload.size*n_flows
    print("load      %d flows x %.3fMb/flow = %dGb" %
            (n_flows, workload.size/1e6, offered_load/1e9))
    print("net load %.3fGb / %.3fGb = %.2f%%" % (offered_load/1e9, capacity/1e9, 100*offered_load/capacity))
    print("iflow all %.3fus" % (iflow_wait*1000))
    # np.poisson returns int, so in ns, then convert back to ms

    if arrive_at_start:
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
        while True:
            flow_id += 1
            wait = np.random.poisson(lam=iflow_wait*1e6, size=1)[0]/1e6

            # pairs
            #print("pairs")
            pair_id = np.random.choice(len(tor_pairs), size = 1)[0]
            pair = tor_pairs[pair_id]

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
