
import random
import csv
from itertools import product
import numpy as np

from workloads.websearch import websearch_distribution, websearch_size
from workloads.chen import chen_distribution, chen_size

from collections import defaultdict


# HELPERS ========================================================

WORKLOAD_FNS = defaultdict(
        websearch = (websearch_distribution, websearch_size),
        chen      = (chen_distribution, chen_size),
        default   = lambda _: Exception('Unrecognized workload {}'.format(workload)))

# MAIN ===========================================================

def generate_flows(
    load, bandwidth,
    time_limit,
    num_tors,
    num_rotors,
    workload,
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
    generate_workload, size = WORKLOAD_FNS[workload]

    # construct tor pairs
    tors = set(range(num_tors))
    tor_pairs = list(set(product(tors, tors)) - { (i, i) for i in tors })

    # model num_flows flow arrivals with a poisson process
    print("size of 1 flow: %s" % (size))
    interflow_arrival = 1000 * size / bandwidth / load # in ms, so *1000
    print(interflow_arrival)
    flow_arrivals_per_ms = num_tors*num_rotors/interflow_arrival
    print(flow_arrivals_per_ms)
    arrivals = list(np.random.poisson(flow_arrivals_per_ms, time_limit))
    print(sum(arrivals))

    # create flows 
    flows = []
    for arrival_slot, flows_arriving in enumerate(arrivals):
        # take note of flows arriving in this slot
        for _ in range(flows_arriving):
            # get flow size from specified workload
            size = generate_workload()
            # assign the current slot to this flow's arrival
            arrival = arrival_slot
            # src --> dst chosen randomly over tor pairs
            src, dst = random.choice(tor_pairs)
            # assign unique flow id to this flow
            flow_id = len(flows)

            flows.append((arrival, flow_id, size, src, dst))

    # write flows out to csv in increasing arrival order
    with open(results_file, 'w') as csv_file:
        # write csv header
        csv_writer = csv.writer(csv_file) 
        csv_writer.writerow(fields)
        # write flows 
        csv_writer.writerows(flows)
