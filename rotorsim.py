import random
import math
from network import RotorNet
from logger import Log
from helpers import *



def generate_demand(min_demand = 0, max_demand = 1):
    assert 0 <= min_demand
    assert min_demand <= max_demand
    # Demand is scaled: 1 how much can be sent in 1 matching slot
    # Intra-rack traffic doesn't go over RotorNet
    return [[random.uniform(min_demand, max_demand) if dst != src else 0
        for dst in range(N_TOR)] for src in range(N_TOR)]

def generate_static_demand(matching, max_demand = 1):
    return [[1 if matching[src] == dst else 0
        for dst in range(N_TOR)] for src in range(N_TOR)]


def main():
    #print("%d ToRs, %d rotors, %d packets/slot" %
    #        (N_TOR, N_ROTOR, PACKETS_PER_SLOT))
    #print("  => %d matchings, %d slots/cycle" %
    #        (N_MATCHINGS, N_SLOTS))

    # Initial demand
    #active_links = N_TOR*N_ROTOR
    #total_links = N_TOR*(N_TOR-1)
    #frac = active_links/total_links

    logger = Log()
    net = RotorNet(n_rotor = 2, n_tor = 5, logger = logger)
    n_cycles = 5

    #demand = generate_static_demand(matchings_by_slot[-1], max_demand = frac)
    #run_demand = sum(sum(d) for d in demand)


    print("---")
    for cycle in range(n_cycles):

        # Send data
        for slot in range(3):
            net.do_slot()

    #close_log()

    print("done")

if __name__ == "__main__":

    main()
