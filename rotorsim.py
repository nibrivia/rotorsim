import random
import math
from switches import *

N_TOR   = 5
N_ROTOR = 2
N_MATCHINGS = N_TOR - 1 #don't link back to yourself
N_SLOTS = math.ceil(N_MATCHINGS / N_ROTOR)

VERBOSE = False

#random.seed(375620)

def generate_matchings(tors):
    all_matchings = []
    n_tors = len(tors)

    for offset in range(1, n_tors):
        # Compute the indices
        slot_matching = [(src_i, (src_i+offset) % n_tors)
                for src_i in range(n_tors)]

        # Add to the list of matchings
        all_matchings.append(slot_matching)

    return all_matchings

def generate_demand(min_demand = 0, max_demand = 1):
    assert 0 <= min_demand
    assert min_demand <= max_demand
    # Demand is scaled: 1 how much can be sent in 1 matching slot
    # Intra-rack traffic doesn't go over RotorNet
    return [[random.uniform(min_demand, max_demand) if dst != src else 0 for dst in range(N_TOR)] for src in range(N_TOR)]

def add_demand(old, new):
    for row, _ in enumerate(old):
        for col, _ in enumerate(old):
            old[row][col] += new[row][col]
    return old


def print_demand(tors, prefix = ""):
    print("    Demand")
    for src_i, src in enumerate(tors):
        line_str = "      " + str(src) + " -> "
        for dst_i, dst in enumerate(tors):
            line_str += "%.2f " % src.outgoing[dst_i]
        print(line_str)
    print()

def print_buffer(buffer, prefix = ""):
    for tor_n, buffer2d in enumerate(buffer):
        print("%sBuffer %d" % (prefix, tor_n+1))
        print_demand(buffer2d, prefix = prefix + "  ")

def available(dst, buffer, demand):
    a = demand
    for src, dst_buffers in enumerate(buffer):
        a -= dst_buffers[dst]

    return a

LINK_CAPACITY = 100


def main():
    print("Starting simulation with %d ToR switches and %d rotor switches" %
            (N_TOR, N_ROTOR))
    print("There are %d matchings, with %d slots per cycle" %
            (N_MATCHINGS, N_SLOTS))

    # ToR switches
    tors = [ToRSwitch(name = "%s" % (i+1), n_tor = N_TOR) for i in range(N_TOR)]

    # Rotor switches
    rotors = [RotorSwitch(tors, name = "%s/%s" % (i+1, N_ROTOR)) for i in range(N_ROTOR)]

    # Matchings
    matchings_by_slot = generate_matchings(tors)

    # Initial demand
    active_links = N_TOR*N_ROTOR
    total_links = N_TOR*(N_TOR-1)
    frac = active_links/total_links

    demand = generate_demand(max_demand = frac*1.5)
    run_demand = sum(sum(d) for d in demand)
    run_delivered = 0

    double_hop = True
    verbose = True

    print()
    N_CYCLES = 10
    for cycle in range(N_CYCLES):
        if verbose:
            print()
            print("Cycle %d/%d" % (cycle+1, N_CYCLES))

        # Send data
        for slot in range(N_SLOTS):
            if verbose:
                print("  Slot %d/%d" % (slot+1, N_SLOTS))

            # Generate new demand
            for src_i, src in enumerate(tors):
                for dst_i, dst in enumerate(tors):
                    if dst_i == src_i:
                        continue
                    src.outgoing[dst_i].add(random.uniform(0, frac*1.5))

            if verbose:
                print_demand(tors)

            # Initialize each rotor
            for r_n, rotor in enumerate(rotors):
                # Rotor n gets matchings that are n modulo N_SLOTS
                matching_i = (slot + r_n*N_SLOTS) % N_MATCHINGS
                rotor_matchings = matchings_by_slot[matching_i]

                rotor.init_slot(rotor_matchings)

            # Old indirect traffic
            if double_hop:
                if verbose:
                    print("    1. Old Indirect")
                for rotor in rotors:
                    rotor.send_old_indirect()

                if verbose:
                    print_demand(tors)

            # Direct traffic
            if verbose:
                print("    2. Direct")
            for rotor in rotors:
                rotor.send_direct()

            if verbose:
                print_demand(tors)

            # New indirect traffic
            if double_hop:
                if verbose:
                    print("    3. New Indirect")
                for rotor in rotors:
                    rotor.send_new_indirect()

                if verbose:
                    print_demand(tors)


    print("End of simulation with %d ToR switches and %d rotor switches" % (N_TOR, N_ROTOR))
    print("There are %d matchings, with %d slots per cycle" % (N_MATCHINGS, N_SLOTS))
    print("%d/%d=%.2f links are active at any time" % (active_links, total_links, frac))
    average_delivered = 100*run_delivered/N_CYCLES/N_SLOTS
    print("Average slot delivered is %d/%d utilization %d%%" %
            (average_delivered, active_links, average_delivered/active_links ))

if __name__ == "__main__":
    main()
