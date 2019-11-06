import random
import math
from switches import *


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
    return [[random.uniform(min_demand, max_demand) if dst != src else 0
        for dst in range(N_TOR)] for src in range(N_TOR)]

def generate_static_demand(matching, max_demand = 1):
    return [[1 if matching[src] == dst else 0
        for dst in range(N_TOR)] for src in range(N_TOR)]



def print_demand(tors, prefix = "", print_buffer = False):
    print()
    print("\033[0;32m      Demand")
    print("        Direct")
    print_buffer = True
    for src_i, src in enumerate(tors):
        line_str = "          " + str(src) + " -> "
        for dst_i, dst in enumerate(tors):
            line_str += "%2d " % src.outgoing[dst_i].size
        print(line_str)

    if print_buffer:
        print("        Indirect")
        for ind_i, ind in enumerate(tors):
            line_str = "          ToR " + str(ind_i+1) + "\n"
            for dst_i, dst in enumerate(tors):
                tot = 0
                line_str += "            " 
                for src_i, src in enumerate(tors):
                    qty = ind.indirect[dst_i][src_i].size
                    tot += qty
                    line_str += "%2d " % qty
                line_str += "-> %d  =%2d\n" % (dst_i+1, tot)
            print(line_str)
    print("\033[00m")


def main():
    print("%d ToRs, %d rotors, %d packets/slot" %
            (N_TOR, N_ROTOR, PACKETS_PER_SLOT))
    print("  => %d matchings, %d slots/cycle" %
            (N_MATCHINGS, N_SLOTS))

    # ToR switches
    tors = [ToRSwitch(name = "%s" % (i+1), n_tor = N_TOR)
            for i in range(N_TOR)]

    # Rotor switches
    rotors = [RotorSwitch(tors, name = "%s/%s" % (i+1, N_ROTOR))
            for i in range(N_ROTOR)]

    # Matchings
    matchings_by_slot = generate_matchings(tors)

    # Initial demand
    active_links = N_TOR*N_ROTOR
    total_links = N_TOR*(N_TOR-1)
    frac = active_links/total_links

    demand = generate_static_demand(matchings_by_slot[-1], max_demand = frac)
    run_demand = sum(sum(d) for d in demand)
    run_delivered = 0

    double_hop = True
    verbose = VERBOSE

    print("---")
    for cycle in range(N_CYCLES):
        if verbose:
            print()
        print("\033[01;31mCycle %d/%d\033[00m" % (cycle+1, N_CYCLES))

        # Send data
        for slot in range(N_SLOTS):
            global T
            T.add(1/N_SLOTS)
            if verbose:
                print("  \033[0;31mSlot %d/%d\033[00m" % (slot+1, N_SLOTS))
                print("  %f" % T)

            # Generate new demand
            for src_i, src in enumerate(tors):
                for dst_i, dst in enumerate(tors):
                    if dst_i == src_i:
                        continue
                    src.outgoing[dst_i].add(
                            round(random.uniform(
                                0,
                                frac*0.7*PACKETS_PER_SLOT)))
            #for src, dst in matchings_by_slot[0]:
            #    tors[src].outgoing[dst].add(N_ROTOR*PACKETS_PER_SLOT)

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
                for rotor in shuffle(rotors):
                    rotor.send_old_indirect()

                if verbose:
                    print_demand(tors)

            # Direct traffic
            if verbose:
                print("    2. Direct")
            for rotor in shuffle(rotors):
                rotor.send_direct()

            if verbose:
                print_demand(tors)

            # New indirect traffic
            if double_hop:
                if verbose:
                    print("    3. New Indirect")
                for rotor in shuffle(rotors):
                    rotor.send_new_indirect()

                if verbose:
                    print_demand(tors)

    close_log()

    print("done")

if __name__ == "__main__":
    main()
