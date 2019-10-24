import random
import math

N_TOR   = 17
N_ROTOR = 4
N_MATCHINGS = N_TOR - 1 #don't link back to yourself
N_SLOTS = math.ceil(N_MATCHINGS / N_ROTOR)

#random.seed(375620)

def generate_matchings():
    all_matchings = []
    for offset in range(1, N_TOR):
        slot_matching = [-1 for _ in range(N_TOR)]
        for src in range(N_TOR):
            slot_matching[src] = (src+offset) % N_TOR
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


def print_demand(demand, prefix = "", verbose = True):
    if verbose:
        for src, d in enumerate(demand):
            print("%s%2d -> %s" % (prefix, src+1, " ".join(["%3d" % round(i*100) for i in d])))
    else:
        print("%s%d" % (prefix, sum(sum(100*d) for d in demand)/(N_TOR*(N_TOR-1))))

def print_buffer(buffer, prefix = ""):
    for tor_n, buffer2d in enumerate(buffer):
        print("%sBuffer %d" % (prefix, tor_n+1))
        print_demand(buffer2d, prefix = prefix + "  ")

def available(dst, buffer, demand):
    a = demand
    for src, dst_buffers in enumerate(buffer):
        a -= dst_buffers[dst]

    return a


if __name__ == "__main__":
    print("Starting simulation with %d ToR switches and %d rotor switches" % (N_TOR, N_ROTOR))
    print("There are %d matchings, with %d slots per cycle" % (N_MATCHINGS, N_SLOTS))

    matchings_by_slot = generate_matchings()

    print()
    print("matchings")
    for slot, matches in enumerate(matchings_by_slot):
        slot_str = "slot %d: " % slot
        for src, dst in enumerate(matches):
            slot_str += "%d->%d " % (src+1, dst+1)
        print(slot_str)

    # Initial demand
    active_links = N_TOR*N_ROTOR
    total_links = N_TOR*(N_TOR-1)
    frac = active_links/total_links

    demand = generate_demand(max_demand = frac*1.5)
    run_demand = sum(sum(d) for d in demand)

    double_hop = False
    verbose = False

    # Buffers
    # buffer[rotor_n][src][dst]
    all_tor_buffers = [[[0 for dst in range(N_TOR)] for src in range(N_TOR)] for _ in range(N_TOR)]

    print()
    N_CYCLES = 200
    for cycle in range(N_CYCLES):
        print("Cycle %d/%d" % (cycle+1, N_CYCLES))

        # Send data
        for slot in range(N_SLOTS):
            print("  Slot %d/%d" % (slot+1, N_SLOTS))
            print("     Demand")
            print_demand(demand, prefix = "       ", verbose=verbose)
            if verbose:
                print_buffer(all_tor_buffers, prefix = "     ")
            remaining = [[1 for _ in range(N_TOR)] for _ in range(N_TOR)]

            print()
            # Go through each Rotor switch
            for rotor_n in range(N_ROTOR):
                # Rotor n gets matchings that are n modulo N_SLOTS
                matching_i = (slot + rotor_n*N_SLOTS) % N_MATCHINGS
                rotor_matchings = matchings_by_slot[matching_i]
                if verbose:
                    print("     Rotor %d (matching %d/%d)" % (rotor_n, matching_i+1, N_MATCHINGS))

                # Received indirect traffic: first send what we have stored
                #remaining = [1 for _ in range(N_TOR)]
                for src, dst in enumerate(rotor_matchings):
                    if not double_hop:
                        break

                    for dta_src, _ in enumerate(all_tor_buffers[src]):
                        to_send = all_tor_buffers[src][dta_src][dst]
                        if to_send > 0:
                            data_sent = min(max(0, to_send), remaining[src][dst])
                            if data_sent > 0 and verbose:
                                print("        (%2d->)%-2d->%-2d: sending %3d" %
                                        (dta_src+1, src+1, dst+1, round(100*data_sent)))
                            all_tor_buffers[src][dta_src][dst] -= data_sent
                            remaining[src][dst] -= data_sent


                # Direct traffic: go through every matching and send as much as we can
                for src, dst in enumerate(rotor_matchings):
                    # Send to remote destination
                    data_sent = min(max(0, demand[src][dst]), remaining[src][dst])
                    if verbose:
                        print("        %2d->%-2d: sending %3d" % (src+1, dst+1, round(100*data_sent)))
                    demand[src][dst] -= data_sent
                    remaining[src][dst] -= data_sent


                # New indirect traffic: use the spare capacity
                for src, ind in sorted(enumerate(rotor_matchings), key = lambda k: random.random()):
                    if not double_hop:
                        break
                    # If we still have demand, indirect it somewhere
                    for dst in range(N_TOR):
                        if remaining[src][ind] > 0:
                            data_sent = max(min(demand[src][dst],
                                                remaining[src][ind],
                                                available(dst, all_tor_buffers[ind], demand[ind][dst])),
                                    0)
                            if verbose and data_sent > 0:
                                print("        %2d->%-2d(->%-2d): sending (%3d) %3d/%d available" %
                                        (src+1, ind+1, dst+1, round(100*demand[src][dst]),
                                            round(100*data_sent), round(100*remaining[src][ind])))

                            demand[src][dst] -= data_sent
                            all_tor_buffers[ind][src][dst] += data_sent
                            remaining[src][ind] -= data_sent

            print()
            #print("     Demand")
            #print_demand(demand, prefix = "       ")
            #print_buffer(all_tor_buffers, prefix = "     ")

            # Generate demand on every slot
            new_d = generate_demand(max_demand = frac*1.3)
            run_demand += sum(sum(d) for d in new_d)
            demand = add_demand(demand, new_d)

    print("End of simulation with %d ToR switches and %d rotor switches" % (N_TOR, N_ROTOR))
    print("%d/%d=%.2f links are active at any time" % (active_links, total_links, frac))
    print("There are %d matchings, with %d slots per cycle" % (N_MATCHINGS, N_SLOTS))

