import random

N_TOR   = 4
N_ROTOR = 1

random.seed(375620)

def generate_matchings():
    all_matchings = []
    for offset in range(1, N_TOR):
        slot_matching = [-1 for _ in range(N_TOR)]
        for src in range(N_TOR):
            slot_matching[src] = (src+offset) % N_TOR
        all_matchings.append(slot_matching)

    return all_matchings

def generate_demand():
    # Demand is scaled: 1 how much can be sent in 1 matching slot
    return [[random.random() for dst in range(N_TOR)] for src in range(N_TOR)]

def add_demand(old, new):
    for row, _ in enumerate(old):
        for col, _ in enumerate(old):
            old[row][col] += new[row][col]
    return old


def print_demand(demand, prefix = ""):
    for src, d in enumerate(demand):
        print("%s%d -> %s" % (prefix, src, " ".join(["%.2f" % i for i in d])))

if __name__ == "__main__":
    print("Starting simulation with %d ToR switches and %d rotor switches" % (N_TOR, N_ROTOR))

    matchings_by_slot = generate_matchings()

    print()
    print("matchings")
    for slot, matches in enumerate(matchings_by_slot):
        slot_str = "slot %d: " % slot
        for src, dst in enumerate(matches):
            slot_str += "%d->%d " % (src, dst)
        print(slot_str)

    # Initial demand
    demand = generate_demand()

    print()
    N_CYCLES = 10
    for cycle in range(N_CYCLES):
        print("Cycle %d" % cycle)

        # Send data
        for slot, matchings in enumerate(matchings_by_slot):
            print("    Slot %d" % slot)
            print_demand(demand, prefix = "     ")

            # Go through every matching and send as much as we can
            for src, dst in enumerate(matchings):
                # Send to remote destination
                data_sent = min(max(0, demand[src][dst]), 1)
                print("        %d->%d: sending %.2f" % (src, dst, data_sent))
                demand[src][dst] -= data_sent

                # Send locally
                data_sent = min(max(0, demand[src][src]), 1)
                print("        %d->%d: sending %.2f" % (src, src, data_sent))
                demand[src][src] -= data_sent



        # Generate demand on every cycle
        demand = add_demand(demand, generate_demand())


