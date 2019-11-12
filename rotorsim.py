from network import RotorNet
from logger import Log
from helpers import *
import click



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

@click.command()
@click.option(
        "--n_tor",
        type=int,
        default=4
)
@click.option(
        "--n_rotor",
        type=int,
        default=2
)
@click.option(
        "--packets_per_slot",
        type=int,
        default=10
)
@click.option(
        "--log",
        type=str,
        default="out.csv"
)
@click.option(
        "--n_cycles",
        type=int,
        default=5
)
@click.option(
        "--verbose",
        is_flag=True
)
def main(n_tor, n_rotor, packets_per_slot, log, n_cycles, verbose):
    #print("%d ToRs, %d rotors, %d packets/slot" %
    #        (N_TOR, N_ROTOR, PACKETS_PER_SLOT))
    #print("  => %d matchings, %d slots/cycle" %
    #        (N_MATCHINGS, N_SLOTS))

    # Initial demand
    #active_links = N_TOR*N_ROTOR
    #total_links = N_TOR*(N_TOR-1)
    #frac = active_links/total_links


    logger = Log(fn = log)
    net = RotorNet(n_rotor = n_rotor, n_tor = n_tor, logger = logger, verbose = verbose)

    #demand = generate_static_demand(matchings_by_slot[-1], max_demand = frac)
    #run_demand = sum(sum(d) for d in demand)


    print("---")
    ones = [[2 if i != j else 0 for i in range(n_tor)] for j in range(n_tor)]
    
    for cycle in range(n_cycles):
        net.add_demand(ones)

        # Send data
        for slot in range(net.n_slots):
            net.do_slot()

    #close_log()
    logger.close()

    print("done")

if __name__ == "__main__":

    main()
