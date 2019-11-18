from network import RotorNet, R
from logger import Log
from helpers import *
import sys
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
    print("%d ToRs, %d rotors, %d packets/slot for %d cycles" %
            (n_tor, n_rotor, packets_per_slot, n_cycles))

    print("Setting up network...")
    logger = Log(fn = log)
    net = RotorNet(n_rotor = n_rotor,
                   n_tor   = n_tor,
                   packets_per_slot = packets_per_slot,
                   logger  = logger,
                   verbose = verbose)

    print("Setting up demand...")
    ones = [[2 if i != j else 0 for i in range(n_tor)] for j in range(n_tor)]
    if n_tor == 4:
        demand = [
                #0  1  2  3   # ->to
                [0, 0, 0, 1], # ->0
                [1, 0, 1, 0], # ->1
                [0, 1, 0, 1], # ->2
                [1, 0, 0, 0], # ->3
                ]
    elif n_tor == 8:
        demand = [
                #1  2  3  4  5  6  7  8   # ->to
                [0, 0, 0, 0, 0, 1, 0, 0], # ->1
                [0, 0, 0, 0, 1, 0, 0, 0], # ->2
                [0, 0, 0, 0, 0, 0, 1, 0], # ->3
                [0, 0, 0, 0, 0, 0, 0, 1], # ->4
                [1, 0, 0, 0, 0, 0, 0, 0], # ->5
                [0, 1, 0, 0, 0, 0, 0, 0], # ->6
                [0, 0, 0, 1, 0, 0, 0, 0], # ->7
                [0, 0, 1, 0, 0, 0, 0, 0]  # ->8
                ]
    else:
        demand = [[random.uniform(0, 1) for r in range(n_tor)] for c in range(n_tor)]

    demand = [[demand[j][i] for j in range(n_tor)] for i in range(n_tor)]

    demand = [[int(v*packets_per_slot*1000) for v in row] for row in demand]
    R.call_in(-.01, net.add_demand, demand)
    



    print("Starting simulator...")
    # Start the simulator
    net.run(n_cycles)

    #close_log()
    logger.close()

    print("done")

if __name__ == "__main__":

    main()
