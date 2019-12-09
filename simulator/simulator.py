from network import RotorNet
from event import R
from logger import Log
from helpers import *
import sys
import click
import math


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
        "--slot_duration",
        type=float,
        default=-1
)
@click.option(
        "--reconfiguration_time",
        type=float,
        default=0
)
@click.option(
        "--jitter",
        type=float,
        default=0
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
@click.option(
        "--no-log",
        is_flag=True
)
@click.option(
        "--no-pause",
        is_flag=True
)
def main(n_tor, n_rotor,
        packets_per_slot, n_cycles,
        slot_duration, reconfiguration_time, jitter,
        log,
        verbose, no_log, no_pause):
    print("%d ToRs, %d rotors, %d packets/slot for %d cycles" %
            (n_tor, n_rotor, packets_per_slot, n_cycles))

    print("Setting up network...")
    if no_log:
        logger = None
    else:
        logger = Log(fn = log)
        logger.add_timer(R)

    if slot_duration == -1:
        slot_duration = 1/math.ceil((n_tor-1)/n_rotor)

    net = RotorNet(n_rotor = n_rotor,
                   n_tor   = n_tor,
                   packets_per_slot     = packets_per_slot,
                   reconfiguration_time = reconfiguration_time,
                   slot_duration        = slot_duration,
                   jitter               = jitter,
                   logger  = logger,
                   verbose = verbose, 
                   do_pause = not no_pause)

    print("Setting up demand...")
    ones = [[2 if i != j else 0 for i in range(n_tor)] for j in range(n_tor)]
    if n_tor == 4:
        demand = [
                #0  1  2  3   # ->to
                [0, 0, 0, 1], # ->0
                [0, 0, 1, 0], # ->1
                [0, 1, 0, 0], # ->2
                [1, 0, 0, 0], # ->3
                ]
    elif n_tor == 5:
        demand = [
                #0  1  2  3  4   # ->to
                [0, 0, 0, 1, 1], # ->0
                [0, 0, 1, 0, 0], # ->1
                [0, 1, 0, 0, 0], # ->2
                [1, 0, 0, 0, 0], # ->3
                [1, 0, 0, 0, 0], # ->4
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

    demand = [[int(v*packets_per_slot*9) for v in row] for row in demand]
    R.call_in(-.01, net.add_demand, demand)

    for raw_slot in range(n_cycles*net.n_slots+1):
        cycle = raw_slot // net.n_slots
        slot  = raw_slot %  net.n_slots
        if slot != 0 and not verbose:
            continue
        time = raw_slot*slot_duration
        R.call_in(time,
                print, "\n@%.2f Cycle %s, Slot %s/%s" % (time, cycle, slot, net.n_slots),
                priority = -100)
        if not no_pause:
            R.call_in(time, print_demand, net.tors, priority=100)
            R.call_in(time, pause, priority=100)

    print("Starting simulator...")
    # Start the simulator
    net.run(n_cycles)

    if not no_log:
        logger.close()

    print("done")

if __name__ == "__main__":

    main()
