from network import RotorNet
from event import R
from logger import Log
from helpers import *
from tcp_flow import TCPFlow
from flow_generator import generate_flows
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

def load_flows(slot_duration):
    flows = TCPFlow.make_from_csv()
    for f in flows:
        f.slot_duration = slot_duration
    return flows


@click.command()
@click.option(
        "--num_flows",
        type=int,
        default=1
)
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
        "--slice_duration",
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
        "--pkts-file",
        type=str,
        default="pkts.txt"
)
@click.option(
        "--n_cycles",
        type=int,
        default=5
)
@click.option(
        "--workload",
        type=str,
        default='websearch'
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
def main(
        num_flows,
        n_tor, 
        n_rotor,
        packets_per_slot,
        n_cycles,
        workload,
        slice_duration, 
        reconfiguration_time, 
        jitter,
        log, 
        pkts_file,
        verbose, 
        no_log, 
        no_pause
    ):
    print("%d ToRs, %d rotors, %d packets/slot for %d cycles" %
            (n_tor, n_rotor, packets_per_slot, n_cycles))

    print("Setting up network...")
    if no_log:
        logger = None
    else:
        logger = Log(fn = log)
        logger.add_timer(R)

    if slice_duration == -1:
        slice_duration = 1

    net = RotorNet(n_rotor = n_rotor,
                   n_tor   = n_tor,
                   packets_per_slot     = packets_per_slot,
                   reconfiguration_time = reconfiguration_time,
                   slice_duration       = slice_duration,
                   jitter               = jitter,
                   logger  = logger,
                   verbose = verbose, 
                   do_pause = not no_pause)

    print("Setting up flows...")
    open(pkts_file, 'w').close()
    # generate flows
    max_slots = n_cycles*net.n_slots
    # TODO hacky
    if workload == "all":
        num_flows = n_tor*n_cycles
        workload = "chen"
    generate_flows(max_slots, num_flows, n_tor, workload)

    # open connection for each flow at the time it should arrive
    slot_duration = slice_duration*n_rotor
    flows = load_flows(slot_duration)
    for f in flows:
        time_for_arrival = f.arrival * slot_duration
        R.call_in(time_for_arrival, net.open_connection, f)

    # set up printing
    for raw_slice in range(max_slots*n_rotor):
        if raw_slice % 10 != 0:
            continue
        cycle =  raw_slice // (n_rotor*net.n_slots)
        slot  = (raw_slice // n_rotor) % net.n_slots
        sli_t =  raw_slice % n_rotor
        time = raw_slice*slice_duration
        R.call_in(time,
                print, "\033[1;91m@%.2f Cycle %s/%s, Slot %s/%s, Slice %s/%s\033[00m" % (
                    time,
                    cycle+1, n_cycles,
                    slot+1, net.n_slots,
                    sli_t+1, n_rotor),
                priority = -100)
        if not no_pause:
            R.call_in(time, print_demand, net.tors, priority=100)
            R.call_in(time, pause, priority=100)

    print("Starting simulator...")
    # Start the simulator
    net.run(n_cycles)

    if not no_log:
        logger.close()

    # dump status for all flows
    if verbose and False:
        for f in flows:
            f.dump_status()
    
    print("done")

if __name__ == "__main__":

    main()
