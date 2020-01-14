#from network import RotorNet
from event import R
from logger import Log
from helpers import *
#from tcp_flow import TCPFlow, BYTES_PER_PACKET
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
@click.option( # TODO change to some explicit control over network load
        "--load",
        type=float,
        default=0.3,
        help="% of link capacity to use"
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
        default=90,
        help='in us'
)
@click.option(
        "--reconfiguration_time",
        type=float,
        default=0,
        help='in us, disabled if 0'
)
@click.option(
        "--jitter",
        type=float,
        default=0
)
@click.option(
        "--latency",
        type=int,
        default=500,
        help='latency in ns'
)
@click.option(
        "--bandwidth",
        type=int,
        default=10e3,
        help='bandwidth in Mb/s'
)
@click.option(
        "--log",
        type=str,
        default="out.csv"
)
@click.option(
        "--time_limit",
        type=int,
        default=5000,
        help="in ms"
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
        load,
        n_tor,
        n_rotor,
        bandwidth,
        latency,
        time_limit,
        workload,
        slice_duration,
        reconfiguration_time,
        jitter,
        log,
        verbose,
        no_log,
        no_pause
    ):

    if no_log:
        logger = None
    else:
        logger = Log(fn = log)
        logger.add_timer(R)

    packets_per_slot = int(bandwidth*slice_duration/BYTES_PER_PACKET/8) # (Mb/s)*us/8 works out to (B/s)*s

    print("Setting up network...")

    slice_duration /= 1000 #divide to be in ms
    bandwidth /= 8 # Everything else uses MB/s

    net = RotorNet(n_rotor = n_rotor,
                   n_tor   = n_tor,
                   packets_per_slot     = packets_per_slot,
                   reconfiguration_time = reconfiguration_time/1000,
                   slice_duration       = slice_duration, # R.time will be in ms
                   jitter               = jitter,
                   logger  = logger,
                   verbose = verbose,
                   do_pause = not no_pause)

    n_cycles = math.ceil(time_limit/(n_rotor*net.n_slots*slice_duration))
    print("%d ToRs, %d rotors, %d packets/slot for %d cycles" %
            (n_tor, n_rotor, packets_per_slot, n_cycles))
    slot_duration = slice_duration*n_rotor
    cycle_duration = slot_duration*net.n_slots
    print("Time limit %dms, cycle %.3fms, slot %.3fms, slice %.3fms" %
            (time_limit, cycle_duration, slot_duration, slice_duration))

    print("Setting up flows...")
    # generate flows
    max_slots = n_cycles*net.n_slots
    # TODO hacky
    if workload == "all":
        assert False
        #num_flows = n_tor*n_cycles
        workload = "chen"
    generate_flows(
            load = load,
            bandwidth  = bandwidth,
            num_tors   = n_tor,
            num_rotors = n_rotor,
            time_limit = time_limit,
            workload   = workload)

    # open connection for each flow at the time it should arrive
    flows = load_flows(slot_duration)
    for f in flows:
        time_for_arrival = f.arrival * slot_duration
        R.call_in(time_for_arrival, net.open_connection, f)

    # set up printing
    for cycle in range(n_cycles):
        time = cycle*cycle_duration
        R.call_in(time,
                print, "\033[1;91m@%.2f Cycle %s/%s\033[00m" % (
                    time, cycle+1, n_cycles),
                priority = -100)
        if verbose and not no_pause:
            R.call_in(time, print_demand, net.tors, priority=100)
            R.call_in(time, pause, priority=100)

    print("Starting simulator...")
    # Start the simulator
    net.run(time_limit)

    if not no_log:
        logger.close()

    # dump status for all flows
    if verbose and False:
        for f in flows:
            f.dump_status()

    print("done")

if __name__ == "__main__":

    main()
