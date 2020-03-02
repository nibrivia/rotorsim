from network import RotorNet
import csv
from event import R
from logger import LOG, init_log
from helpers import *
#from tcp_flow import TCPFlow, BYTES_PER_PACKET
from flow_generator import generate_flows, FLOWS, N_FLOWS, N_DONE, BYTES_PER_PACKET
import sys, uuid
import click
import math
from params import *


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


# TODO fix time values
#100us per slot +10s reconfiguration

# use same seed

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
        "--n_switches",
        type=int,
        default=2
)
@click.option(
        "--n_cache",
        type=int,
        default=None
)
@click.option(
        "--n_xpand",
        type=int,
        default=0
)
@click.option(
        "--slice_duration",
        type=float,
        default=100,
        help='in us'
)
@click.option(
        "--reconfiguration_time",
        type=float,
        default=10,
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
        "--cache_policy",
        type=str,
        default=""
)
@click.option(
        "--uuid",
        type=str,
        default=None
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
@click.option(
        "--skewed",
        is_flag=True
)
@click.option(
        "--is_ml",
        is_flag=True
)
@click.option(
        "--arrive-at-start",
        is_flag=True
)
def main(
        load,
        n_tor,
        n_switches,
        n_xpand,
        n_cache,
        bandwidth,
        arrive_at_start,
        latency,
        time_limit,
        workload,
        slice_duration,
        reconfiguration_time,
        jitter,
        uuid,
        log,
        verbose,
        no_log,
        no_pause,
        skewed,
        cache_policy,
        is_ml
    ):


    NET_PARAMS.load = load
    NET_PARAMS.n_switches = n_switches
    NET_PARAMS.cache_policy = cache_policy
    packets_per_slot = int(bandwidth*slice_duration/(BYTES_PER_PACKET*8)) # (Mb/s)*us/8 works out to (B/s)*s

    print("Setting up network...")

    slice_duration /= 1000 #divide to be in ms

    net = RotorNet(n_switches = n_switches,
                   n_cache = n_cache,
                   n_xpand = n_xpand,
                   n_tor   = n_tor,
                   arrive_at_start = arrive_at_start,
                   packets_per_slot     = packets_per_slot,
                   reconfiguration_time = reconfiguration_time/1000,
                   slice_duration       = slice_duration, # R.time will be in ms
                   jitter               = jitter,
                   verbose = verbose,
                   do_pause = not no_pause)

    if net.n_rotor > 0:
        n_cycles = math.ceil(time_limit/(net.n_rotor*net.n_slots*slice_duration))
    else:
        n_cycles = 1
    #print("%d ToRs, %d rotors, %d packets/slot for %d cycles" %
            #(n_tor, n_rotor, packets_per_slot, n_cycles))
    slot_duration = slice_duration*net.n_rotor
    cycle_duration = slot_duration*net.n_slots
    print("Time limit %dms, cycle %.3fms, slot %.3fms, slice %.3fms" %
            (time_limit, cycle_duration, slot_duration, slice_duration))
    print("#tor: %d, #rotor: %d, #links: %d, bw: %dGb/s, capacity: %.3fGb/s" %
            (n_tor, net.n_rotor, n_tor*net.n_rotor, bandwidth/1e3, n_tor*n_switches*bandwidth/1e3))

    print("Setting up flows, load %d%%..." % (100*load))
    # generate flows
    max_slots = n_cycles*net.n_slots
    flow_gen = generate_flows(
            load = load,
            bandwidth  = bandwidth,
            num_tors   = n_tor,
            num_switches = n_switches,
            time_limit = time_limit,
            arrive_at_start = arrive_at_start,
            workload_name   = workload,
            skewed = skewed,
            is_ml = is_ml)


    # Start the log
    if not no_log:
        #base_fn = "{n_tor}-{n_switches}:{n_cache},{n_xpand}-{workload}-{load}-{time_limit}ms".format(**locals())
        n_rotor = net.n_rotor
        n_cache = net.n_cache
        n_xpand = net.n_xpand
        if arrive_at_start:
            base_fn = "drain-" + base_fn
        init_log(fn = None, **locals())

    # set up printing
    time = 0
    while time < time_limit*10:
        time += slice_duration
        if verbose and not no_pause:
            R.call_in(time, print_demand, net.tors, priority=99)
            R.call_in(time, pause, priority=100)

    #print time
    R.call_in(0, print_time, time_limit)

    print("Starting simulator...")
    # Start the simulator
    net.run(flow_gen = flow_gen, time_limit = time_limit)

    # Force log the unfinished flows
    for f in FLOWS.values():
        LOG.log_flow_done(f)

    # Create a new log with the 
    u_fn = "utilization-" + str(LOG.sim_id) + ".csv"
    max_packets = (R.time/1000) * (bandwidth*1e6) / (BYTES_PER_PACKET*8)
    with open(u_fn, "w") as f:
        print("switch,type,port,n_packets,divisor", file = f)
        for s in net.switches:
            if s.tag == "cache":
                divisor = R.time
            else:
                divisor = max_packets

            for port_id, n in enumerate(s.n_packets):
                print(",".join(str(x) for x in [s.id, s.tag, port_id, n, divisor]), file = f)

    # Done!
    if LOG is not None:
        LOG.close()
    print("done")


def print_time(time_limit):
    print("\x1b[2K\r\033[1;91m%dms of %dms \033[00m %d (%d)" % (
        R.time, time_limit, len(FLOWS), N_FLOWS[0]),
        end = "")
    #print("%dms of %dms \033[00m %d" % (R.time, time_limit, len(FLOWS)), end = "")
    R.call_in(1, print_time, time_limit)


if __name__ == "__main__":

    main()
