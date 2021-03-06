from network import RotorNet
import csv
from event import R
from logger import LOG, init_log
from flow_generator import generate_flows, FLOWS, N_FLOWS, N_DONE, BYTES_PER_PACKET, flow_combiner, ml_generator
import sys, uuid
import click
import uuid as _uuid
import math, sys
from params import PARAMS
from helpers import *


@click.command()
@click.option(
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
        "--valiant",
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
        is_ml,
        valiant
    ):


    # Set parameters
    # (Mb/s)*us/8 works out to (B/s)*s
    packets_per_slot = int(bandwidth*slice_duration/(BYTES_PER_PACKET*8))
    slice_duration /= 1000 #divide to be in ms
    reconfiguration_time /= 1000 #divide to be in ms
    bandwidth_Bms = bandwidth * 1e6 / 1e3 / 8

    random.seed(40) # TODO Just to make things reproducible

    # Compute switch counts
    if n_xpand is not None:
        assert n_xpand <= n_switches
        n_xpand = n_xpand
    else:
        n_xpand = 0 #round(min(5, n_switches/3))

    if n_cache is not None:
        assert n_cache + n_xpand <= n_switches
        assert n_cache < n_switches
        n_cache = n_cache
    else:
        n_cache = floor((n_switches - n_xpand) / 2)

    n_rotor = n_switches - n_xpand - n_cache
    print("%d xpander, %d rotor, %d cache. %d total" %
            (n_xpand, n_rotor, n_cache, n_switches))

    if uuid is None:
        uuid = _uuid.uuid4()
    slot_duration = slice_duration#*n_rotor
    cycle_duration = slice_duration*n_rotor


    del slice_duration
    PARAMS.set_many(locals())
    PARAMS.flow_print = -1
    print(PARAMS)
    gen_ports()
    print("Setting up network...")

    # Uses global params object
    net = RotorNet()

    if n_rotor > 0:
        n_slots = math.ceil(time_limit/slot_duration)
        n_cycles = math.ceil(time_limit/(n_rotor*n_slots*PARAMS.slot_duration))
    else:
        n_cycles = 1
        n_slots = 1

    max_slots = n_cycles*n_slots
    #cycle_duration = slot_duration*n_slots
    slice_duration = slot_duration

    print("Time limit %dms, cycle %.3fms, slot %.3fms, slice %.3fms" %
            (PARAMS.time_limit, PARAMS.cycle_duration, PARAMS.slot_duration, slice_duration))
    print("#tor: %d, #rotor: %d, #links: %d, bw: %dGb/s, capacity: %.3fGb/s" %
            (PARAMS.n_tor, PARAMS.n_rotor, PARAMS.n_tor*PARAMS.n_rotor, PARAMS.bandwidth/1e3,
                PARAMS.n_tor*PARAMS.n_switches*PARAMS.bandwidth/1e3))

    print("Setting up flows, load %d%%..." % (100*load))
    # generate flows
    flow_gen = generate_flows(
            load = load,
            n_tor = n_tor,
            bandwidth = bandwidth,
            time_limit = time_limit,
            n_switches = n_switches,
            workload_name = workload,
            arrive_at_start = arrive_at_start,
            skewed = skewed,
            )


    # Start the log
    if not no_log:
        init_log(fn = None, **locals())

    # set up printing
    time = 0
    while time < time_limit*10:
        time += slice_duration
        if verbose and not no_pause:
            R.call_in(time, pause, priority=100)

    #print time
    R.call_in(0, print_time, time_limit)

    print("Starting simulator...")
    if is_ml:
        ml_generator(network = net, n_jobs = 3, servers_per_ring = 4, model_name = "resnet")
        ml_generator(network = net, n_jobs = 3, servers_per_ring = 4, model_name = "vgg")
        ml_generator(network = net, n_jobs = 3, servers_per_ring = 4, model_name = "gpt2")

    # Start the simulator
    net.run(flow_gen = flow_gen, time_limit = time_limit)

    # Force log the unfinished flows
    for f in FLOWS.values():
        LOG.log_flow_done(f)

    # Create a new log with the 
    u_fn = "utilization-" + str(LOG.sim_id) + ".csv"
    max_packets = (R.time/1000) * (bandwidth*1e6) / (BYTES_PER_PACKET*8)
    """
    with open(u_fn, "w") as f:
        print("switch,type,port,n_packets,divisor", file = f)
        for s in net.switches:
            #if s.tag == "cache":
                #divisor = R.time
            #else:
            divisor = max_packets

            for port_id, n in enumerate(s.n_packets):
                print(",".join(str(x) for x in [s.id, s.tag, port_id, n, divisor]), file = f)#
                """

    # Done!
    if LOG is not None:
        LOG.close()
    print("done")


def print_time(time_limit):
    if PARAMS.verbose:
        end = "\n"
    else:
        end = ""

    print("\x1b[2K\r\033[1;91m%.3fms of %dms \033[00m %d (%d)" % (
        R.time, time_limit, len(FLOWS), N_FLOWS[0]),
        end = end,
        )
    #print("%dms of %dms \033[00m %d" % (R.time, time_limit, len(FLOWS)), end = "")
    R.call_in(1, print_time, time_limit)

if __name__ == "__main__":
    main()
