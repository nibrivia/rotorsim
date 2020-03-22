import random
from functools import lru_cache
import click
import sys
from event import R
from params import PARAMS

def vprint(*args, **kwargs):
    if PARAMS.verbose:
        print("%.3f     " % R.time,
                end = "")
        print(*args, **kwargs)

@lru_cache()
def bound(lo, val, hi):
    return max(lo, min(val, hi))

def shuffle(generator):
    return sorted(generator, key = lambda k: random.random())

def print_demand(tors, prefix = "", print_buffer = False):
    for tor in tors:
        print("%s%s" % (prefix, tor.buffer_str()))

def color(obj, s = None):
    color = hash(obj) % 6 + 31
    if s == None:
        s = str(obj)
    return "\033[1;%dm%s\033[0;00m" % (color, s)

def color_str_(fn):
    def wrapped(obj):
        s = fn(obj)
        return color(obj, s)
    return wrapped


_pause_enabled = True
def pause():
    global _pause_enabled
    if _pause_enabled:
        user_str = input("Press Enter to continue, (c) to continue, (x) to exit...")
        if user_str == "c":
            _pause_enabled = False
        if user_str == "x":
            sys.exit()

def get_port_type(port_id):
    if port_id < PARAMS.n_rotor:
        return "rotor"
    if port_id < PARAMS.n_rotor + PARAMS.n_xpand:
        return "xpand"
    else:
        return "cache"

def gen_ports():
    global rotor_ports
    for port_id in range(PARAMS.n_rotor):
        rotor_ports.append(port_id)

    global xpand_ports
    for i in range(PARAMS.n_xpand):
        xpand_ports.append(PARAMS.n_rotor + i)

    global cache_ports
    for i in range(PARAMS.n_cache):
        cache_ports.append(PARAMS.n_rotor + PARAMS.n_xpand + i)

rotor_ports = []
xpand_ports = []
cache_ports = []
