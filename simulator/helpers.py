import random
from functools import lru_cache
import click
import sys

def vprint(s = "", verbose = True):
    if verbose:
        print(s)

@lru_cache()
def bound(lo, val, hi):
    return max(lo, min(val, hi))

def shuffle(generator):
    return sorted(generator, key = lambda k: random.random())

def print_demand(tors, prefix = "", print_buffer = False):
    print()
    print("\033[0;32m      Demand")

    for ind_i, ind in enumerate(tors):
        line_str = "          ToR " + str(ind_i) + " d" +str(ind.tot_demand) + "\n"
        for dst_i, dst in enumerate(tors):
            tot = 0
            if dst_i == ind_i:
                line_str += "\033[1;32m"
            line_str += "            " 
            for src_i, src in enumerate(tors):

                if src_i == dst_i:
                    line_str += " - "
                    continue

                if src_i == ind_i:
                    line_str += "\033[1;32m"

                qty = ind.buffers[(src_i, dst_i)].size
                tot += qty
                line_str += "%2d " % qty

                if src_i == ind_i:
                    line_str += "\033[0;32m"
            line_str += "-> %d  =%2d" % (dst_i, tot)
            if dst_i == ind_i:
                line_str += "\033[0;32m  rx'd"
            line_str += "\n"
        print(line_str)
    print("\033[00m")


_pause_enabled = True
def pause():
    global _pause_enabled
    if _pause_enabled:
        user_str = input("Press Enter to continue, (c) to continue, (x) to exit...")
        if user_str == "c":
            _pause_enabled = False
        if user_str == "x":
            sys.exit()
