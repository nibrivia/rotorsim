import random
import click

def vprint(s = "", verbose = True):
    if verbose:
        print(s)

def bound(lo, val, hi):
    return max(lo, min(val, hi))

def shuffle(generator):
    return sorted(generator, key = lambda k: random.random())

def print_demand(tors, prefix = "", print_buffer = False):
    print()
    print("\033[0;32m      Demand")
    all_tot = 0


    for ind_i, ind in enumerate(tors):
        line_str = "          ToR " + str(ind_i) + "\n"
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
                all_tot += qty
                line_str += "%2d " % qty

                if src_i == ind_i:
                    line_str += "\033[0;32m"
            line_str += "-> %d  =%2d" % (dst_i, tot)
            if dst_i == ind_i:
                line_str += "\033[0;32m  rx'd"
                all_tot -= tot
            line_str += "\n"
        print(line_str)
    print("\033[00m")

    return all_tot

