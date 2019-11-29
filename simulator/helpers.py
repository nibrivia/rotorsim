import random
from functools import lru_cache
import click
import sys
from event import R

def vprint(s = "", verbose = True):
    if verbose:
        print(s)

@lru_cache()
def bound(lo, val, hi):
    return max(lo, min(val, hi))

def shuffle(generator):
    return sorted(generator, key = lambda k: random.random())

def print_demand(tors, prefix = "", print_buffer = False):
    for tor in tors:
        print("%s%s" % (prefix, tor.buffer_str()))

# def print_packet(p, logfile='pkts.txt', ack=False):
#     parts = []
#     parts.append('Packet(src={}'.format(p.src.id))
#     parts.append('dst={}'.format(p.dst.id))
#     parts.append('seq_num={}'.format(p.seq_num))
#     parts.append('flow={}'.format(p.flow.id))
#     parts.append('ack={}'.format(ack))
#     parts.append('time={})'.format(R.time))

#     out = ', '.join(parts)
#     with open(logfile, 'a') as lfile:
#         lfile.write(out + '\n')
#     # print(out)


_pause_enabled = True
def pause():
    global _pause_enabled
    if _pause_enabled:
        user_str = input("Press Enter to continue, (c) to continue, (x) to exit...")
        if user_str == "c":
            _pause_enabled = False
        if user_str == "x":
            sys.exit()
