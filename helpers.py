import random

def vprint(s = "", verbose = True):
    if verbose:
        print(s)

def bound(lo, val, hi):
    return max(lo, min(val, hi))

def shuffle(generator):
    return sorted(generator, key = lambda k: random.random())
