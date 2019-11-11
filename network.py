import random
from math import ceil
from helpers import *
from switches import ToRSwitch, RotorSwitch

def print_demand(tors, prefix = "", print_buffer = False):
    print()
    print("\033[0;32m      Demand")
    print("        Direct")
    print_buffer = True
    for src_i, src in enumerate(tors):
        line_str = "          " + str(src) + " -> "
        for dst_i, dst in enumerate(tors):
            line_str += "%2d " % src.outgoing[dst_i].size
        print(line_str)

    if print_buffer:
        print("        Indirect")
        for ind_i, ind in enumerate(tors):
            line_str = "          ToR " + str(ind_i+1) + "\n"
            for dst_i, dst in enumerate(tors):
                tot = 0
                line_str += "            " 
                for src_i, src in enumerate(tors):
                    qty = ind.indirect[dst_i][src_i].size
                    tot += qty
                    line_str += "%2d " % qty
                line_str += "-> %d  =%2d\n" % (dst_i+1, tot)
            print(line_str)
    print("\033[00m")


class RotorNet:
    def __init__(self, n_rotor, n_tor, logger, verbose = True):
        self.n_rotor = n_rotor
        self.n_tor   = n_tor
        self.slot_time = -1

        # Internal variables
        self.n_slots = 1
        self.tors = [ToRSwitch(
                            name = "%s" % (i+1),
                            n_tor = n_tor,
                            logger = logger)
                for i in range(n_tor)]
        self.rotors = [RotorSwitch(
                            self.tors,
                            name = "%s/%s" % (i+1, n_rotor))
                for i in range(n_rotor)]

        self.generate_matchings()
        self.slots_per_cycle = ceil(len(self.matchings) / self.n_rotor)

        # Printing stuff
        self.logger = logger
        self.verbose = verbose

    def generate_matchings(self):
        self.matchings = []
        n_tors = len(self.tors)

        for offset in range(1, n_tors):
            # Compute the indices
            slot_matching = [(src_i, (src_i+offset) % n_tors)
                    for src_i in range(n_tors)]

            # Add to the list of matchings
            self.matchings.append(slot_matching)


    @property
    def time_in_slots(self):
        return self.slot_time

    @property
    def time_in_cycles(self):
        return self.slot_time / len(self.matchings)

    def run(self, n_cycles = 1):
        """Run the simulation for n_cycles cycles"""
        for c in range(n_cycles):
            self.vprint("\n\033[01;31mCycle %d/%d\033[00m" % (cycle+1, n_cycles))
            for s in range(self.slots_per_cycle):
                self.do_slot(verbose = verbose)

    def add_demand(self, new_demand):
        for src_i, src_demand in enumerate(new_demand):
            for dst_i, demand in enumerate(src_demand):
                src.add_demand_to(dst_i, demand)

    def vprint(self, s = ""):
        if self.verbose:
            print(s)

    def print_demand(self):
        if self.verbose:
            print_demand(self.tors)

    def do_slot(self):
        self.slot_time += 1
        current_slot = self.slot_time % self.slots_per_cycle

        self.vprint("  \033[0;31mSlot %d/%d\033[00m" % (current_slot+1, self.n_slots))

        self.print_demand()

        # Initialize rotors for this slot
        for r_n, rotor in enumerate(self.rotors):
            # Rotor n gets matchings that are n modulo N_SLOTS
            matching_i = (current_slot + r_n*self.n_slots) % len(self.matchings)
            rotor_matchings = self.matchings[matching_i]

            rotor.init_slot(rotor_matchings) # TODO

        # Old indirect traffic
        self.vprint("    1. Old Indirect")
        for rotor in shuffle(self.rotors):
            rotor.send_old_indirect()
        self.print_demand()

        # Direct traffic
        self.vprint("    2. Direct")
        for rotor in shuffle(self.rotors):
            rotor.send_direct()
        self.print_demand()

        # New indirect traffic
        self.vprint("    3. New Indirect")
        for rotor in shuffle(self.rotors):
            rotor.send_new_indirect()
        self.print_demand()


