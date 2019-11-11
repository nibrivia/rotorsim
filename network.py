import random
from math import ceil
from helpers import *
from switches import ToRSwitch, RotorSwitch

def print_demand(tors, prefix = "", print_buffer = False):
    vprint()
    vprint("\033[0;32m      Demand")
    vprint("        Direct")
    print_buffer = True
    for src_i, src in enumerate(tors):
        line_str = "          " + str(src) + " -> "
        for dst_i, dst in enumerate(tors):
            line_str += "%2d " % src.outgoing[dst_i].size
        vprint(line_str)

    if print_buffer:
        vprint("        Indirect")
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
            vprint(line_str)
    vprint("\033[00m")


class RotorNet:
    def __init__(self, n_rotor, n_tor, logger):
        self.n_rotor = n_rotor
        self.n_tor   = n_tor
        self.slot_time = 0

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
        self.verbose = False

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
            vprint("\n\033[01;31mCycle %d/%d\033[00m" % (cycle+1, n_cycles))
            for s in range(self.slots_per_cycle):
                self.do_slot(verbose = verbose)

    def add_demand(self, new_demand):
        for src_i, src_demand in enumerate(new_demand):
            for dst_i, demand in enumerate(src_demand):
                src.add_demand_to(dst_i, demand)

    def print_console(self, s):
        if self.verbose:
            print(s)

    def do_slot(self):
        self.slot_time += 1
        current_slot = self.slot_time % self.slots_per_cycle

        vprint("  \033[0;31mSlot %d/%d\033[00m" % (current_slot+1, self.n_slots))

        print_demand(self.tors)

        # Initialize rotors for this slot
        for r_n, rotor in enumerate(self.rotors):
            # Rotor n gets matchings that are n modulo N_SLOTS
            matching_i = (current_slot + r_n*self.n_slots) % len(self.matchings)
            rotor_matchings = self.matchings[matching_i]

            rotor.init_slot(rotor_matchings) # TODO

        # Old indirect traffic
        vprint("    1. Old Indirect")
        for rotor in shuffle(self.rotors):
            rotor.send_old_indirect()
        print_demand(self.tors)

        # Direct traffic
        vprint("    2. Direct")
        for rotor in shuffle(self.rotors):
            rotor.send_direct()
        print_demand(self.tors)

        # New indirect traffic
        vprint("    3. New Indirect")
        for rotor in shuffle(self.rotors):
            rotor.send_new_indirect()
        print_demand(self.tors)


