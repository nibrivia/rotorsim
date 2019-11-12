import random
from math import ceil
from helpers import *
from switches import ToRSwitch

def print_demand(tors, prefix = "", print_buffer = False):
    print()
    print("\033[0;32m      Demand")

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
                line_str += "%2d " % qty

                if src_i == ind_i:
                    line_str += "\033[0;32m"
            line_str += "-> %d  =%2d" % (dst_i, tot)
            if dst_i == ind_i:
                line_str += "\033[0;32m  rx'd"
            line_str += "\n"
        print(line_str)
    print("\033[00m")


class RotorNet:
    def __init__(self, n_rotor, n_tor, logger, verbose = True):
        self.n_rotor = n_rotor
        self.n_tor   = n_tor
        self.slot_time = -1

        # Internal variables
        self.tors = [ToRSwitch(
                            name    = i,
                            n_tor   = n_tor,
                            n_rotor = n_rotor,
                            logger  = logger,
                            verbose = verbose)
                for i in range(n_tor)]
        #self.rotors = [RotorSwitch(
        #                    self.tors,
        #                    name = "%s/%s" % (i+1, n_rotor))
        #        for i in range(n_rotor)]

        # Hack-y, generates matchings, and matches those to ToRs
        self.generate_matchings()
        self.matchings = [[(self.tors[src], self.tors[dst]) for src, dst in m]
                for m in self.matchings]

        # Number of slots depends on number of matchings
        self.n_slots = ceil(len(self.matchings) / self.n_rotor)

        # I/O stuff
        self.logger  = logger
        self.verbose = verbose

    def generate_matchings(self):
        self.matchings = []
        n_tors = len(self.tors)

        for offset in range(1, n_tors):
            # Compute the indices
            slot_matching = [(src_i, ((src_i+offset) % n_tors))
                    for src_i in range(n_tors)]

            # Add to the list of matchings
            self.matchings.append(slot_matching)


    @property
    def time_in_slots(self):
        return self.slot_time

    @property
    def time_in_cycles(self):
        return self.slot_time / self.n_slots

    def run(self, n_cycles = 1):
        """Run the simulation for n_cycles cycles"""
        for c in range(n_cycles):
            for s in range(self.n_slots):
                self.do_slot(verbose = verbose)

    def add_demand(self, new_demand):
        for src_i, src in enumerate(self.tors):
            for dst_i, dst in enumerate(self.tors):
                src.add_demand_to(dst, new_demand[src_i][dst_i])

    def vprint(self, s = "", indent = 0):
        indent_str = "  " * indent
        if self.verbose:
            print(indent_str + str(s))

    def print_demand(self):
        if self.verbose and True:
            print_demand(self.tors)

    def do_slot(self):
        self.slot_time += 1
        current_slot = self.slot_time % self.n_slots

        # It's a new cycle
        if self.time_in_slots % self.n_slots == 0:
            self.vprint("\033[1;31mCycle %d\033[00m" % (self.time_in_cycles))

        # Print slot
        self.vprint("\033[0;31mSlot %d/%d\033[00m" %
                (current_slot+1, self.n_slots), 1)

        self.print_demand()

        # Initialize tors for this slot
        for rotor_id in range(self.n_rotor):
            # Rotor n gets matchings that are n modulo N_SLOTS
            matching_i = (current_slot + rotor_id*self.n_slots) % \
                    len(self.matchings)
            rotor_matchings = self.matchings[matching_i]
            for src, dst in rotor_matchings:
                src.connect_to(rotor_id = rotor_id, tor = dst)

            #rotor.init_slot(rotor_matchings) # TODO

        # Old indirect traffic
        self.vprint("1. Old Indirect", 2)
        for tor in shuffle(self.tors):
            tor.send_old_indirect()
        #self.print_demand()

        # Direct traffic
        self.vprint("2. Direct", 2)
        for tor in shuffle(self.tors):
            tor.send_direct()
        #self.print_demand()

        # Offers
        self.vprint("3a. Offers", 2)
        for tor in shuffle(self.tors):
            tor.offer()

        # Accepts
        self.vprint("3b. Accepts", 2)
        for tor in shuffle(self.tors):
            tor.accept()

        # New indirect traffic
        self.vprint("3c. New Indirect", 2)
        for tor in shuffle(self.tors):
            tor.send_new_indirect()
        #self.print_demand()


