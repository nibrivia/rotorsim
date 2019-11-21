import random
import sys
from math import ceil
from helpers import *
from tor_switch import ToRSwitch
from rotor_switch import RotorSwitch
from event import Registry, Delay, stop_simulation, R

class RotorNet:
    def __init__(self, n_rotor, n_tor, packets_per_slot, logger, verbose = True, do_pause = True):
        self.n_rotor = n_rotor
        self.n_tor   = n_tor
        self.slot_time = -1

        # Internal variables
        self.rotors = [RotorSwitch(id = i, n_ports = n_tor) for i in range(n_rotor)]

        self.tors = [ToRSwitch(
                            name    = i,
                            n_tor   = n_tor,
                            n_rotor = n_rotor,
                            packets_per_slot = packets_per_slot,
                            logger  = logger,
                            verbose = verbose)
                for i in range(n_tor)]

        for rotor in self.rotors:
            rotor.connect_tors(self.tors)

        # Hack-y, generates matchings, and matches those to ToRs
        self.generate_matchings()
        self.matchings = [[(self.tors[src], self.tors[dst]) for src, dst in m]
                for m in self.matchings]

        # Number of slots depends on number of matchings
        self.n_slots = ceil(len(self.matchings) / self.n_rotor)

        # I/O stuff
        self.verbose  = verbose
        self.do_pause = do_pause

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
                R.call_in(delay = c+s/self.n_slots, fn = self.do_slot)
        R.run_next()

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
        # There's nothing to do, stop
        total_demand = sum(tor.tot_demand for tor in self.tors)
        if total_demand == 0:
            print("Stopping simulation. Slot #%d (Cycle #%d)" % (
                self.time_in_slots, self.time_in_cycles))
            stop_simulation(R)
            return

        self.slot_time += 1
        current_slot = self.slot_time % self.n_slots

        # It's a new cycle
        if self.time_in_slots % self.n_slots == 0:
            print("\033[1;31mCycle %d\033[00m" % (self.time_in_cycles))

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

            self.rotors[rotor_id].install_matchings(rotor_matchings)
            for src, dst in rotor_matchings:
                src.connect_to(rotor_id = rotor_id, tor = dst)


        if self.verbose and self.do_pause:
            pause()


