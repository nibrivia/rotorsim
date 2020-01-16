import random
import sys
from math import ceil
from helpers import *
from tor_switch import ToRSwitch
from rotor_switch import RotorSwitch
from event import Registry, Delay, stop_simulation, R

class RotorNet:
    def __init__(self,
                 n_rotor, 
                 n_tor,
                 packets_per_slot,
                 slice_duration = 1, 
                 reconfiguration_time = 0, 
                 jitter = 0,
                 logger = None,
                 verbose = True, 
                 do_pause = True):
        self.n_rotor = n_rotor
        self.n_tor   = n_tor

        # Matchings need to be done early to get constants
        self.generate_matchings()
        self.n_slots = ceil(len(self.matchings) / self.n_rotor)

        self.slice_duration = slice_duration
        self.slot_duration  = slice_duration*n_rotor
        self.cycle_duration = self.slot_duration * self.n_slots

        # Internal variables
        self.rotors = [RotorSwitch(
                            id = i,
                            n_ports = n_tor,
                            n_rotor = n_rotor,
                            slice_duration       = slice_duration,
                            reconfiguration_time = reconfiguration_time,
                            clock_jitter         = jitter,
                            verbose = verbose,
                            logger  = logger)
                for i in range(n_rotor)]

        self.tors = [ToRSwitch(
                            name    = i,
                            n_tor   = n_tor,
                            n_rotor = n_rotor,
                            packets_per_slot = packets_per_slot,
                            slice_duration = slice_duration,
                            clock_jitter  = jitter,
                            logger  = logger,
                            verbose = verbose)
                for i in range(n_tor)]

        for tor in self.tors:
            tor.set_tor_refs(self.tors)

        # Physically connect them up
        for rotor in self.rotors:
            rotor.connect_tors(self.tors)

        # We can now associate the matchings with objects
        self.matchings = [[(self.tors[src], self.tors[dst]) for src, dst in m]
                for m in self.matchings]


        # This is what we'll distribute
        self.matchings_by_slot_rotor = []
        for slot in range(self.n_slots):
            slot_matchings = []
            for rotor in self.rotors:
                matching_i = (slot + rotor.id*self.n_slots) % \
                        len(self.matchings)
                rotor_matchings = self.matchings[matching_i]
                slot_matchings.append(rotor_matchings)
            self.matchings_by_slot_rotor.append(slot_matchings)

        # Distribute to ToRs
        for tor in self.tors:
            tor.add_matchings(self.matchings_by_slot_rotor)

        # Distribute to rotors
        for rotor in self.rotors:
            rotor_matchings_by_slot = [
                    self.matchings_by_slot_rotor[slot][rotor.id]
                            for slot in range(self.n_slots)]
            rotor.add_matchings(rotor_matchings_by_slot)

        # I/O stuff
        self.verbose  = verbose
        self.do_pause = do_pause

    def generate_matchings(self):
        self.matchings = []
        n_tors = self.n_tor

        for offset in range(1, n_tors):
            # Compute the indices
            slot_matching = [(src_i, ((src_i+offset) % n_tors))
                    for src_i in range(n_tors)]

            # Add to the list of matchings
            self.matchings.append(slot_matching)


    def run(self, time_limit, flows):
        """Run the simulation for n_cycles cycles"""
        # Register first events
        for r in self.rotors:
            r.start()
        for t in self.tors:
            t.start()

        # Flows
        for f in flows:
            R.call_in(f.arrival, self.open_connection, f)

        # Start events
        R.limit = time_limit
        R.run_next()

    def open_connection(self, flow):
        # override src and dst to tor objects
        print("@%.2f Should start flow %d" % (R.time, flow.id))

        # begin sending
        #tcpflow.send()

    def print_demand(self):
        if self.verbose:
            print_demand(self.tors)
