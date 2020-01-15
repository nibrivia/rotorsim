import random
import sys
from math import ceil
from helpers import *
#from tor_switch import ToRSwitch
#from rotor_switch import RotorSwitch
from event import Registry, Delay, stop_simulation, R

class RotorNet:
    def __init__(self,
                 n_rotor, 
                 n_tor,
                 #packets_per_slot,
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
        self.tors = [(None, None, None) for _ in range(n_tor)]

        # We can now associate the matchings with objects
        self.matchings = [[(self.tors[src], self.tors[dst]) for src, dst in m]
                for m in self.matchings]

        # Breakup matchings by slot, and rotor
        self.matchings_by_slot_rotor = []
        for slot in range(self.n_slots):
            slot_matchings = []
            for rotor_id in range(n_rotor):
                matching_i = (slot + rotor_id*self.n_slots) % \
                        len(self.matchings)
                rotor_matchings = self.matchings[matching_i]
                slot_matchings.append(rotor_matchings)
            self.matchings_by_slot_rotor.append(slot_matchings)

        # Actual demand matrix
        self.xpand_from_to = [[[] for _ in range(n_tor)] for __ in range(n_tor)]
        self.rotor_from_to = [[[] for _ in range(n_tor)] for __ in range(n_tor)]
        self.cache_from_to = [[[] for _ in range(n_tor)] for __ in range(n_tor)]

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

    @property
    def slice_id(self):
        return round(R.time/self.slice_duration)

    def do_slice(self, tor_id):
        # Just for clarity:
        #  - slice is the time between any topology change
        #  - slot  is the time for each rotor to change once
        #  - cycle is the time for a whole topology period

        # Call ourselves back at the end of the slice
        R.call_in(self.slice_duration, self.do_slice, tor_id = tor_id)

        # Expander
        for dst in range(self.n_tor):
            capacity = 1
            for f in self.xpand_from_to[tor_id][dst]:
                if capacity == 0:
                    break
                amount = min(capacity, f.remaining)
                f.send(amount) # TODO implement flow method
                capacity -= amount

        # RotorNet
        rotor_dst = 0 # TODO
        # Old indirect
        # Direct
        for f in self.rotor_from_to[tor_id][rotor_dst]:
            pass
        # New indirect


        # CacheNet
        cache_flows = self.cache_from_to[tor_id]
        for f in cache_flows:
            break

            if f.dst in self.cache_links[tor_id]:
                # send 
                pass
            else:
                # try to get cache
                pass



    def run(self, flows, time_limit):
        """Run the simulation for n_cycles cycles"""
        # Add flows
        for f in flows:
            R.call_in(f.arrival, self.open_connection, f)

        # Start first events
        for tor in range(self.n_tor):
            R.call_in(0, self.do_slice, tor_id = tor)

        # Start events
        R.limit = time_limit
        R.run_next()

    def open_connection(self, tcpflow):
        if tcpflow.size < 1e6:
            self.xpand_from_to[tcpflow.src][tcpflow.dst].append(tcpflow)
        elif tcpflow.size < 1e9:
            self.rotor_from_to[tcpflow.src][tcpflow.dst].append(tcpflow)
        else:
            self.cache_from_to[tcpflow.src][tcpflow.dst].append(tcpflow)

    def print_demand(self):
        if self.verbose:
            print_demand(self.tors)
