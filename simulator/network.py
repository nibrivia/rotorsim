import random
import sys
from math import ceil, floor
from helpers import *
from tor_switch import ToRSwitch
from rotor_switch import RotorSwitch
from event import Registry, Delay, stop_simulation, R

class RotorNet:
    def __init__(self,
                 n_tor,
                 packets_per_slot,
                 n_switches,
                 n_cache    = None,
                 n_xpand    = None,
                 slice_duration = 1, 
                 reconfiguration_time = 0, 
                 jitter = 0,
                 logger = None,
                 verbose = True, 
                 do_pause = True):

        # Network config
        self.n_tor   = n_tor

        self.n_switches = n_switches

        if n_xpand is not None:
            assert n_xpand < n_switches
            self.n_xpand = n_xpand
        else:
            self.n_xpand = 1 #round(min(5, n_switches/3))

        if n_cache is not None:
            assert n_cache + self.n_xpand < n_switches
            assert n_cache < n_switches
            self.n_cache = n_cache
        else:
            self.n_cache = floor((n_switches - self.n_xpand) / 2)

        self.n_rotor = n_switches - self.n_xpand - self.n_cache

        print("%d xpander, %d rotor, %d cache. %d total" %
                (self.n_xpand, self.n_rotor, self.n_cache, self.n_switches))

        # Matchings need to be done early to get constants
        self.generate_matchings()
        self.n_slots = ceil(len(self.matchings) / self.n_rotor)

        # TODO switch for Opera
        self.slice_duration = slice_duration
        self.slot_duration  = slice_duration#*self.n_rotor
        self.cycle_duration = self.slot_duration * self.n_slots

        # Internal variables
        self.switches = [RotorSwitch(
                            id = i,
                            n_ports  = n_tor,
                            verbose = verbose,
                            logger  = logger)
                for i in range(self.n_switches)]

        self.tors = [ToRSwitch(
                            name    = i,
                            n_tor   = n_tor,
                            n_xpand = self.n_xpand,
                            n_rotor = self.n_rotor,
                            n_cache = self.n_cache,
                            packets_per_slot = packets_per_slot,
                            slot_duration = slice_duration,
                            clock_jitter  = jitter,
                            logger  = logger,
                            verbose = verbose)
                for i in range(n_tor)]

        for tor in self.tors:
            tor.set_tor_refs(self.tors)

        # Physically connect them up
        for s in self.switches:
            s.connect_tors(self.tors)

        # ROTORNET
        ##########

        # We can now associate the matchings with objects
        self.matchings = [[(self.tors[src], self.tors[dst]) for src, dst in m]
                for m in self.matchings]


        # This is what we'll distribute
        self.matchings_by_slot_rotor = []
        for slot in range(self.n_slots):
            slot_matchings = []
            for rotor_id in self.rotor_ports:
                rotor = self.switches[rotor_id]

                matching_i = (slot + rotor.id*self.n_slots) % \
                        len(self.matchings)
                rotor_matchings = self.matchings[matching_i]
                slot_matchings.append(rotor_matchings)
            self.matchings_by_slot_rotor.append(slot_matchings)


        # Distribute to rotors
        for rotor_id in self.rotor_ports:
            rotor = self.switches[rotor_id]
            rotor_matchings_by_slot = [
                    self.matchings_by_slot_rotor[slot][rotor_id]
                            for slot in range(self.n_slots)]
            rotor.add_matchings(rotor_matchings_by_slot, self.n_rotor)
        # Distribute to ToRs
        for tor in self.tors:
            tor.add_rotor_matchings(self.matchings_by_slot_rotor)

        # EXPANDER
        ##########
        for i, xpand_id in enumerate(self.xpand_ports):
            # Install one matching per switch, never changes
            xpand = self.switches[xpand_id]
            xpand_matchings = [self.matchings[i]]
            xpand.add_matchings(xpand_matchings, 1)
        for tor in self.tors:
            all_xpand_matchings = zip(self.xpand_ports,
                    (self.matchings[i] for i, _ in enumerate(self.xpand_ports)))
            tor_xpand_matchings = {xpand_id: [d for s, d in m if s == tor][0]
                    for xpand_id, m in all_xpand_matchings}
            tor.add_xpand_matchings(tor_xpand_matchings)


        # CACHE
        #######
        for cache_id in self.cache_ports:
            # Start with a default, this will be changing though...
            cache = self.switches[cache_id]
            cache_matchings = [self.matchings[0]]
            cache.add_matchings(cache_matchings, 1)

        # I/O stuff
        self.verbose  = verbose
        self.do_pause = do_pause

    @property
    def rotor_ports(self):
        for port_id in range(self.n_rotor):
            yield port_id

    @property
    def xpand_ports(self):
        for i in range(self.n_xpand):
            yield self.n_rotor + i

    @property
    def cache_ports(self):
        for i in range(self.n_cache):
            yield self.n_rotor + self.n_xpand + i

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
        for s_id, s in enumerate(self.switches):
            if s_id < self.n_rotor:
                s.start(slice_duration = self.slice_duration)
            else:
                s.start(slice_duration = float("Inf"))
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
        self.tors[flow.src].recv_flow(flow)

        # begin sending
        #tcpflow.send()

    def print_demand(self):
        if self.verbose:
            print_demand(self.tors)
