import random
import sys
from logger import LOG
from math import ceil, floor
from helpers import *
from tor_switch import ToRSwitch
from rotor_switch import RotorSwitch
from event import Registry, Delay, stop_simulation, R
from xpand_108 import xpand1

class RotorNet:
    def __init__(self):
                 #n_tor,
                 #packets_per_slot,
                 #n_switches,
                 #arrive_at_start,
                 #n_cache    = None,
                 #n_xpand    = None,
                 #slice_duration = 1, 
                 #reconfiguration_time = .1, 
                 #jitter = 0,
                 #verbose = True, 
                 #do_pause = True):

        # Network config

        # Matchings need to be done early to get constants
        self.generate_matchings()
        if PARAMS.n_rotor > 0:
            PARAMS.n_slots = ceil(len(self.matchings) / PARAMS.n_rotor)
        else:
            PARAMS.n_slots = 1

        #assert PARAMS.slot_duration is     None or PARAMS.slice_duration is     None
        #assert PARAMS.slot_duration is not None or PARAMS.slice_duration is not None
        PARAMS.reconf_cache   = 15 # TODO as argument

        #if PARAMS.slice_duration is not None:
            #self.packet_ttime   = PARAMS.slice_duration / PARAMS.packets_per_slot
        if PARAMS.slot_duration is not None:
            PARAMS.packet_ttime   = PARAMS.slot_duration  / PARAMS.packets_per_slot

        # Timings
        #self.slice_duration = slice_duration
        #self.slot_duration  = slice_duration#*self.n_rotor
        #self.cycle_duration = self.slot_duration * self.n_slots
        #self.reconf_time    = reconfiguration_time

        # Internal variables
        self.switches = [RotorSwitch(
                            id = i,
                            #n_ports  = n_tor,
                            tag = port_type(i),
                            #verbose = verbose
                            )
                for i in range(PARAMS.n_switches)]

        self.tors = [ToRSwitch(
                            name    = i)
                            #n_tor   = n_tor,
                            #n_xpand = self.n_xpand,
                            #n_rotor = self.n_rotor,
                            #n_cache = self.n_cache,
                            #packets_per_slot = packets_per_slot,
                            #slot_duration = slice_duration,
                            #reconfiguration_time = self.reconf_time,
                            #clock_jitter  = jitter,
                            #verbose = verbose)
                for i in range(PARAMS.n_tor)]

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
        for slot in range(PARAMS.n_slots):
            slot_matchings = []
            for rotor_id in rotor_ports:
                rotor = self.switches[rotor_id]

                matching_i = (slot + rotor.id*PARAMS.n_slots) % \
                        len(self.matchings)
                rotor_matchings = self.matchings[matching_i]
                slot_matchings.append(rotor_matchings)
            self.matchings_by_slot_rotor.append(slot_matchings)

        # Distribute to rotors
        for rotor_id in rotor_ports:
            rotor = self.switches[rotor_id]
            rotor_matchings_by_slot = [
                    self.matchings_by_slot_rotor[slot][rotor_id]
                            for slot in range(PARAMS.n_slots)]
            rotor.add_matchings(rotor_matchings_by_slot, PARAMS.n_rotor)

        # Distribute to ToRs
        for tor in self.tors:
            tor.add_rotor_matchings(self.matchings_by_slot_rotor)

        # EXPANDER
        ##########
        xpand_matchings = dict()
        if True:
            for i, xpand_id in enumerate(xpand_ports):
                found = False
                while not found:
                    tor_ids = [i for i in range(PARAMS.n_tor)]
                    random.shuffle(tor_ids)
                    rand_matching = [(i, j) for i, j in enumerate(tor_ids)]

                    found = True
                    for src, dst in rand_matching:
                        if src == dst:
                            found = False
                            break

                matching = [(self.tors[src], self.tors[dst]) for src, dst in enumerate(tor_ids)]
                xpand_matchings[xpand_id] = matching
                self.switches[xpand_id].add_matchings([matching], 1)
        else:
            for i, port_id in enumerate(xpand_ports):
                # Install one matching per switch, never changes
                m = xpand1[i]
                xpand_matchings[port_id] = [(src-1, dst-1) for src, dst in m]
                xpand_matchings[port_id].extend([(dst-1, src-1) for src, dst in m])
                xpand_matchings[port_id] = [x for x in sorted(xpand_matchings[port_id])]
                xpand_matchings[port_id] = [(self.tors[s], self.tors[d]) for s, d in xpand_matchings[port_id]]


                self.switches[port_id].add_matchings([xpand_matchings[port_id]], 1)


        for tor in self.tors:
            tor_xpand_matchings = {xpand_id: m[tor.id][1] for xpand_id, m in xpand_matchings.items()}
            tor.add_xpand_matchings(tor_xpand_matchings)


        # CACHE
        #######
        for cache_id in cache_ports:
            # Start with a default, this will be changing though...
            cache = self.switches[cache_id]
            cache_matchings = [self.matchings[0]]
            cache.add_matchings(cache_matchings, 1)


    def generate_matchings(self):
        self.matchings = []

        for offset in range(1, PARAMS.n_tor):
            # Compute the indices
            slot_matching = [(src_i, ((src_i+offset) % PARAMS.n_tor))
                    for src_i in range(PARAMS.n_tor)]

            # Add to the list of matchings
            self.matchings.append(slot_matching)


    def run(self, time_limit, flow_gen):
        """Run the simulation for n_cycles cycles"""
        self.flow_gen = flow_gen
        wait, flow = next(flow_gen)
        # make sure this isn't the first thing we do
        R.call_in(wait, self.open_connection, flow, priority=-1)

        # Register first events
        for s_id, s in enumerate(self.switches):
            if s_id < PARAMS.n_rotor:
                s.start(slice_duration = PARAMS.slot_duration,
                        reconf_time    = PARAMS.reconfiguration_time)
            else:
                s.start(slice_duration = float("Inf"))
        for t in self.tors:
            t.start()

        # Start events
        if not PARAMS.arrive_at_start:
            R.limit = time_limit
        R.run_next()

    def open_connection(self, flow):
        if flow is not None:
            self.tors[flow.src].recv_flow(flow)

        try:
            wait, flow = next(self.flow_gen)
            R.call_in(wait, priority = -1,
                    fn = self.open_connection, flow = flow)
        except:
            pass

    def print_demand(self):
        if PARAMS.verbose:
            print_demand(self.tors)
