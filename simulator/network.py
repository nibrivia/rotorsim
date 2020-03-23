import random
import sys
from logger import LOG
from math import ceil, floor
from helpers import vprint, xpand_ports, rotor_ports, cache_ports
from switch import NIC
from tor_switch import ToRSwitch
from rotor_switch import RotorSwitch
from optical_switch import OpticalSwitch
from event import Registry, Delay, stop_simulation, R
from xpand_108 import xpand1
from params import PARAMS
from server import Server
from flow_generator import FLOWS, N_DONE, N_FLOWS

class RotorNet:
    def __init__(self):
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

        # Switches
        self.switches = []
        self.switches.extend([  RotorSwitch(id = r_id) for r_id in rotor_ports])
        self.switches.extend([OpticalSwitch(id = x_id) for x_id in xpand_ports])
        self.switches.extend([OpticalSwitch(id = c_id) for c_id in cache_ports])

        # ToRs
        self.tors = [ToRSwitch(name = i) for i in range(PARAMS.n_tor)]

        # Servers
        PARAMS.servers_per_rack = 5
        self.servers = [Server(
                    server_id = rack_id*PARAMS.servers_per_rack + rack_slot,
                    server_name = "tor%s.%s" % (rack_id, rack_slot))
                for rack_id   in range(PARAMS.n_tor)
                for rack_slot in range(PARAMS.servers_per_rack)
                ]

        # "Physically" connect them up

        # ToR <> backbone
        for s in self.switches:
            for t in self.tors:
                uplink   = NIC(
                        s.make_recv(t.id),
                        name = "%s->%s" % (t, s),
                        delay = .001,
                        bandwidth_Bms = PARAMS.bandwidth_Bms,
                        max_size_bytes = 40e6,
                        )
                t.connect_backbone(s.id, s, uplink)
            downlinks = [NIC(
                    t.recv,
                    name = "%s->%s" % (s, t),
                    delay = .001,
                    bandwidth_Bms = PARAMS.bandwidth_Bms,
                    max_size_bytes = 40e6,
                    )
                    for t in self.tors]

            s.connect_tors(downlinks)

        # ToR <> ToR? TODO check if needed
        for tor in self.tors:
            tor.set_tor_refs(self.tors)

        # ToR <> servers
        for rack_id, tor in enumerate(self.tors):
            rack_offset = rack_id * PARAMS.servers_per_rack
            for rack_slot in range(PARAMS.servers_per_rack):
                server_ix = rack_offset + rack_slot
                server = self.servers[server_ix]

                # Create links
                # TODO capacity, latency, bandwidth
                uplink   = NIC(
                        tor.recv,
                        name = "%s->%s" % (server, tor),
                        delay = .001,
                        bandwidth_Bms = PARAMS.bandwidth_Bms,
                        max_size_bytes = 40e6,
                        )
                downlink = NIC(
                        server.recv,
                        name = "%s->%s" % (tor, server),
                        delay = .001,
                        bandwidth_Bms = PARAMS.bandwidth_Bms,
                        max_size_bytes = 40e6,
                        )

                server.connect_tor(uplink)
                tor.connect_server(server, downlink)




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
                self.switches[xpand_id].add_matchings(matching)
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
        #for cache_id in cache_ports:
            ## Start with a default, this will be changing though...
            #cache = self.switches[cache_id]
            #cache_matchings = [self.matchings[0]]
            #cache.add_matchings(cache_matchings, 1)


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
        flow = next(flow_gen)
        # make sure this isn't the first thing we do
        R.call_at(flow.arrival, self.open_connection, flow, priority=-1)

        # Register first events
        for s_id, s in enumerate(self.switches):
            s.start()
        for t in self.tors:
            t.start()

        # Start events
        if not PARAMS.arrive_at_start:
            R.limit = time_limit
        R.run_next()

    @staticmethod
    def del_flow(flow_id):
        global FLOWS, N_DONE
        vprint("%s done!" % (FLOWS[flow_id]))
        flow = FLOWS[flow_id]
        flow.end = R.time
        LOG.log_flow_done(FLOWS[flow_id])

        N_DONE[0] += 1
        del FLOWS[flow_id]

    def open_connection(self, flow):
        if flow is not None:
            # Server -> flow
            self.servers[flow.src].add_flow(flow, flow.src_recv)
            self.servers[flow.dst].add_flow(flow, flow.dst_recv)

            # Flow -> server
            flow.add_src_send(self.servers[flow.src].uplink)
            flow.add_dst_send(self.servers[flow.dst].uplink)

            # Global book-keeping
            global FLOWS, N_FLOWS
            FLOWS[flow.id] = flow
            N_FLOWS[0] += 1
            flow.add_callback_done(self.del_flow)

            # Actually start things...
            flow.start()

        try:
            flow = next(self.flow_gen)
            R.call_at(flow.arrival, priority = -1,
                    fn = self.open_connection, flow = flow)
        except:
            # No more flows
            pass

    #def print_demand(self):
    #    if PARAMS.verbose:
    #        print_demand(self.tors)
