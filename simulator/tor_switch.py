import math
import heapq
# from buffer import *
from logger import LOG
from helpers import get_port_type, rotor_ports, cache_ports, xpand_ports, vprint, color_str_
from event import Delay, R
from functools import lru_cache
from collections import deque
from flow_generator import FLOWS
from params import PARAMS
from debuglog import DebugLog


class ToRSwitch(DebugLog):
    def __init__(self, name):

        # Stuff about me
        self.id   = int(name)
        self.name = "Tor %d" % self.id

        # ... about others
        self.switches = [None for _ in range(PARAMS.n_switches)]
        self.local_dests = dict()

        # receiving tor, queues
        #self.ports_src = [None for _ in range(PARAMS.n_switches)]

        # transmit queue an dest
        self.ports_tx  = [None for _ in range(PARAMS.n_switches)]
        self.ports_dst = [None for _ in range(PARAMS.n_switches)]


        # TODO parametrize types of traffic
        tags = ["xpand", "rotor", "rotor-old", "cache"]
        self.buffers_dst_type = [{t: deque() for t in tags} for _ in range(PARAMS.n_tor)]
        self.buffers_dst_type_sizes = [{t: 0 for t in tags} for _ in range(PARAMS.n_tor)]
        self.available_ports = set()
        self.available_types = {t: 0 for t in tags}

        # rotor
        self.capacities = [0    for _ in range(PARAMS.n_tor)] # of destination
        self.capacity   = [PARAMS.packets_per_slot for _ in range(PARAMS.n_tor)]
        #self.out_queue_t = [-1 for rotor_id in rotor_ports]

        # xpander
        #self.connections = dict() # Destination ToRs
        self.dst_to_port = dict() # routing table
        self.dst_to_tor  = dict() # for virtual queue purposes
        self.tor_to_port = dict() # for individual moment decisions
        #self.port_to_tor = dict()

        self.t = [0]


    # One-time setup
    ################

    def connect_backbone(self, port_id, switch, queue):
        # queue is an object with a .recv that can be called with (packets)
        vprint("%s: %s connected on :%d" % (self, switch, port_id))
        self.switches[port_id] = switch
        self.ports_tx[port_id] = queue
        self.available_ports.add(port_id)
        self.available_types[get_port_type(port_id)] += 1
        queue.empty_callback = self.make_pull(port_id)


    def connect_server(self, server, queue):
        # This will be the next port_id
        self.local_dests[server.id] = len(self.ports_dst)

        self.ports_dst.append(server)
        self.ports_tx.append(queue)


    def add_rotor_matchings(self, matchings_by_slot_rotor):
        self.matchings_by_slot_rotor = [[ None for _ in m]
                for m in matchings_by_slot_rotor]
        for slot, matchings_by_rotor in enumerate(matchings_by_slot_rotor):
            assert len(matchings_by_rotor) == PARAMS.n_rotor, \
                    "Got %s, expected %s" % (
                            len(matchings_by_rotor), PARAMS.n_rotor)
            for rotor_id, matchings in enumerate(matchings_by_rotor):
                for src, dst in matchings:
                    if src.id == self.id:
                        self.matchings_by_slot_rotor[slot][rotor_id] = dst

        self.n_slots = len(matchings_by_slot_rotor)

    def add_xpand_matchings(self, xpand_matchings):
        assert len(xpand_matchings) == PARAMS.n_xpand

        self.xpand_matchings = xpand_matchings

        for port_id, dst_tor in xpand_matchings.items():
            self.ports_dst[port_id] = dst_tor
            self.tor_to_port[dst_tor.id] = port_id


    def set_tor_refs(self, tors):
        self.tors = tors

    def start(self):
        """Call once at setup"""
        # Rotor
        #######

        # This is the first time, we need to connect everyone
        slot_t = 0
        matchings_in_effect = self.matchings_by_slot_rotor[slot_t % PARAMS.n_slots]

        # For all active matchings, connect them up!
        for rotor_id in rotor_ports:
            dst = matchings_in_effect[rotor_id]
            self.connect_to(rotor_id, dst)

        # Set a countdown for the next slot, just like normal
        if PARAMS.slot_duration is not None:
            self.slot_id = 0
            self.new_slice = Delay(PARAMS.slot_duration  + PARAMS.reconfiguration_time, priority = 1000)(self.new_slice)
        #if PARAMS.slice_duration is not None:
        #    self.new_slice = Delay(self.slice_duration + self.reconf_time, priority = 1000)(self.new_slice)
        self.new_slice()
        self.make_route()
        R.call_in(0, self._send)

        # Expander
        ##########

        # This only iterates over the very beginning of the connections: the rotors
        # for port_id, tor in self.xpand_matchings.items():
        #     self.tor_to_port[tor.id] = port_id


    # Every slice setup
    ###################

    #@property
    #def slice_t(self):
        #assert PARAMS.slice_duration is not None
        #return round(R.time/(PARAMS.slice_duration + PARAMS.reconfiguration_time))

    @property
    def slot_t(self):
        assert PARAMS.slot_duration is not None
        return round(R.time/(PARAMS.slot_duration + PARAMS.reconfiguration_time))

    def new_slice(self):
        """Starts a new slice"""
        # Switch up relevant matching
        #if PARAMS.slice_duration is not None:
        #    slot_t = self.slice_t // PARAMS.n_rotor
        #    matchings_in_effect = self.matchings_by_slot_rotor[self.slot_t % self.n_slots]

        #    rotor_id = self.slice_t % PARAMS.n_rotor
        #    dst = matchings_in_effect[rotor_id]
        #    self.connect_to(rotor_id, dst)

        # If Rotor
        #vprint("%s: capacity %s" % (self, self.capacity))
        if PARAMS.slot_duration is not None:
            self.slot_id = self.slot_t % self.n_slots
            #vprint("%.3f %s switch to slot_id %d" % (R.time, self, self.slot_id))
            matchings_in_effect = self.matchings_by_slot_rotor[self.slot_id]
            for rotor_id in rotor_ports:
                dst = matchings_in_effect[rotor_id]
                self.connect_to(rotor_id, dst)

        # Set a countdown for the next slot
        self.new_slice() # is a delay() object

    #@Delay(0, priority = 10)
    def connect_to(self, port_id, tor):
        """This gets called for every rotor and starts the process for that one"""
        # Set the connection
        #vprint("%s:%d -> %s" % (self, port_id, tor))
        self.ports_dst[port_id] = tor
        R.call_in(0, self.ports_tx[port_id].resume)
        R.call_in(PARAMS.slot_duration - .002,
                self.disconnect_from, port_id, priority = -1)

        # Get capacities for indirection if rotor
        if port_id < PARAMS.n_rotor:
            self.capacities[port_id] = tor.capacity

        # Start sending
        #self._send(port_id)

    def disconnect_from(self, port_id):
        self.ports_tx[port_id].pause()
        if port_id in self.available_ports:
            self.available_ports.remove(port_id)
            self.available_types["rotor"] -= 1
        #vprint("%s: available ports: %s" % (self, self.available_ports))



    @property
    @lru_cache(maxsize = None)
    def link_state(self):
        links = dict()
        for port_id in xpand_ports:
            tor = self.ports_dst[port_id]
            links[tor.id] = 1
        return links

    # By having a delay 0 here, this means that every ToR will have gone
    # through its start, which will then mean that we can call link_state
    @Delay(0, priority = -10)
    def make_route(self, slice_id = None):
        """Builds a routing table"""
        for t in self.tors:
            for dst_id in t.local_dests:
                self.dst_to_tor[dst_id] = t.id

        if PARAMS.n_xpand == 0:
            return

        self.route_tor = [(None, PARAMS.n_tor*1000) for _ in range(PARAMS.n_tor)]
        self.route_tor[self.id] = ([], 0)
        queue = deque()
        queue.append(self)

        #This is a bastardized dijkstra - it assumes all cost are one
        while len(queue) > 0:
            tor    = queue.popleft()
            path, cost = self.route_tor[tor.id]

            # Take the new connection...
            for con_id in tor.link_state:
                cur_path, cur_cost = self.route_tor[con_id]
                con_tor = self.tors[con_id]
                # see if it does better...
                if cost+1 < cur_cost:
                    # update the cost and add back to the queue
                    self.route_tor[con_id] = (path + [con_id], cost+1)
                    queue.append(con_tor)

        self.route = dict()
        self.possible_tor_dsts = dict()
        for dst_tor_id, (path, _) in enumerate(self.route_tor):
            # Local destination, skip
            if dst_tor_id == self.id:
                continue

            # Figure out what the next path is...
            try:
                next_tor = path[0]
            except:
                print()
                print(self.route_tor)
                print(self, dst_tor_id, path)
                print()
                raise
            next_port_id = self.tor_to_port[next_tor]

            # Write that for each server at our destination
            dst_tor = self.tors[dst_tor_id]
            for dst in dst_tor.local_dests:
                # This is just for expander
                self.dst_to_port[dst] = next_port_id



    # SENDING ALGORITHMS
    ####################
    def next_queue(self, port_id):
        p_type = get_port_type(port_id)
        if p_type == "rotor":
            return self.next_queue_rotor(port_id)
        if p_type == "xpand":
            return self.next_queue_xpand(port_id)
        if p_type == "cache":
            return self.next_queue_cache(port_id)

    def cache_port_for_flow(self, flow):
        for port in cache_ports:
            # Port already busy
            if self.active_flow[port] is not None:
                continue

            switch = self.switches[port]
            if switch.request_matching(self, flow.dst):
                return switch

    def next_queue_cache(self, port_id):
        switch = self.switches[port_id]

        # Do the current flow
        f = self.active_flow[port_id]
        if f is not None:
            return None # We're not simulating at the packet level

        # Or try to establish a new one....
        for i, f in enumerate(self.flows_cache):
            if switch.request_matching(self, f.dst):
                #print("@%.3f %s got matching" % (R.time, f))
                vprint("\033[0;33mflow %d start (%s)\033[00m" % (f.id, f.tag))

                # Get the flow
                self.ports_dst[port_id] = self.tors[f.dst]
                self.flows_cache.pop(i)

                # Figure out how long it takes
                fct = f.remaining_packets * PARAMS.packet_ttime
                n_packets = f.size_packets
                time_left = R.limit - R.time - PARAMS.reconf_cache

                # Update book-keeping
                self.active_flow[port_id] = f

                # Make sure end-of-simulation gets handled gracefully
                if fct > time_left:
                    n_packets = math.floor(time_left/fct * n_packets)
                    if n_packets < 0:
                        return
                    fct = n_packets * PARAMS.packet_ttime

                # Get the packets from the flow
                lump = f.pop_lump(n_packets)

                # Come back when we're done
                R.call_in(PARAMS.reconf_cache + fct,
                        self.cache_flow_done,
                        port_id = port_id, lump = lump)

                return None # Still not simulating packet level

    def cache_flow_done(self, port_id, lump):
        vprint("\033[0;33mflow", self.active_flow[port_id].id, "is done (cache)")

        # Figure out who was done
        flow_id, dst, n = lump
        FLOWS[flow_id].rx(n)

        # Reset book-keeping
        self.active_flow[port_id] = None

        # Release the cache
        self.switches[port_id].release_matching(self)

    def next_packet_rotor(self, port_id, dst_tor_id):
        """Sends over a lump"""
        # Check if we've computed this before
        #queue_t  = self.out_queue_t[port_id]
        #if queue_t == self.slot_id:
            #return None

        # Get connection info
        #rotor_id = port_id # TODO translate this
        #dst_q = self.ports_rx[rotor_id]
        dst   = self.tors[dst_tor_id]

        # Old indirect traffic goes first
        
        old_queue = self.buffers_dst_type[dst_tor_id]["rotor-old"]
        if self.buffers_dst_type_sizes[dst_tor_id]["rotor-old"] > 0:
            self.capacity[dst_tor_id] += 1
            return old_queue.popleft()
        #q = self.lumps_ind[dst_id]
        #remaining = PARAMS.packets_per_slot - self.lumps_ind_n[dst_id]
        #self.capacity[dst_id] += self.lumps_ind_n[dst_id]
        #self.lumps_ind[dst_id]   = []
        #self.lumps_ind_n[dst_id] = 0

        #assert self.buffers_ind[dst_id].size == 0
        #assert remaining >= 0, "@%.3f %s:%d->%s: %s remaining, q: %s (capacity %s)" % (
        #        R.time, self, port_id, dst, remaining, q, str(self.capacity))

        # Direct traffic
        #to_pop = 0

        dir_queue = self.buffers_dst_type[dst_tor_id]["rotor"]
        if self.buffers_dst_type_sizes[dst_tor_id]["rotor"] > 0:
            self.capacity[dst_tor_id] += 1
            return dir_queue.popleft()

        for ind_target in range(PARAMS.n_tor):
            new_queue = self.buffers_dst_type[ind_target]["rotor"]
            #if ind_target == dst_tor_id: # Should already be empty if we're here...
            #    assert len(new_queue) == 0

            if self.buffers_dst_type_sizes[dst_tor_id]["rotor"] > 0 and dst.capacity[ind_target] > 0:
                #vprint("%s: sending indirect %s.capacity[%s] = %s" % 
                        #(self, dst_tor_id, dst.capacity)
                self.capacity[ind_target] += 1
                return new_queue.popleft()

        #for f in self.flows_rotor[dst_id]:
        #    if remaining < f.remaining_packets:
        #        p = f.pop_lump(remaining)
        #        q.append(p)
        #        remaining = 0
        #        break
        #    else:
        #        p = f.pop_lump(f.remaining_packets)
        #        q.append(p)
        #        to_pop += 1

        #self.n_flows -= to_pop
        #for _ in range(to_pop):
        #    self.flows_rotor[dst_id].pop(0)
        #self.flows_rotor[dst_id] = [f for f in self.flows_rotor[dst_id] if f.remaining_packets > 0]

        return None
        # New indirect traffic
        # TODO should actually load balance
        #delta = 1
        #aggregate = dict()
        #while remaining > 0 and delta > 0 and self.n_flows > 0:
        #    delta = 0
        #    for final_dst_tor_id, tor in enumerate(self.tors):
        #        if dst.capacity[final_dst_tor_id] <= 0:
        #            continue
        #        if len(self.flows_rotor[final_dst_tor_id]) == 0:
        #            continue

        #        f = self.flows_rotor[final_dst_tor_id][0]

        #        cur_n = aggregate.get(f.id, 0)
        #        aggregate[f.id] = cur_n+1
        #        remaining -= 1
        #        delta += 1
        #        dst.capacity[final_dst_tor_id] -= 1
        #        self.capacity[final_dst_tor_id] += 1

        #        if cur_n+1 == f.remaining_packets:
        #            self.flows_rotor[final_dst_tor_id].pop(0)
        #            self.n_flows -= 1
        #        if remaining == 0:
        #            break

        #for fid, n in aggregate.items():
        #    lump = FLOWS[fid].pop_lump(n)
        #    q.append(lump)

        #self.out_queue_t[port_id] = self.slot_id
        #dst_q.recv((dst_id, port_id, self.slot_t, q))

    #@Delay(0, priority = 100) #do last
    #def rx_rotor(self, lumps):
    #    t = R.time
    #    for l in lumps:
    #        flow, dst, n = l
    #        t += n*PARAMS.packet_ttime

    #        if self.id == dst:
    #            FLOWS[flow].rx(n=n, t=t)
    #        else:
    #            self.lumps_ind[dst].append(l)
    #            self.lumps_ind_n[dst] += n

    def next_packet_xpand(self, port_id, dst_tor_id):
        """Given a connection to a certain destination, give a packet
        that we can either shortcut, or is equivalent, to something we'd
        normally do on expander..."""
        # TODO this whole thing is grossly inefficient...

        assert self.ports_dst[port_id].id == dst_tor_id 

        # Get destinations that go that way
        #vprint("%s: xpand :%s -> Tor #%s" % (self, port_id, dst_id))
        #vprint(self.route_tor)
        if dst_tor_id in self.possible_tor_dsts:
            possible_tor_dsts = self.possible_tor_dsts[dst_tor_id]
        else:
            if PARAMS.n_xpand > 0:
                possible_tor_dsts = set(
                        tor_id
                        for tor_id, (path, _) in enumerate(self.route_tor)
                                if dst_tor_id in path)
                #possible_tor_dsts = set(self.dst_to_tor[dst]
                        #for dst, p in self.dst_to_port.items() if p == port_id)
            else:
                # No expander, literally anything is better...
                possible_tor_dsts = set(t for t in range(PARAMS.n_tor))

            self.possible_tor_dsts[dst_tor_id] = possible_tor_dsts
        #vprint(possible_tor_dsts)

        # Get all packets that wanna go that way
        possible_pkts = []
        for d in possible_tor_dsts:
            if self.buffers_dst_type_sizes[d]["xpand"] > 0:
                possible_pkts.append((d, self.buffers_dst_type[d]["xpand"][0]))

        # Find the earliest one
        if len(possible_pkts) > 0:
            dst, pkt = min(possible_pkts, key = lambda t: t[1]._tor_arrival)
            pkt = self.buffers_dst_type[dst]["xpand"].pop()
            return pkt







    # Actual packets moving
    ########################

    def make_pull(self, port_id):
        port_type = get_port_type(port_id)
        def pull():
            #vprint("%s: pull from port %s" % (self, port_id))
            self.available_ports.add(port_id)
            self.available_types[port_type] += 1
            self._send()
        return pull

    def activate_cache_link(self, port_id, dst_tor_id):
        vprint("%s: activate :%d -> %s" % (self, port_id, dst_tor_id))
        self.ports_dst[port_id] = self.tors[dst_tor_id]
        self._send()

    def recv(self, packet):
        """Receives packets for `port_id`"""

        if packet.flow_id == PARAMS.flow_print:
            vprint("%s: %s recv" % (self, packet))

        # Sanity check
        if packet.intended_dest != None:
            assert packet.intended_dest == self.id, \
                "@%.3f %s received %s, was intendd for %s" % (R.time, self, packet, packet.intended_dest)

        # Update hop count
        packet.hop_count += 1
        assert packet.hop_count < 1000, "Hop count >1000? %s" % packet


        # Deliver locally
        if packet.dst_id in self.local_dests:
            if packet.flow_id == PARAMS.flow_print:
                vprint("%s: %s Local destination" % (self, packet))

            next_port_id = self.local_dests[packet.dst_id]
            self.ports_tx[next_port_id].enq(packet)
        else:
            packet._tor_arrival = R.time
            next_tor_id = self.dst_to_tor[packet.dst_id]

            # CACHE handling
            for port_id in cache_ports:
                if self.ports_dst[port_id] is None:
                    if self.switches[port_id].request_matching(self, next_tor_id):
                        R.call_in(15, self.activate_cache_link, port_id, next_tor_id)
                        break


            # ROTOR requires some handling...
            # ...adapt our capacity on rx
            if packet.tag == "rotor":
                self.capacity[next_tor_id] -= 1

            # ... if indirect, put it in higher queue...
            dst_tag = packet.tag
            if packet.tag == "rotor" and packet.src_id not in self.local_dests:
                dst_tag = "rotor-old"

            self.buffers_dst_type[next_tor_id][dst_tag].append(packet)
            self.buffers_dst_type_sizes[next_tor_id][dst_tag] += 1


            # debug print
            if packet.flow_id == PARAMS.flow_print:
                vprint("%s: %s Outer destination %s/%s (%d)" % (
                    self, packet, next_tor_id, packet.tag,
                    len(self.buffers_dst_type[next_tor_id][packet.tag])))

            # trigger send loop
            buf = self.buffers_dst_type[next_tor_id][dst_tag]
            sz  = self.buffers_dst_type_sizes[next_tor_id][dst_tag]
            #assert len(buf) == sz, "%s: recv buffer[%s][%s] size %s, recorded %s" % (self, next_tor_id, dst_tag, len(buf), sz)
            self._send()



    def _send(self):
        #vprint("%s: _send()" % self)
        priorities = dict(
                xpand = ["xpand", "rotor", "cache"],
                rotor = ["rotor", "xpand", "cache"],
                cache = ["cache", "xpand", "rotor"],
                )
        pull_fns = dict(
                xpand = self.next_packet_xpand,
                rotor = self.next_packet_rotor,
                #cache = self.next_packet_cache
                )

        #vprint("%s: available ports: %s" % (self, self.available_ports))
        for free_port in list(self.available_ports):
            port_type = get_port_type(free_port)
            dst = self.ports_dst[free_port]
            if dst is None:
                continue
            port_dst  = self.ports_dst[free_port].id
            buffers_type = self.buffers_dst_type[port_dst]

            for priority_type in priorities[port_type]:
                buf = buffers_type[priority_type]
                sz  = self.buffers_dst_type_sizes[port_dst][priority_type]
                #assert len(buf) == sz, "%s: buffer[%s][%s] size %s, recorded %s" % (self, port_dst, priority_type, len(buf), sz)

                if False:
                    vprint("%s:   considering :%s/%s %s/%s (%d)..." % (
                            self,
                            free_port, port_type,
                            port_dst, priority_type,
                            sz
                            #end = ""
                            ))

                pkt = None
                if priority_type in pull_fns:
                    # Eventually should all be here, for now, not all implemented...
                    pkt = pull_fns[priority_type](port_id = free_port, dst_tor_id = port_dst)
                elif sz > 0:
                    #vprint(" has packets!")
                    pkt = buf.popleft()

                if pkt is not None:
                    pkt.intended_dest = port_dst
                    if pkt.flow_id == PARAMS.flow_print:
                        vprint("%s: sending %s on :%s -> %s" % (self, pkt, free_port, port_dst))
                    self.ports_tx[free_port].enq(pkt)
                    self.available_ports.remove(free_port)
                    self.available_types[port_type] -= 1
                    pkt_tor_dst = self.dst_to_tor[pkt.dst_id]
                    self.buffers_dst_type_sizes[pkt_tor_dst][pkt.tag] -= 1
                    break




        # expander: use the routing table
        #elif packet.tag == "xpand" and PARAMS.n_xpand > 0:
        #    if packet.flow_id == PARAMS.flow_print:
        #        vprint("%s: %s xpand destination" % (self, packet))
        #    next_port_id = self.dst_to_port[packet.dst_id]  # Use routing table

        ## rotor: figure out 1st/2nd hop and adjust
        #elif packet.tag == "rotor":
        #    # Add to rotor queue
        #    if packet.flow_id == PARAMS.flow_print:
        #        vprint("%s: %s rotor destination" % (self, packet))
        #    #self.rotor_queue.enq(packet)
        #    self.port_tx[0].enq(packet)
        #    return

        ## cache: TODO attempt to send it on cache, fallback on rotor
        #elif packet.tag == "cache":
        #    if packet.flow_id == PARAMS.flow_print:
        #        vprint("%s: %s Cache destination" % (self, packet))
        #    self.cache_queue.enq(packet)
        #    return



    # Printing stuffs
    ################

    @color_str_
    def __str__(self):
        self.t[0] = 1

        return self.name

    def buffer_str(self):
        s = "\n" + str(self)
        s += "\nOld Indirect\n  "
        for dst, b in enumerate(self.buffers_ind):
            s += "%2d " % b.size
        s += "/%d" % self.packets_per_slot
        return s

        #s += "\nDirect\n  "
        #for dst, b in enumerate(self.buffers_dir):
        #    s += "%2d " % b.size

        #s += "\nReceived\n  "
        #for src, b in enumerate(self.buffers_rcv):
        #    s += "%2d " % b.size

        #return s

