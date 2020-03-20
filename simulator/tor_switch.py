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
from switch import QueueLink


class ToRSwitch:
    def __init__(self, name):

        # Stuff about me
        self.id   = int(name)
        self.name = "Tor %d" % self.id

        # ... about others
        self.switches = [None for _ in range(PARAMS.n_switches)]
        self.local_dests = dict()

        # receiving tor, queues
        self.ports_rx  = [QueueLink(
                                    self.recv,
                                    name  = "%s:%2d" % (self, port_id),
                                    delay = 0, #TODO
                                    bandwidth_Bms = None, # TODO
                                    max_size_bytes = None,
                                    )
                for port_id in range(PARAMS.n_switches)] # Just the queue
        self.ports_src = [None for _ in range(PARAMS.n_switches)]

        # transmit queue an dest
        self.ports_tx  = [None for _ in range(PARAMS.n_switches)]
        self.ports_dst = [None for _ in range(PARAMS.n_switches)]


        # rotor
        self.capacities = [0    for _ in range(PARAMS.n_tor)] # of destination
        self.capacity   = [PARAMS.packets_per_slot for _ in range(PARAMS.n_tor)]
        self.out_queue_t = [-1 for rotor_id in rotor_ports]
        self.rotor_queue = QueueLink(lambda p : None)
        self.cache_queue = QueueLink(lambda p : None)
        #self.n_flows = 0

        # xpander
        self.connections = dict() # Destination ToRs
        self.dst_to_port = dict() # routing table
        #self.port_to_tor = dict()


    # One-time setup
    ################

    def connect_backbone(self, port_id, switch, queue):
        # queue is an object with a .recv that can be called with (packets)
        self.switches[port_id] = switch

        if get_port_type(port_id) == "xpand":
            self.connections[port_id] = queue

        self.ports_rx[port_id] = queue

        return None

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

        # Expander
        ##########

        # This only iterates over the very beginning of the connections: the rotors
        # for port_id, tor in self.xpand_matchings.items():
        #     self.tor_to_port[tor.id] = port_id


    # Every slice setup
    ###################

    @property
    def slice_t(self):
        assert PARAMS.slice_duration is not None
        return round(R.time/(PARAMS.slice_duration + PARAMS.reconfiguration_time))

    @property
    def slot_t(self):
        assert PARAMS.slot_duration is not None
        return round(R.time/(PARAMS.slot_duration + PARAMS.reconfiguration_time))

    def new_slice(self):
        # Switch up relevant matching
        #if PARAMS.slice_duration is not None:
        #    slot_t = self.slice_t // PARAMS.n_rotor
        #    matchings_in_effect = self.matchings_by_slot_rotor[self.slot_t % self.n_slots]

        #    rotor_id = self.slice_t % PARAMS.n_rotor
        #    dst = matchings_in_effect[rotor_id]
        #    self.connect_to(rotor_id, dst)

        # If Rotor
        if PARAMS.slot_duration is not None:
            self.slot_id = self.slot_t % self.n_slots
            #vprint("%.3f %s switch to slot_id %d" % (R.time, self, self.slot_id))
            matchings_in_effect = self.matchings_by_slot_rotor[self.slot_id]
            for rotor_id in rotor_ports:
                dst = matchings_in_effect[rotor_id]
                self.connect_to(rotor_id, dst)

        # Set a countdown for the next slot
        self.new_slice() # is a delay() object

    @Delay(0, priority = 10)
    def connect_to(self, port_id, tor):
        """This gets called for every rotor and starts the process for that one"""
        # Set the connection
        self.ports_dst[port_id] = tor

        # Get capacities for indirection if rotor
        if port_id < PARAMS.n_rotor:
            self.capacities[port_id] = tor.capacity

        # Start sending
        #self._send(port_id)


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
        self.route = [(None, PARAMS.n_tor*1000) for _ in range(PARAMS.n_tor)]
        self.route[self.id] = ([], 0)
        queue = deque()
        queue.append(self)

        #This is a bastardized dijkstra - it assumes all cost are one
        while len(queue) > 0:
            tor    = queue.popleft()
            path, cost = self.route[tor.id]

            # Take the new connection...
            for con_id in tor.link_state:
                cur_path, cur_cost = self.route[con_id]
                con_tor = self.tors[con_id]
                # see if it does better...
                if cost+1 < cur_cost:
                    # update the cost and add back to the queue
                    self.route[con_id] = (path + [con_id], cost+1)
                    queue.append(con_tor)


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
                R.call_in(PARAMS.reconf_cache + fct, self.cache_flow_done, port_id = port_id, lump = lump)

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

    def next_queue_rotor(self, port_id):
        """Sends over a lump"""
        # Check if we've computed this before
        queue_t  = self.out_queue_t[port_id]
        if queue_t == self.slot_id:
            return None

        # Get connection info
        rotor_id = port_id # TODO translate this
        dst_q = self.ports_rx[rotor_id]
        dst   = self.ports_dst[rotor_id]

        # Old indirect traffic goes first
        q = self.lumps_ind[dst.id]
        remaining = PARAMS.packets_per_slot - self.lumps_ind_n[dst.id]
        self.capacity[dst.id] += self.lumps_ind_n[dst.id]
        self.lumps_ind[dst.id]   = []
        self.lumps_ind_n[dst.id] = 0

        #assert self.buffers_ind[dst.id].size == 0
        assert remaining >= 0, "@%.3f %s:%d->%s: %s remaining, q: %s (capacity %s)" % (
                R.time, self, port_id, dst, remaining, q, str(self.capacity))

        # Direct traffic
        to_pop = 0
        for f in self.flows_rotor[dst.id]:
            if remaining < f.remaining_packets:
                p = f.pop_lump(remaining)
                q.append(p)
                remaining = 0
                break
            else:
                p = f.pop_lump(f.remaining_packets)
                q.append(p)
                to_pop += 1

        self.n_flows -= to_pop
        for _ in range(to_pop):
            self.flows_rotor[dst.id].pop(0)
        #self.flows_rotor[dst.id] = [f for f in self.flows_rotor[dst.id] if f.remaining_packets > 0]

        # New indirect traffic
        # TODO should actually load balance
        delta = 1
        aggregate = dict()
        while remaining > 0 and delta > 0 and self.n_flows > 0:
            delta = 0
            for flow_dst_id, tor in enumerate(self.tors):
                if dst.capacity[flow_dst_id] <= 0:
                    continue
                if len(self.flows_rotor[flow_dst_id]) == 0:
                    continue

                f = self.flows_rotor[flow_dst_id][0]

                cur_n = aggregate.get(f.id, 0)
                aggregate[f.id] = cur_n+1
                remaining -= 1
                delta += 1
                dst.capacity[flow_dst_id] -= 1
                self.capacity[flow_dst_id] += 1

                if cur_n+1 == f.remaining_packets:
                    self.flows_rotor[flow_dst_id].pop(0)
                    self.n_flows -= 1
                if remaining == 0:
                    break

        for fid, n in aggregate.items():
            lump = FLOWS[fid].pop_lump(n)
            q.append(lump)

        self.out_queue_t[port_id] = self.slot_id
        dst_q.recv((dst.id, port_id, self.slot_t, q))

    @Delay(0, priority = 100) #do last
    def rx_rotor(self, lumps):
        t = R.time
        for l in lumps:
            flow, dst, n = l
            t += n*PARAMS.packet_ttime

            if self.id == dst:
                FLOWS[flow].rx(n=n, t=t)
            else:
                self.lumps_ind[dst].append(l)
                self.lumps_ind_n[dst] += n

    def next_queue_xpand(self, port_id):
        """Gets the next queue for xpander"""

        # If there are already packets waiting
        if self.buffers_fst[port_id].size > 0:
            return self.buffers_fst[port_id]

        # If we have flows waiting
        flows = self.flows_xpand[port_id]
        if len(flows) > 0:
            _, _, f = flows[0]

            # Remove if we're done
            if f.remaining_packets == 1:
                heapq.heappop(self.flows_xpand[port_id])

            return f


    # Actual packets moving
    ########################

    #@lru_cache(maxsize=None)
    #def packet_lag(self, p):
        #return PARAMS.packet_ttime / BYTES_PER_PACKET * p

    def _enable_out(self, port_id):
        self.out_enable[port_id] = True
        # We're done transmitting, try again
        self._send(port_id)

    def recv(self, packet):
        """Receives packets for `port_id`"""
        #vprint("%s rx %s" % (self, packet))

        if packet.flow_id == PARAMS.flow_print:
            vprint("%s: %s recv" % (self, packet))

        if packet.tag == "xpand" and PARAMS.n_xpand == 0:
            packet.tag = "rotor"


        # Sanity check
        if packet.intended_dest != None:
            assert packet.intended_dest == self.id, \
                "@%.3f %s received %s" % (R.time, self, packet)

        # Update hop count
        packet.hop_count += 1
        assert packet.hop_count < 1000, "Hop count >1000? %s" % packet

        # Switch packet around
        next_port_id = -1

        # Deliver locally
        #print(self.local_dests)
        if packet.dst_id in self.local_dests:
            if packet.flow_id == PARAMS.flow_print:
                vprint("%s: %s Local destination" % (self, packet))
            next_port_id = self.local_dests[packet.dst_id]

        # expander: use the routing table
        elif packet.tag == "xpand" and PARAMS.n_xpand > 0:
            if packet.flow_id == PARAMS.flow_print:
                vprint("%s: %s xpand destination" % (self, packet))
            next_port_id = self.dst_to_port[packet.dst_id]  # Use routing table

        # rotor: figure out 1st/2nd hop and adjust
        elif packet.tag == "rotor":
            # Add to rotor queue
            if packet.flow_id == PARAMS.flow_print:
                vprint("%s: %s rotor destination" % (self, packet))
            self.rotor_queue.enq(packet)
            return

        # cache: TODO attempt to send it on cache, fallback on rotor
        elif packet.tag == "cache":
            if packet.flow_id == PARAMS.flow_print:
                vprint("%s: %s Cache destination" % (self, packet))
            self.cache_queue.enq(packet)
            return


        # Get next hop
        # path, _ = self.route[p.dst_id]
        # next_hop = path[0]
        # out_port_id = self.tor_to_port[next_hop]


        # Add to queue
        self.ports_tx[next_port_id].enq(packet)
    '''
    # Useful only for pretty prints: what comes first, packets second
    def _send(self, port_id):
        """Called for every port, attempts to send"""
        # If we're still transmitting, stop
        if not self.out_enable[port_id]:
            return

        queue = self.next_queue(port_id)
        dst_q   = self.ports_rx[port_id]
        dst_tor = self.ports_dst[port_id]

        # Nothing to do, return
        if queue is None:
            return

        # Get the packet
        p = queue.pop()
        p.intended_dest = dst_tor.id
        if get_port_type(port_id) == "rotor":
            self.capacity[p.dst_id] += 1
            #self.capacities[port_id][p.dst_id] -= 1

        if LOG is not None:
            LOG.log(src = self, dst = dst_tor,
                    rotor = self.switches[port_id], packet = p)

        if PARAMS.verbose:
            if p.tag == "xpand":
                vprint("\033[0;31m", end = "")
            if p.tag == "rotor":
                vprint("\033[0;32m", end = "")
            if p.tag == "cache":
                vprint("\033[0;33m", end = "")
            vprint("@%.3f   %s %d:%d->%d %s\033[00m"
                    % (R.time, p.tag, self.id, port_id, dst_tor.id, p))

        # Send the packet
        dst_q.enq(p)

        # We're back to being busy, and come back when we're done
        self.out_enable[port_id] = False
        packet_ttime = self.packet_lag(p.size)
        R.call_in(delay = packet_ttime, fn = self._enable_out, port_id = port_id)

    def _recv(self, p):
        assert p.intended_dest == self.id, "@%.3f %s received %s" % (R.time, self, p)

        # You have arrived :)
        if p.dst_id == self.id:
            FLOWS[p.flow_id].rx()
            return

        if p.tag == "cache" and PARAMS.n_cache > 0:
            assert False, "Cache should always hit home..."

        # Get next hop
        path, _ = self.route[p.dst_id]
        next_hop = path[0]
        port_id = self.tor_to_port[next_hop]

        # Add to queue
        self.buffers_fst[port_id].recv(p)

        # Attempt to send now
        self._send(port_id)

    def recv(self, p):
        packet_ttime = self.packet_lag(p.size) 
        R.call_in(packet_ttime, fn = self._recv, p=p)

    def recv_flow(self, flow, add_to = None):
        """Receives a new flow to serve"""
        # Add the flow, and then attempt to send
        if add_to is None:
            add_to = flow.tag

        if add_to == "xpand":
            if PARAMS.n_xpand == 0:
                return self.recv_flow(flow, add_to = "rotor")

            #path, _ = self.route[flow.dst]
            #n_tor   = path[0]
            #port_id = self.tor_to_port[n_tor]

            port_id = random.choice(xpand_ports)

            heapq.heappush(self.flows_xpand[port_id], (flow.remaining_packets, flow.id, flow))
            self._send(port_id)
            return

        if add_to == "rotor":
            if PARAMS.n_rotor == 0:
                return self.recv_flow(flow, add_to = "xpand")
            self.flows_rotor[flow.dst].append(flow)
            self.capacity[flow.dst] -= flow.remaining_packets
            self.n_flows += 1
            return

        if add_to == "cache":
            if PARAMS.n_cache == 0:
                return self.recv_flow(flow, add_to = "rotor")

            # If all cache links are busy, route to rotor
            if PARAMS.cache_policy == "rotor":
                free = False
                for p in cache_ports:
                    if self.active_flow[p] is None:
                        free = True
                        break
                if not free:
                    return self.recv_flow(flow, add_to = "rotor")


            self.flows_cache.append(flow)
            for cache_port in cache_ports:
                self._send(cache_port)
                '''


    # Printing stuffs
    ################

    @color_str_
    def __str__(self):
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

