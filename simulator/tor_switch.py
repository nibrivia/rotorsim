import sys, math, heapq
from buffer import *
from logger import LOG
from helpers import *
from event import Delay, R
from functools import lru_cache
from collections import deque
from flow_generator import FLOWS, BYTES_PER_PACKET

class ToRSwitch:
    def __init__(self, name,
            n_tor, n_xpand, n_rotor, n_cache,
            packets_per_slot, clock_jitter,
            verbose,
            slot_duration = None, slice_duration = None,
            reconfiguration_time = 0
            ):
        assert slot_duration is     None or slice_duration is     None
        assert slot_duration is not None or slice_duration is not None

        # Stuff about me
        self.id      = int(name)
        self.name    = "Tor %d" % self.id

        # ... about others
        self.n_xpand = n_xpand
        self.n_rotor = n_rotor
        self.n_cache = n_cache
        self.n_switches = n_xpand + n_rotor + n_cache
        self.switches = [None for _ in range(self.n_switches)]

        self.n_tor  = n_tor

        # receiving tor, queues
        self.ports      = [[None, None] for _ in range(self.n_switches)]
        self.out_enable = [True for _ in range(self.n_switches)] # whether the NIC can send

        # ... about time
        self.packets_per_slot = packets_per_slot
        self.slot_duration  = slot_duration
        self.slice_duration = slice_duration
        self.clock_jitter   = clock_jitter
        self.reconf_time    = reconfiguration_time
        self.reconf_cache   = 15 # TODO as argument
        if slice_duration is not None:
            self.packet_ttime   = self.slice_duration / packets_per_slot
        if slot_duration is not None:
            self.packet_ttime   = self.slot_duration / packets_per_slot

        self.recv = Delay(0.0005)(self._recv)
        #self.recv = self._recv

        # ... about IO
        self.verbose = verbose

        # Demand
        self.flows_cache = []
        self.flows_rotor = [[] for dst in range(n_tor)]
        self.flows_xpand = {port_id: [] for port_id in self.xpand_ports}
        self.lumps_ind   = [[] for dst in range(n_tor)] # holds indirected packets (rotor)
        self.lumps_ind_n = [0 for dst in range(n_tor)] # holds indirected packets (rotor)

        # TODO cache
        self.active_flow = {port_id: None for port_id in self.cache_ports}

        # rotor
        self.capacities  = [0    for _ in range(self.n_tor)] # Capacities of destination
        self.capacity    = [self.packets_per_slot for _ in range(self.n_tor)]
        self.out_queue_t = [-1 for rotor_id in self.rotor_ports]
        self.n_flows = 0

        # xpander
        self.connections = dict() # Destination ToRs
        self.tor_to_port = dict() # the rotor to use to get the next destination
        self.port_to_tor = dict()
        self.buffers_fst = {port_id: Buffer(parent = self,
                                   src = self.id, dst = None,
                                   name = "fst[%s]" % port_id,
                                   verbose = verbose)
                                for port_id in self.xpand_ports} # holds indirected packets (xpand)


    # One-time setup
    ################

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

    def port_type(self, port_id):
        if port_id < self.n_rotor:
            return "rotor"
        if port_id < self.n_rotor + self.n_xpand:
            return "xpand"
        else:
            return "cache"

    def connect_queue(self, port_id, switch, queue):
        # queue is an object with a .recv that can be called with (packets)
        self.switches[port_id] = switch

        if self.port_type(port_id) == "xpand":
            self.connections[port_id] = queue

        self.ports[port_id][1] = queue

    def add_rotor_matchings(self, matchings_by_slot_rotor):
        self.matchings_by_slot_rotor = [[ None for _ in m] for m in matchings_by_slot_rotor]
        for slot, matchings_by_rotor in enumerate(matchings_by_slot_rotor):
            assert len(matchings_by_rotor) == self.n_rotor
            for rotor_id, matchings in enumerate(matchings_by_rotor):
                for src, dst in matchings:
                    if src.id == self.id:
                        self.matchings_by_slot_rotor[slot][rotor_id] = dst

        self.n_slots = len(matchings_by_slot_rotor)

    def add_xpand_matchings(self, xpand_matchings):
        assert len(xpand_matchings) == self.n_xpand

        self.xpand_matchings = xpand_matchings

        for port_id, dst_tor in xpand_matchings.items():
            self.ports[port_id][0] = dst_tor


    def set_tor_refs(self, tors):
        self.tors = tors

    def start(self):
        """Call once at setup"""
        # Rotor
        #######

        # This is the first time, we need to connect everyone
        slot_t = 0
        n_slots = len(self.matchings_by_slot_rotor)
        matchings_in_effect = self.matchings_by_slot_rotor[slot_t % n_slots]

        # For all active matchings, connect them up!
        for rotor_id in self.rotor_ports:
            dst = matchings_in_effect[rotor_id]
            self.connect_to(rotor_id, dst)

        # Set a countdown for the next slot, just like normal
        if self.slot_duration is not None:
            self.slot_id = 0
            self.new_slice = Delay(self.slot_duration  + self.reconf_time, priority = 1000)(self.new_slice)
        if self.slice_duration is not None:
            self.new_slice = Delay(self.slice_duration + self.reconf_time, priority = 1000)(self.new_slice)
        self.new_slice()
        self.make_route()

        # Expander
        ##########

        # This only iterates over the very beginning of the connections: the rotors
        for port_id, tor in self.xpand_matchings.items():
            self.tor_to_port[tor.id] = port_id


    # Every slice setup
    ###################

    @property
    def slice_t(self):
        assert self.slice_duration is not None
        return round(R.time/(self.slice_duration+self.reconf_time))

    @property
    def slot_t(self):
        assert self.slot_duration is not None
        return round(R.time/(self.slot_duration+self.reconf_time))

    def new_slice(self):
        # Switch up relevant matching
        if self.slice_duration is not None:
            slot_t = self.slice_t // self.n_rotor
            matchings_in_effect = self.matchings_by_slot_rotor[self.slot_t % self.n_slots]

            rotor_id = self.slice_t % self.n_rotor
            dst = matchings_in_effect[rotor_id]
            self.connect_to(rotor_id, dst)

        # If Rotor
        if self.slot_duration is not None:
            self.slot_id = self.slot_t % self.n_slots
            self.vprint("%.6f %s switch %d" % (R.time, self, self.slot_id))
            matchings_in_effect = self.matchings_by_slot_rotor[self.slot_id]
            for rotor_id in self.rotor_ports:
                dst = matchings_in_effect[rotor_id]
                self.connect_to(rotor_id, dst)

        # Set a countdown for the next slot
        self.new_slice() # is a delay() object

    @Delay(0, priority = 10)
    def connect_to(self, port_id, tor):
        """This gets called for every rotor and starts the process for that one"""
        # Set the connection
        self.ports[port_id][0] = tor

        # Get capacities for indirection if rotor
        if port_id < self.n_rotor:
            self.capacities[port_id] = tor.capacity

        # Start sending
        self._send(port_id)


    @property
    def link_state(self):
        # TODO do this in connect_to, reduces complexity by at least O(n_tor)
        links = dict()
        for port_id in self.xpand_ports:
            tor, _ = self.ports[port_id]
            links[tor.id] = 1
        return links


    # By having a delay 0 here, this means that every ToR will have gone
    # through its start, which will then mean that we can call link_state
    @Delay(0, priority = -10)
    def make_route(self, slice_id = None):
        # Routing table
        self.route = [(None, self.n_tor*1000) for _ in range(self.n_tor)]
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
        port_type = self.port_type(port_id)
        if port_type == "rotor":
            return self.next_queue_rotor(port_id)
        if port_type == "xpand":
            return self.next_queue_xpand(port_id)
        if port_type == "cache":
            return self.next_queue_cache(port_id)

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
                self.vprint("\033[0;33mflow %d start (%s)\033[00m" % (f.id, f.tag))

                self.ports[port_id][0] = self.tors[f.dst]
                self.flows_cache.pop(i)

                fct = f.remaining_packets * self.packet_ttime
                n_packets = f.size_packets

                time_left = R.limit - R.time - self.reconf_cache
                self.active_flow[port_id] = f

                if fct > time_left:
                    n_packets = math.floor(time_left/fct * n_packets)
                    if n_packets < 0:
                        return
                    fct = n_packets * self.packet_ttime

                lump = f.pop_lump(n_packets)

                R.call_in(self.reconf_cache + fct, self.cache_flow_done, port_id = port_id, lump = lump)

                return None # Still not simulating packet level

    def cache_flow_done(self, port_id, lump):
        self.vprint("\033[0;33mflow", self.active_flow[port_id].id, "is done (cache)")

        flow_id, dst, n = lump
        FLOWS[flow_id].rx(n)

        self.active_flow[port_id] = None
        self.switches[port_id].release_matching(self)

    def next_queue_rotor(self, port_id):
        """Sends over a lump"""
        # Check if we've computed this before
        queue_t  = self.out_queue_t[port_id]
        if queue_t == self.slot_id:
            return None

        # Get connection info
        rotor_id = port_id # TODO translate this
        dst, dst_q = self.ports[rotor_id]

        # Old indirect traffic goes first
        q = self.lumps_ind[dst.id]
        remaining = self.packets_per_slot - self.lumps_ind_n[dst.id]
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
            t += n*self.packet_ttime

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

    @lru_cache(maxsize=None)
    def packet_lag(self, p):
        return self.packet_ttime / BYTES_PER_PACKET * p

    def _enable_out(self, port_id):
        self.out_enable[port_id] = True
        # We're done transmitting, try again
        self._send(port_id)

    # Useful only for pretty prints: what comes first, packets second
    def _send(self, port_id):
        """Called for every port, attempts to send"""
        # If we're still transmitting, stop
        if not self.out_enable[port_id]:
            return

        queue = self.next_queue(port_id)
        dst_tor, dst_q = self.ports[port_id]

        # Nothing to do, return
        if queue is None:
            return

        # Get the packet
        p = queue.pop()
        p.intended_dest = dst_tor.id
        if self.port_type(port_id) == "rotor":
            self.capacity[p.dst_id] += 1
            #self.capacities[port_id][p.dst_id] -= 1

        if LOG is not None:
            LOG.log(src = self, dst = dst_tor,
                    rotor = self.switches[port_id], packet = p)

        if self.verbose:
            if p.tag == "xpand":
                self.vprint("\033[0;31m", end = "")
            if p.tag == "rotor":
                self.vprint("\033[0;32m", end = "")
            if p.tag == "cache":
                self.vprint("\033[0;33m", end = "")
            self.vprint("@%.3f   %s %d:%d->%d %s\033[00m"
                    % (R.time, p.tag, self.id, port_id, dst_tor.id, p))

        # Send the packet
        dst_q.recv(p)

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

        if p.tag == "cache" and self.n_cache > 0:
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
            if self.n_xpand == 0:
                return self.recv_flow(flow, add_to = "rotor")

            path, _ = self.route[flow.dst]
            n_tor   = path[0]
            port_id = self.tor_to_port[n_tor]

            heapq.heappush(self.flows_xpand[port_id], (flow.remaining_packets, flow.id, flow))
            self._send(port_id)
            return

        if add_to == "rotor":
            if self.n_rotor == 0:
                return self.recv_flow(flow, add_to = "xpand")
            self.flows_rotor[flow.dst].append(flow)
            self.capacity[flow.dst] -= flow.remaining_packets
            self.n_flows += 1
            return

        if add_to == "cache":
            if self.n_cache == 0:
                return self.recv_flow(flow, add_to = "rotor")

            # If all cache links are busy, route to rotor
            free = False
            for cache_port in self.cache_ports:
                if self.active_flow[cache_port] is None:
                    free = True
                    break
            if not free:
                return self.recv_flow(flow, add_to = "rotor")


            self.flows_cache.append(flow)
            for cache_port in self.cache_ports:
                self._send(cache_port)


    # Printing stuffs
    ################

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

    def vprint(self, msg="", level = 0, *args, **kwargs):
        if self.verbose:
            pad = "  " * level
            print("%s%s" % (pad, msg), *args, **kwargs)


