import sys
from buffer import *
from helpers import *
from event import Delay, R
from functools import lru_cache
from collections import deque

class ToRSwitch:
    def __init__(self, name,
            n_tor, n_xpand, n_rotor, n_cache,
            packets_per_slot, clock_jitter,
            logger, verbose,
            slot_duration = None, slice_duration = None
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
        if slice_duration is not None:
            self.packet_ttime   = self.slice_duration / packets_per_slot
        if slot_duration is not None:
            self.packet_ttime   = self.slot_duration / packets_per_slot

        self.recv = Delay(self.packet_ttime)(self._recv)

        # ... about IO
        self.verbose = verbose
        self.logger  = logger

        # Demand
        self.flows_cache = []
        self.flows_rotor = [[] for dst in range(n_tor)]
        self.flows_xpand = {port_id: [] for port_id in self.xpand_ports}
        self.buffers_ind = [Buffer(parent = self,
                                   src = self.id, dst = dst,
                                   name = "ind[%s]" % dst,
                                   logger = logger, verbose = verbose)
                                for dst in range(n_tor)] # holds indirected packets (rotor)

        # TODO cache
        self.active_flow = {port_id: None for port_id in self.cache_ports}

        # rotor
        self.capacities  = [0    for _ in range(self.n_tor)] # Capacities of destination
        self.capacity    = [self.packets_per_slot for _ in range(self.n_tor)]
        self.out_queues  = [(-1, Buffer(parent = self, src = self.id, dst = None,
                                   name = "rot_port[%s]" % rotor_id,
                                   logger = logger, verbose = verbose))
                            for rotor_id in self.rotor_ports]

        # xpander
        self.connections = dict() # Destination ToRs
        self.tor_to_port = dict() # the rotor to use to get the next destination
        self.port_to_tor = dict()
        self.buffers_fst = {port_id: Buffer(parent = self,
                                   src = self.id, dst = None,
                                   name = "fst[%s]" % port_id,
                                   logger = logger, verbose = verbose)
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

        if False: # TODO check with > 1 expander
            print(xpand_matchings)
            print()
            print(self, "->")
            for port_id, m in xpand_matchings.items():
                print("      -> ", m)
            print("--\n")
        self.xpand_matchings = xpand_matchings

        for port_id, dst_tor in xpand_matchings.items():
            self.ports[port_id][0] = dst_tor


    def set_tor_refs(self, tors):
        self.tors = tors

    def start(self):
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
            self.new_slice = Delay(self.slot_duration, priority = -1000)(self.new_slice)
        if self.slice_duration is not None:
            self.new_slice = Delay(self.slice_duration, priority = -1000)(self.new_slice)
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
        return int(R.time/self.slice_duration)

    @property
    def slot_t(self):
        assert self.slot_duration is not None
        return round(R.time/self.slot_duration)

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

    @Delay(0, priority = -1000)
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
    @Delay(0, priority = 100)
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
        # Kinda hacky, but ok
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
                print("\033[0;33mflow %d start (%s)\033[00m" % (f.id, f.tag))

                self.ports[port_id][0] = self.tors[f.dst]
                self.flows_cache.pop(i)

                fct = f.remaining_packets * self.packet_ttime
                self.active_flow[port_id] = f

                R.call_in(fct, self.cache_flow_done, port_id = port_id)

                return None # Still not simulating packet level
        #print("@%.3f no matchings available %s" % (R.time, self))

    def cache_flow_done(self, port_id):
        #print("@%.3f %s done" % (R.time, self.active_flow[port_id]))
        self.vprint("\033[0;33mflow", self.active_flow[port_id].id, "is done (cache)")

        self.logger.log_flow_done(self.active_flow[port_id].id)
        self.active_flow[port_id] = None
        self.switches[port_id].release_matching(self)

    def next_queue_rotor(self, port_id):
        # Check if we've computed this before
        queue_t, q = self.out_queues[port_id]
        if queue_t == self.slot_id:
            if q.size == 0:
                return None
            return q

        # We're starting from scratch, this should be empty
        assert q.size == 0
        remaining = self.packets_per_slot

        rotor_id = port_id # TODO translate this
        dst, _ = self.ports[rotor_id]

        if self.buffers_ind[dst.id].size > self.packets_per_slot:
            print(self.buffer_str())

        # Old indirect traffic goes first
        indirect_packets = self.buffers_ind[dst.id].empty()
        q.recv_many(indirect_packets)

        assert q.size <= self.packets_per_slot, self.buffer_str()
        remaining -= q.size

        # Direct traffic
        while remaining > 0 and len(self.flows_rotor[dst.id]) > 0:
            f = self.flows_rotor[dst.id][0]

            p = f.pop()
            q.recv(p)
            remaining -= 1

            if f.remaining_packets == 0:
                self.flows_rotor[dst.id].pop(0)

        # New indirect traffic
        # TODO should actually load balance
        delta = 1
        while delta > 0 and remaining > 0:
            delta = 0
            for dst_id, tor in enumerate(self.tors):
                if remaining == 0:
                    break

                if len(self.flows_rotor[dst_id]) > 0:
                    f = self.flows_rotor[dst_id][0]

                    if tor.capacity[dst_id] <= 0:
                        continue

                    p = f.pop()
                    q.recv(p)
                    remaining -= 1
                    delta += 1
                    tor.capacity[dst_id] -= 1

                    if f.remaining_packets == 0:
                        self.flows_rotor[dst_id].pop(0)


        self.out_queues[port_id] = (self. slot_id, q)
        if q.size > 0:
            return q
        else:
            return None

    def next_queue_xpand(self, port_id):
        # Priority queue

        # If there are already packets waiting
        if self.buffers_fst[port_id].size > 0:
            return self.buffers_fst[port_id]

        # If we have flows waiting
        flows = self.flows_xpand[port_id]
        if len(flows) > 0:
            f = flows[0]

            # Remove if we're done
            if f.remaining_packets == 1:
                self.flows_xpand[port_id].pop(0)

            return f




    # Actual packets moving
    ########################

    def _enable_out(self, port_id):
        self.out_enable[port_id] = True
        # We're done transmitting, try again
        self._send(port_id)

    # Useful only for pretty prints: what comes first, packets second
    def _send(self, port_id):
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

        if self.logger is not None:
            self.logger.log(src = self, dst = dst_tor,
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
        R.call_in(delay = self.packet_ttime, fn = self._enable_out, port_id = port_id)

    def _recv(self, p):
        assert p.intended_dest == self.id, "@%.3f %s received %s" % (R.time, self, p)
        # You have arrived :)
        if p.dst_id == self.id:
            if p.is_last:
                if self.verbose:
                    if p.tag == "xpand":
                        print("\033[0;31m", end = "")
                    if p.tag == "rotor":
                        print("\033[0;32m", end = "")
                    print("flow %s done  (%s)\033[00m" % (p.flow_id, p.tag))
                self.logger.log_flow_done(p.flow_id)
            # accept packet into the receive buffer
            #self.buffers_rcv[flow_src.id].recv(p)
            return

        # Time-sensitive stuff
        if p.tag == "cache" and self.n_cache > 0:
            assert False, "Cache should always hit home..."


        # Low latency, through the expander
        if p.tag == "xpand":
            # Get next hop
            path, _ = self.route[p.dst_id]
            next_hop = path[0]
            port_id = self.tor_to_port[next_hop]

            # Add to queue
            self.buffers_fst[port_id].recv(p)

            # Attempt to send now
            self._send(port_id)
            return

        # From my hosts
        if p.src_id == self.id:
            assert False, "%s received packet %p"
            queue = self.buffers_dir[p.dst_id]
            self.non_zero_dir[flow_dst.id] = self.buffers_dir[flow_dst.id]
        else: # or indirect
            self.capacity[p.dst_id] -= 1
            queue = self.buffers_ind[p.dst_id]

        queue.recv(p)
        self.capacity[p.dst_id] -= 1

    def recv_flow(self, flow):
        # Add the flow, and then attempt to send
        if flow.size < 1e6:
            path, _ = self.route[flow.dst]
            n_tor   = path[0]
            port_id = self.tor_to_port[n_tor]

            self.flows_xpand[port_id].append(flow)
            self._send(port_id)

        elif flow.size < 100e6:
            self.flows_rotor[flow.dst].append(flow)
            self.capacity[flow.dst] -= flow.remaining_packets
            for port_id in self.rotor_ports:
                self._send(port_id)

        else:
            if self.n_cache == 0:
                self.flows_rotor[flow.dst].append(flow)
                for port_id in self.rotor_ports:
                    self._send(port_id)

            else:
                self.flows_cache.append(flow)
                # TODO attempt to create a new cache connection
                for port_id in self.cache_ports:
                    self._send(port_id)




    #TODO remove
    @Delay(0)
    def add_demand_to(self, dst, packets):
        for p in packets:
            self._recv(p)

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


