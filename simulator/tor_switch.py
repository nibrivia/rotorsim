import sys
from buffer import *
from helpers import *
from event import Delay, R
from functools import lru_cache
from collections import deque

class ToRSwitch:
    def __init__(self, name,
            n_tor, n_xpand, n_rotor, n_cache,
            slice_duration, packets_per_slot, clock_jitter,
            logger, verbose):
        # Stuff about me
        self.id      = int(name)
        self.name    = "Tor %d" % self.id

        # ... about others
        self.n_xpand = n_xpand
        self.n_rotor = n_rotor
        self.n_cache = n_cache
        self.n_switches = n_xpand + n_rotor + n_cache

        self.n_tor  = n_tor

        self.switches   = [None for _ in range(self.n_switches)] # queues
        self.out_enable = [True for _ in range(self.n_switches)]

        # ... about time
        self.packets_per_slot = packets_per_slot
        self.slice_t       = -1
        #self.slot_duration = slot_duration
        self.slice_duration = slice_duration
        self.clock_jitter   = clock_jitter
        self.packet_ttime   = self.slice_duration / packets_per_slot

        self.recv = Delay(self.packet_ttime)(self._recv)

        # ... about IO
        self.verbose = verbose
        self.logger  = logger

        # Demand
        self.flows_cache = [[] for dst in range(n_tor)]
        self.flows_rotor = [[] for dst in range(n_tor)]
        self.flows_xpand = [[] for dst in range(n_tor)]
        self.buffers_ind = [Buffer(parent = self,
                                   src = self.id, dst = dst,
                                   name = "ind[%s]" % dst,
                                   logger = logger, verbose = verbose)
                                for dst in range(n_tor)]

        # Each item has the form (tor, link_remaining)
        self.connections = [(None, 0) for _ in range(self.n_rotor)] # TODO ???
        self.capacities  = [0         for _ in range(self.n_rotor)] # Capacities of destination
        self.capacity    = [self.packets_per_slot for _ in range(n_tor)]
        self.tor_to_rotor = dict()

        self.tables       = dict()
        self.non_zero_dir = dict()
        self.dests        = dict()
        self.last_sent = 0

    # One-time setup
    ################

    def connect_switch(self, port, queue):
        # queue is an object with a .recv that can be called with (packets)
        self.switches[port] = queue

    def add_matchings(self, matchings_by_slot_rotor):
        self.matchings_by_slot_rotor = [[ None for _ in m] for m in matchings_by_slot_rotor]
        for slot, matchings_by_rotor in enumerate(matchings_by_slot_rotor):
            for rotor_id, matchings in enumerate(matchings_by_rotor):
                for src, dst in matchings:
                    if src.id == self.id:
                        self.matchings_by_slot_rotor[slot][rotor_id] = dst

    def set_tor_refs(self, tors):
        self.tors = tors

    def start(self):
        # This is the first time, we need to connect everyone
        self.slice_t = 0
        slot_t = self.slice_t // self.n_rotor
        n_slots = len(self.matchings_by_slot_rotor)
        matchings_in_effect = self.matchings_by_slot_rotor[slot_t % n_slots]

        # For all active matchings, connect them up!
        for rotor_id in range(self.n_rotor):
            dst = matchings_in_effect[rotor_id]
            self.connect_to(rotor_id, dst)

        # Set a countdown for the next slot, just like normal
        self.new_slice = Delay(self.slice_duration, jitter = self.clock_jitter)(self.new_slice)
        self.new_slice()
        #R.call_in(0, self.make_route)
        self.make_route()

        # TODO be smarter about this
        self.tor_to_rotor = dict()
        for rotor_id, (tor, _) in enumerate(self.connections):
            self.tor_to_rotor[tor.id] = rotor_id


    # Every slice setup
    ###################

    def new_slice(self):
        self.slice_t += 1
        slot_t = self.slice_t // self.n_rotor
        n_slots = len(self.matchings_by_slot_rotor)
        matchings_in_effect = self.matchings_by_slot_rotor[slot_t % n_slots]

        # Switch up relevant matching
        rotor_id = self.slice_t % self.n_rotor
        dst = matchings_in_effect[rotor_id]
        self.connect_to(rotor_id, dst)

        # Set a countdown for the next slot
        self.new_slice() # is a delay() object

        # If we've already computed it for this topology, get it from the cache
        # As a side effect, this means that since make_route doesn't run that 
        # often, it can be pretty inefficiently implemented :)
        if rotor_id in self.tables:
            #print("cached")
            self.route, self.tor_to_rotor = self.tables[rotor_id]
        else:
            self.make_route(rotor_id)

    def connect_to(self, rotor_id, tor):
        """This gets called for every rotor and starts the process for that one"""
        # Set the connection
        self.connections[rotor_id] = (tor, self.packets_per_slot)
        self.dests[rotor_id] = tor

        # Get capacities for indirection
        self.capacities[rotor_id] = tor.capacity

        # Start sending
        self._send(rotor_id)


    @property
    def link_state(self):
        # TODO do this in connect_to, reduces complexity by at least O(n_tor)
        links = dict()
        for tor, _ in self.connections:
            links[tor.id] = 1
        return links

    # By having a delay 0 here, this means that every ToR will have gone
    # through its start, which will then mean that we can call link_state
    @Delay(0)
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

        self.tor_to_rotor = dict()
        for rotor_id, (tor, _) in enumerate(self.connections):
            self.tor_to_rotor[tor.id] = rotor_id

        if slice_id is not None:
            self.tables[slice_id] = (self.route, self.tor_to_rotor)


    # SENDING ALGORITHMS
    ####################

    def next_queue(self, port_id):
        # Kinda hacky, but ok
        if port_id < self.n_rotor:
            return self.next_queue_rotor(port_id)
        if port_id < self.n_rotor + self.n_expand:
            return self.next_queue_xpand(port_id)
        else:
            return self.next_queue_cache(port_id)

    def next_queue_cache(self, port_id):
        for f in self.flows_cache:
            if f.dst == self.cache_links[port_id]:
                # TODO send
                pass

    def next_queue_rotor(self, port_id):
        rotor_id = port_id # TODO translate this
        dst, remaining = self.connections[rotor_id]


        # Old indirect traffic, deal with packets that are here
        if self.buffers_ind[dst.id].size > 0:
            self.vprint("\033[0;33mOld Indirect: %s:%d\033[00m" % (self, rotor_id), 2)
            return self.buffers_ind[dst.id]

        # Direct traffic, deal with the flows here
        flows = self.flows_rotor[dst.id]
        if len(flows) > 0:
            f = flows[0]
            # We need to update bookeeping here
            #if len(flows) == 1 and f.remaining_packets == 1:
            #    del self.non_zero_dir[dst.id]

            if f.remaining_packets == 1:
                print("                               Flow %d done" % f.id)
                self.flows_rotor[dst.id].pop(0)

            self.vprint("\033[0;32mDirect: %s:%d\033[00m" % (self, rotor_id), 2)
            return f

        # New indirect traffic
        # TODO should actually load balance
        for flow_dst, flows in enumerate(self.flows_rotor):
            if len(flows) > 0:
                self.vprint("\033[0;31mNew Indirect: %s:%d\033[00m" % (self, rotor_id), 2)
                f = flows[0]

                if f.remaining_packets == 1:
                    print("                           Flow %d done" % f.id)
                    self.flows_rotor[flow_dst].pop(0)

                return f

        return None

    def next_queue_xpand(self, port_id):
        # Priority queue
        if self.buffers_fst[port_id].size > 0:
            self.vprint("\033[0;31mLow latency: %s:%d\033[00m" % (self, rotor_id), 2)
            return self.buffers_fst[rotor_id]



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

        # Nothing to do, return
        if queue is None:
            return

        # Send the packet
        p = queue.pop()
        self.capacity[p.dst_id] += 1
        self.dests[port_id].recv(p)

        if self.logger is not None:
            self.logger.log(src = self, dst = self.dests[port_id],
                    rotor = self.switches[port_id], packet = p)

        if self.verbose:
            if p.tag == "xpand":
                self.vprint("\033[0;31m", end = "")
            if p.tag == "rotor":
                self.vprint("\033[0;32m", end = "")
            if p.tag == "cache":
                self.vprint("\033[0;33m", end = "")
            self.vprint("@%.3f   %s %s->%d %3d[%s->%s]#%d\033[00m"
                    % (R.time, p.tag, self.id, self.dests[port_id].id,
                        p.flow_id, p.src_id, p.dst_id, p.seq_num))

        # We're back to being busy, and come back when we're done
        self.out_enable[port_id] = False
        R.call_in(delay = self.packet_ttime, fn = self._enable_out, port_id = port_id)

    def _recv(self, p):
        # You have arrived :)
        if p.dst_id == self.id:
            # accept packet into the receive buffer
            #self.buffers_rcv[flow_src.id].recv(p)
            return

        # Time-sensitive stuff
        if p.tag == "cache":
            assert False, "Cache should always hit home..."


        # Low latency, through the expander
        if p.tag == "xpand":
            # Get next hop
            path, _ = self.route[p.dst_id]
            next_hop = path[0]
            rotor_id = self.tor_to_rotor[next_hop]

            # Add to queue
            self.buffers_fst[rotor_id].recv(p)

            # Attempt to send now
            self.capacity[p.dst_id] -= 1
            self._send(rotor_id)
            return

        # From my hosts
        if p.src_id == self.id:
            assert False
            queue = self.buffers_dir[p.dst_id]
            self.non_zero_dir[flow_dst.id] = self.buffers_dir[flow_dst.id]
        else: # or indirect
            queue = self.buffers_ind[p.dst_id]

        queue.recv(p)
        self.capacity[p.dst_id] -= 1



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

        s += "\nDirect\n  "
        for dst, b in enumerate(self.buffers_dir):
            s += "%2d " % b.size

        s += "\nReceived\n  "
        for src, b in enumerate(self.buffers_rcv):
            s += "%2d " % b.size

        return s

    def vprint(self, msg="", level = 0, *args, **kwargs):
        if self.verbose:
            pad = "  " * level
            print("%s%s" % (pad, msg), *args, **kwargs)


