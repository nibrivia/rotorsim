import sys
from buffer import *
from helpers import *
from event import Delay, R
from functools import lru_cache

class ToRSwitch:
    def __init__(self, name,
            n_tor, n_rotor,
            slice_duration, packets_per_slot, clock_jitter,
            logger, verbose):
        # Stuff about me
        self.id      = int(name)

        # ... about others
        self.n_tor  = n_tor
        self.rotors = [None for _ in range(n_rotor)]
        self.out_enable = [True for _ in range(n_rotor)]

        # ... about time
        self.packets_per_slot = packets_per_slot
        self.slice_t       = -1
        #self.slot_duration = slot_duration
        self.slice_duration = slice_duration
        self.clock_jitter  = clock_jitter
        self.packet_ttime  = self.slice_duration / packets_per_slot

        self.recv = Delay(self.packet_ttime)(self._recv)

        # ... about IO
        self.verbose = verbose

        # Demand
        self.logger = logger
        self.buffers_dir = [Buffer(parent = self,
                                   src = self.id, dst = dst,
                                   name = "dir[%s]" % dst,
                                   logger = logger, verbose = verbose)
                                for dst in range(n_tor)]
        self.buffers_ind = [Buffer(parent = self,
                                   src = self.id, dst = dst,
                                   name = "ind[%s]" % dst,
                                   logger = logger, verbose = verbose)
                                for dst in range(n_tor)]
        self.buffers_rcv = [Buffer(parent = self,
                                   src = src, dst = self.id,
                                   name = "rcv[%s]" % src,
                                   logger = logger, verbose = verbose)
                                for src in range(n_tor)]
        self.buffers_fst = [Buffer(parent = self,
                                   src = self.id, dst = rotor_id,
                                   name = "fst[Rot %s]" % rotor_id,
                                   logger = logger, verbose = verbose)
                                for rotor_id in range(n_rotor)]

        # Each item has the form (tor, link_remaining)
        self.connections = [None for _ in range(n_rotor)]
        self.capacities  = [None for _ in range(n_rotor)]
        self.capacity    = [self.packets_per_slot for _ in range(n_tor)]
        self.tor_to_rotor = dict()

    # One-time setup
    ################

    def connect_rotor(self, rotor, queue):
        # queue is an object with a .recv that can be called with (packets)
        self.rotors[rotor.id] = queue

    def add_matchings(self, matchings_by_slot_rotor):
        self.matchings_by_slot_rotor = matchings_by_slot_rotor

    def set_tor_refs(self, tors):
        self.tors = tors

    def start(self):
        # This is the first time, we need to connect everyone
        self.slice_t += 1
        slot_t = self.slice_t // len(self.rotors)
        n_slots = len(self.matchings_by_slot_rotor)
        matchings_in_effect = self.matchings_by_slot_rotor[slot_t % n_slots]

        # For all active matchings, connect them up!
        for rotor_id in range(len(self.rotors)):
            matchings = matchings_in_effect[rotor_id]
            print("%s: slot %s/%s, slice %s/%s -> Rot %s" % (
                self, slot_t+1,n_slots, self.slice_t+1, len(self.rotors), rotor_id))

            for src, dst in matchings:
                if src.id == self.id:
                    self.connect_to(rotor_id, dst)

        # Set a countdown for the next slot, just like normal
        Delay(self.slice_duration, jitter = self.clock_jitter)(self.new_slice)()
        self.make_route()

        # TODO be smarter about this
        self.tor_to_rotor = dict()
        for rotor_id, (tor, _) in enumerate(self.connections):
            self.tor_to_rotor[tor.id] = rotor_id


    # Every slice setup
    ###################

    def new_slice(self):
        self.slice_t += 1
        slot_t = self.slice_t // len(self.rotors)
        n_slots = len(self.matchings_by_slot_rotor)
        matchings_in_effect = self.matchings_by_slot_rotor[slot_t % n_slots]

        # Switch up relevant matching
        rotor_id = self.slice_t % len(self.rotors)
        matchings = matchings_in_effect[rotor_id]
        print("%s: slot %s/%s, slice %s/%s -> Rot %s" % (
            self, slot_t+1,n_slots, self.slice_t+1, len(self.rotors), rotor_id))

        for src, dst in matchings:
            if src.id == self.id:
                self.connect_to(rotor_id, dst)

        # Set a countdown for the next slot
        Delay(self.slice_duration, jitter = self.clock_jitter, priority=-1)(self.new_slice)()
        self.make_route()

    def connect_to(self, rotor_id, tor):
        """This gets called for every rotor and starts the process for that one"""
        # Set the connection
        self.connections[rotor_id] = (tor, self.packets_per_slot)

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
    def make_route(self):
        # Routing table
        self.route = [(None, self.n_tor*1000) for _ in range(self.n_tor)]
        self.route[self.id] = ([], 0)
        queue = [self]

        #This is a bastardized dijkstra - it assumes all cost are one
        while len(queue) > 0:
            tor    = queue.pop()
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


    # SENDING ALGORITHMS
    ####################

    def next_queue(self, rotor_id):
        dst, remaining = self.connections[rotor_id]

        # Priority queue
        if self.buffers_fst[rotor_id].size > 0:
            self.vprint("\033[0;31mLow latency: %s:%d\033[00m" % (self, rotor_id), 2)
            return self.buffers_fst[rotor_id]

        # Old indirect traffic
        if self.buffers_ind[dst.id].size > 0:
            self.vprint("\033[0;33mOld Indirect: %s:%d\033[00m" % (self, rotor_id), 2)
            return self.buffers_ind[dst.id]

        # Direct traffic
        if self.buffers_dir[dst.id].size > 0:
            self.vprint("\033[0;32mDirect: %s:%d\033[00m" % (self, rotor_id), 2)
            return self.buffers_dir[dst.id]

        # New indirect traffic
        for buf in shuffle(self.buffers_dir):
            # TODO figure out how RotorLB works here...
            if buf.size > 0 and self.capacities[rotor_id][buf.dst] > 0:
                self.vprint("\033[1;33mNew\033[0;33m Indirect: %s:%d\033[00m" % (self, rotor_id), 2)
                return buf

        return None


    # Actual packets moving
    ########################

    def _enable_out(self, rotor_id):
        self.out_enable[rotor_id] = True
        # We're done transmitting, try again
        Delay(0)(self._send)(rotor_id)

    # Useful only for pretty prints: what comes first, packets second
    def _send(self, rotor_id):
        # If we're still transmitting, stop
        if not self.out_enable[rotor_id]:
            return

        queue = self.next_queue(rotor_id)

        # Nothing to do, return
        if queue is None:
            return

        # Send the packet
        p = queue.packets[0]
        self.capacity[p.dst.id] += 1
        queue.send_to(self.rotors[rotor_id], 1)

        # We're back to being busy, and come back when we're done
        self.out_enable[rotor_id] = False
        Delay(delay = self.packet_ttime)(self._enable_out)(rotor_id)

        # Actually move the packet
        if False:
            print()
            print("%s sending from %s on port %s, amount %s" % (
                self, queue, rotor_id, 1))

    def _recv(self, port_id, packets):
        for p in packets:
            # Receive the packets
            flow_src = p.src
            flow_dst = p.dst
            seq_num = p.seq_num

            # You have arrived :)
            if flow_dst.id == self.id:
                # accept packet into the receive buffer
                self.buffers_rcv[flow_src.id].recv([p])

                # send an ack to the flow that sent this packet
                # TODO remove transport layer stuff from here
                p.flow.recv([p])
                continue

            # Time-sensitive stuff
            if not p.high_thput:
                # Get next hop
                path, _ = self.route[flow_dst.id]
                next_hop = path[0]
                rotor_id = self.tor_to_rotor[next_hop]

                # Add to queue
                self.buffers_fst[rotor_id].recv([p])

                # Attempt to send now
                self.capacity[flow_dst.id] -= 1
                self._send(rotor_id)
                continue

            # From my hosts
            if flow_src.id == self.id:
                queue = self.buffers_dir[flow_dst.id]
            else: # or indirect
                queue = self.buffers_ind[flow_dst.id]
            queue.recv([p])
            self.capacity[flow_dst.id] -= 1



    #TODO remove
    @Delay(0)
    def add_demand_to(self, dst, packets):
        self._recv(-1, packets)

    # Printing stuffs
    ################

    def __str__(self):
        return "ToR %s" % self.id

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

    def vprint(self, msg="", level = 0):
        if self.verbose:
            pad = "  " * level
            print("%s%s" % (pad, msg))


