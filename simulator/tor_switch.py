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
        self.out_qs = [[]   for _ in range(n_rotor)]
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
        self.tot_demand = 0

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

        # Each item has the form (tor, link_remaining)
        self.connections = [None for _ in range(n_rotor)]
        self.tor_to_rotor = dict()
        self.capacity = self.compute_capacity()

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
        Delay(self.slice_duration, jitter = self.clock_jitter)(self.new_slice)()
        self.make_route()

    def connect_to(self, rotor_id, tor):
        """This gets called for every rotor and starts the process for that one"""
        # Set the connection
        self.connections[rotor_id] = (tor, self.packets_per_slot)


        # TODO when this is more decentralized
            #self.offer()
            #self.accept()

        # Do the stuffs!!
        self.send_old_indirect(rotor_id, self.slice_t)
        #self.send_direct(rotor_id)
        #self.send_new_indirect(rotor_id)

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

        if False:
            print("%s routing table" % self)
            for dest, (path, cost) in enumerate(self.route):
                print("  %s : %s, %s" % (dest, path, cost))

        self.tor_to_rotor = dict()
        for rotor_id, (tor, _) in enumerate(self.connections):
            self.tor_to_rotor[tor.id] = rotor_id


    # SENDING ALGORITHMS
    ####################

    @Delay(0)
    def send_old_indirect(self, rotor_id, slice_id):
        # This checks we're on the right slice
        if slice_id != self.slice_t:
            return

        # Get connection data
        dst, remaining = self.connections[rotor_id]

        # All indirect traffic to dst
        queue = self.buffers_ind[dst.id]

        # Verify link violations
        # Skip due to weird TCP interactions
        if False:
            assert queue.size <= self.packets_per_slot, \
                    "%s->%s old indirect %d > capacity" % (self, dst, queue.size)

        # Stop here if there's nothing to do
        if queue.size == 0:
            return self.send_direct(rotor_id, slice_id)

        self.vprint("\033[0;33mOld Indirect: %s:%d\033[00m" % (self, rotor_id), 2)

        # Send the data
        self.schedule_send(rotor_id, queue, min(queue.size, self.packets_per_slot),
                callback = lambda: self.send_direct(rotor_id, slice_id))

    def send_direct(self, rotor_id, slice_id):
        # This checks we're on the right slice
        if slice_id != self.slice_t:
            return

        # Get connection data
        dst, remaining = self.connections[rotor_id]
        flow = (self.id, dst.id)
        queue = self.buffers_dir[dst.id]
        amount = min(queue.size, remaining)

        # Stop if nothing to do
        if amount == 0:
            return self.send_new_indirect(rotor_id, slice_id)

        self.vprint("\033[0;32mDirect: %s:%d\033[00m" % (self, rotor_id), 2)

        self.schedule_send(rotor_id = rotor_id,
                           queue  = queue,
                           amount = amount,
                           callback = lambda: self.send_new_indirect(rotor_id, slice_id))

    def send_new_indirect(self, rotor_id, slice_id):
        # This checks we're on the right slice
        if slice_id != self.slice_t:
            return

        # Get connection data
        dst, remaining = self.connections[rotor_id]

        # Get indirect-able traffic
        capacities = dst.capacity

        # Send what we can along the remaining capacity
        # TODO offer/accept protocol

        # This iterates to balance the remaining capacity equally across flows
        amounts = [0 for _ in self.buffers_dir]
        change = 1
        while change > 0 and remaining > 0:
            change = 0

            # TODO, not just +1, do it the (faster) divide way
            for flow_dst_id, b in enumerate(self.buffers_dir):
                current = amounts[flow_dst_id]
                new = min(current+1, b.size, remaining, capacities[flow_dst_id])
                amounts[flow_dst_id] = max(new, current)

                # Update tracking vars
                delta = amounts[flow_dst_id] - current
                change    += delta
                remaining -= delta

                if remaining == 0:
                    break

        # Stop if nothing to do
        if sum(amounts) == 0:
            return

        self.vprint("\033[1;33mNew\033[0;33m Indirect: %s:%d\033[00m" % (self, rotor_id), 2)

        # Send the amounts we decided on
        for flow_dst_id, amount in enumerate(amounts):
            if amount == 0 and remaining > 0 and False:
                print("%s: flow %s not sending to %s via %s (remaining %s, %s.capacity[%s]= %s)" %
                        (self, flow, dst.id, rotor_id, remaining, dst.id, flow[1], capacities[flow_dst]))

            self.schedule_send(
                    rotor_id = rotor_id,
                    queue    = self.buffers_dir[flow_dst_id],
                    amount   = amount)

    # May no longer be useful?
    def compute_capacity(self):
        return {dst: self.packets_per_slot for dst in range(self.n_tor)}

    def offer(self):
        raise Error
        # TODO offer
        return self.capacity()

    def offer_rx(self, rotor_id, offer, capacity):
        raise Error
        self.offers[rotor_id] = offer
        self.capacities[rotor_id] = capacity



    # Actual packets moving
    ########################

    def schedule_send(self, rotor_id, queue, amount, priority = False, callback = None):
        if amount == 0:
            # we're done, callback
            if callback is not None:
                Delay(0)(callback)()
            return

        link_dst, link_remaining = self.connections[rotor_id]

        # Check link capacity
        if not priority:
            amount = min(link_remaining, amount)

        # Disable due to priority queues taking over
        if False:
            assert link_remaining >= amount, \
                "%s, flow%s, rotor%s attempting to send %d, but capacity %s" % (
                    self, queue, rotor_id, amount, link_remaining)


        # Update link remaining
        link_remaining -= amount
        self.connections[rotor_id] = (link_dst, link_remaining)

        # Update our capacity
        for i in range(amount):
            p = queue.packets[i]
            self.capacity[p.dst.id] += 1
            self.tot_demand -= 1

        # Actually move the packets
        if priority:
            # TODO, really inefficient
            self.out_qs[rotor_id].insert(0, (queue, amount, callback))
            Delay(0, priority = -1)(self._send)(rotor_id)
        else:
            self.out_qs[rotor_id].append((queue, amount, callback))
            Delay(0)(self._send)(rotor_id)

    def _enable_out(self, rotor_id):
        self.out_enable[rotor_id] = True
        Delay(0)(self._send)(rotor_id)

    # Useful only for pretty prints: what comes first, packets second
    def _send(self, rotor_id):
        # If we're still busy, stop
        if not self.out_enable[rotor_id]:
            return

        # Nothing to do, return
        if len(self.out_qs[rotor_id]) == 0:
            return

        # We're back to being busy, and come back when we're done
        self.out_enable[rotor_id] = False
        Delay(delay = self.packet_ttime)(self._enable_out)(rotor_id)

        # Actually move the packet
        queue, amount, callback = self.out_qs[rotor_id][0]
        if False:
            print()
            print("%s sending from %s on port %s, amount %s" % (
                self, queue, rotor_id, amount))
            print([(str(q), qty) for q, qty, _ in self.out_qs[rotor_id]])
        queue.send_to(self.rotors[rotor_id], 1)
        if amount == 1:
            # TODO not pop(0), really inefficient
            self.out_qs[rotor_id].pop(0)

            # callback if we're done
            if callback is not None:
                Delay(delay = self.packet_ttime)(callback)()
        else:
            self.out_qs[rotor_id][0] = (queue, amount-1, callback)

    def _recv(self, rotor_id, packets):
        for p in packets:
            # Receive the packets
            flow_src = p.src
            flow_dst = p.dst
            seq_num = p.seq_num

            # case: packet was destined for this tor
            if flow_dst.id == self.id:
                # accept packet into the receive buffer
                self.buffers_rcv[flow_src.id].recv([p])

                # send an ack to the flow that sent this packet
                # TODO remove transport layer stuff from here
                p.flow.recv([p])

            elif p.high_thput: # business as usual
                queue = self.buffers_ind[flow_dst.id]
                if False:
                    # This check is skipped due to packets being added in mid-course
                    assert queue.size < self.packets_per_slot, \
                            "%s at capacity to %s. Capacities %s, qsize %s" % (
                                self, flow_dst, self.capacity, queue.size)
                queue.recv([p])

                # Update book-keeping
                self.tot_demand += 1
                self.capacity[flow_dst.id] -= 1
            else:
                path, _ = self.route[flow_dst.id]
                next_hop = path[0]
                rotor_id = self.tor_to_rotor[next_hop]
                queue = Buffer(parent = self, 
                        src = flow_src.id, dst = flow_dst.id,
                        verbose = True, logger = self.logger,
                        name = "{%s->%s %s}" % (flow_src.id, flow_dst.id, p.seq_num))
                queue.recv([p])
                self.schedule_send(rotor_id, queue, 1, priority = True)

                # Bookkeeping
                self.tot_demand += 1
                self.capacity[flow_dst.id] -= 1



    def add_demand_to(self, dst, packets):
        if dst.id != self.id:
            self.buffers_dir[dst.id].recv(packets)
            self.capacity[dst.id] -= len(packets)
            self.tot_demand += len(packets)

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



