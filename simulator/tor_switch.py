import sys
from buffer import *
from helpers import *
from event import Delay, R
from functools import lru_cache

class ToRSwitch:
    def __init__(self, name,
            n_tor, n_rotor,
            slot_duration, packets_per_slot, clock_jitter,
            logger, verbose):
        # Stuff about me
        self.id      = int(name)

        # ... about others
        self.n_tor   = n_tor
        self.rotors  = [None for i in range(n_rotor)]

        # ... about time
        self.packets_per_slot = packets_per_slot
        self.slot_t        = -1
        self.slot_duration = slot_duration
        self.clock_jitter  = clock_jitter

        # ... about IO
        self.verbose = verbose

        # Demand
        self.tot_demand = 0

        self.buffers_dir = [Buffer(parent = self,
                                   src = self.id, dst = dst,
                                   logger = logger, verbose = verbose)
                                for dst in range(n_tor)]
        self.buffers_dst = [Buffer(parent = self,
                                   src = self.id, dst = dst,
                                   logger = logger, verbose = verbose)
                                for dst in range(n_tor)]
        self.buffers_rcv = [Buffer(parent = self,
                                   src = src, dst = self.id,
                                   logger = logger, verbose = verbose)
                                for src in range(n_tor)]

        # Each item has the form (tor, link_remaining)
        self.connections = dict()
        self.capacity = self.compute_capacity()

    def vprint(self, msg="", level = 0):
        if self.verbose:
            pad = "  " * level
            print("%s%s" % (pad, msg))

    def add_demand_to(self, dst, amount):
        if dst.id != self.id:
            self.buffers_dir[dst.id].add_n(amount, src = self, dst = dst)
            self.capacity[dst.id] -= amount
            self.tot_demand += amount

    def connect_rotor(self, rotor, queue):
        # queue is an object with a .recv that can be called with (packets)
        self.rotors[rotor.id] = queue


    def add_matchings(self, matchings_by_slot_rotor):
        self.matchings_by_slot_rotor = matchings_by_slot_rotor

    def send(self, rotor_id, queue, amount):
        if amount == 0:
            return

        link_dst, link_remaining = self.connections[rotor_id]

        # Check link capacity
        assert link_remaining >= amount, \
            "%s, flow%s, rotor%s attempting to send %d, but capacity %s" % (
                self, queue, rotor_id, amount, link_remaining)


        # Update link remaining
        link_remaining -= amount
        self.connections[rotor_id] = (link_dst, link_remaining)

        # Update our capacity
        for i in range(amount):
            flow_dst = queue.packets[i].dst
            self.capacity[flow_dst.id] += 1
            self.tot_demand -= 1

        # Move the actual packets
        # TODO scheduling the sends so that they can happen over time...
        queue.send_to(self.rotors[rotor_id], amount)


    def recv(self, rotor_id, packets):
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
                p.flow.recv([p])

            else:
                queue = self.buffers_dst[flow_dst.id]
                assert queue.size < self.packets_per_slot, \
                        "%s at capacity to %s with %s" % (self, flow_dst, self.capacity)
                queue.recv([p])

                # Update book-keeping
                self.tot_demand += 1
                self.capacity[flow_dst.id] -= 1



    def new_slot(self):
        self.slot_t += 1
        n_slots = len(self.matchings_by_slot_rotor)
        matchings_in_effect = self.matchings_by_slot_rotor[self.slot_t % n_slots]

        # For all active matchings, connect them up!
        for rotor_id, matchings in enumerate(matchings_in_effect):
            for src, dst in matchings:
                if src.id == self.id:
                    self.connect_to(rotor_id, dst)

        # Set a countdown for the next slot
        Delay(self.slot_duration, jitter = self.clock_jitter)(self.new_slot)()


    def connect_to(self, rotor_id, tor):
        # Set the connection
        self.connections[rotor_id] = (tor, self.packets_per_slot)

        # TODO when this is more decentralized
            #self.offer()
            #self.accept()

        # Do the stuffs!!
        self.send_old_indirect(rotor_id)
        self.send_direct(rotor_id)
        self.send_new_indirect(rotor_id)

    @Delay(0, priority = 1)
    def send_old_indirect(self, rotor_id):
        # Get connection data
        dst, remaining = self.connections[rotor_id]

        # All indirect traffic to dst
        queue = self.buffers_dst[dst.id]

        # Verify link violations
        assert queue.size <= self.packets_per_slot, \
                "%s->%s old indirect %d > capacity" % (self, dst, queue.size)

        # Stop here if there's nothing to do
        if queue.size == 0:
            return

        self.vprint("Old Indirect: %s:%d" % (self, rotor_id), 2)

        # Send the data
        self.send(rotor_id, queue, queue.size)

    @Delay(0, priority = 2)
    def send_direct(self, rotor_id):
        # Get connection data
        dst, remaining = self.connections[rotor_id]
        flow = (self.id, dst.id)
        queue = self.buffers_dir[dst.id]
        amount = min(queue.size, remaining)

        # Stop if nothing to do
        if amount == 0:
            return

        self.vprint("Direct: %s:%d" % (self, rotor_id), 2)

        self.send(rotor_id = rotor_id,
                  queue  = queue,
                  amount = amount)

    @Delay(0, priority = 3)
    def send_new_indirect(self, rotor_id):
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

        self.vprint("New Indirect: %s:%d" % (self, rotor_id), 2)

        # Send the amounts we decided on
        for flow_dst_id, amount in enumerate(amounts):
            if amount == 0 and remaining > 0 and False:
                print("%s: flow %s not sending to %s via %s (remaining %s, %s.capacity[%s]= %s)" %
                        (self, flow, dst.id, rotor_id, remaining, dst.id, flow[1], capacities[flow_dst]))

            self.send(
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

    def __str__(self):
        return "ToR %s" % self.id

    def buffer_str(self):
        s = "\n" + str(self)
        s += "\nOld Indirect\n  "
        for dst, b in enumerate(self.buffers_dst):
            s += "%2d " % b.size

        s += "\nDirect\n  "
        for dst, b in enumerate(self.buffers_dir):
            s += "%2d " % b.size

        s += "\nReceived\n  "
        for src, b in enumerate(self.buffers_rcv):
            s += "%2d " % b.size

        return s


