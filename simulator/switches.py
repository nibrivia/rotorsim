import sys
from buffer import *
from helpers import *
from event import delay, R
from functools import lru_cache


class ToRSwitch:
    def __init__(self, name, n_tor, n_rotor, packets_per_slot, logger, verbose, timer):
        # Index by who to send to
        self.id      = int(name)
        self.n_tor   = n_tor
        self.n_rotor = n_rotor
        self.verbose = verbose
        self.timer   = timer
        self.packets_per_slot = packets_per_slot

        # Demand
        self.tot_demand = 0

        self.buffers = { (src, dst) : Buffer(
            name = "%s.%s->%s" % (self.id, src, dst),
            logger = logger,
            verbose = verbose) for src in range(n_tor) for dst in range(n_tor) }

        # Watch out, some might be (intentional) duplicates
        # each item has the form (tor, link_remaining)
        self.connections = dict()
        self.offers = dict()
        self.capacity = self.compute_capacity()

    def vprint(self, msg="", level = 0):
        if self.verbose:
            pad = "  " * level
            print("%s%s" % (pad, msg))


    def print_capacity(self):
        return "%s\nstate: %s\ncompu: %s" % (self, self.capacity, self.compute_capacity())

    def add_demand_to(self, dst, amount):
        if dst.id != self.id:
            self.buffers[(self.id, dst.id)].add_n(amount)
            self.capacity[dst.id] -= amount
            self.tot_demand += amount

    def send(self, rotor_id, flow, amount):
        if amount == 0:
            return

        dst, link_remaining = self.connections[rotor_id]

        # Check link capacity
        assert link_remaining >= amount, \
            "%s, flow%s, rotor%s attempting to send %d, but capacity %s" % (
                self, flow, rotor_id, amount, link_remaining)

        # Move the actual packets
        self.buffers[flow].send_to(dst.buffers[flow], amount, rotor_id)

        # Update link remaining
        link_remaining -= amount
        self.connections[rotor_id] = (dst, link_remaining)

        # Update our + destination capacity
        flow_src, flow_dst = flow
        dst.recv(flow_dst, amount)
        self.capacity[flow_dst] += amount

        self.tot_demand -= amount

        # Return remaining link capacity
        return link_remaining

    def recv(self, dst_id, amount):
        if dst_id != self.id:
            self.tot_demand += amount
            self.capacity[dst_id] -= amount

    def connect_to(self, rotor_id, tor):
        self.connections[rotor_id] = (tor, self.packets_per_slot)

        # TODO when this is more decentralized
            #self.offer()
            #self.accept()
        self.send_old_indirect(rotor_id)
        self.send_direct(rotor_id)
        self.send_new_indirect(rotor_id) 

    def indirect_traffic_to(self, dst):
        # Returns indirect traffic to dst
        flows = [(s, dst.id) for s in range(self.n_tor) if s != self.id]
        return [(f, self.buffers[f]) for f in flows
                    if self.buffers[f].size > 0]

    def direct_traffic(self, dst):
        # Returns direct traffic except to dst
        flows = [(self.id, d) for d in range(self.n_tor) if d != dst.id]
        return [(f, self.buffers[f]) for f in flows
                    if self.buffers[f].size > 0]

    @delay(.001)
    def send_old_indirect(self, rotor_id):
        self.vprint("Old Indirect: %s:%d" % (self, rotor_id), 2)

        # Get connection data
        dst, remaining = self.connections[rotor_id]

        # All indirect traffic to dst
        buffers = self.indirect_traffic_to(dst)

        # Verify link violations
        if False:
            total_send = sum(b.size for _, b in buffers)
            assert total_send <= self.n_rotor*self.packets_per_slot, \
                    "%s->%s old indirect %d > capacity" % (self, dst, total_send)

        # Send the data
        sent = 1
        while sent > 0:
            sent = 0
            for flow, b in buffers:
                if b.size == 0:
                    continue
                if self.connections[rotor_id][1] == 0:
                    break
                self.send(rotor_id = rotor_id,
                          flow     = flow,
                          amount   = 1)
                sent += 1

    @delay(.002)
    def send_direct(self, rotor_id):
        self.vprint("Direct: %s:%d" % (self, rotor_id), 2)

        # Get connection data
        dst, remaining = self.connections[rotor_id]
        flow = (self.id, dst.id)
        amount = bound(0, self.buffers[flow].size, remaining)

        self.send(rotor_id = rotor_id,
                  flow = flow,
                  amount = amount)

    @delay(delay_t = .003)
    def send_new_indirect(self, rotor_id):
        self.vprint("New Indirect: %s:%d" % (self, rotor_id), 2)
        # Get connection data
        dst, remaining = self.connections[rotor_id]

        # Get indirect-able traffic
        traffic = self.direct_traffic(dst)
        capacities = dst.capacity

        # Send what we can along the remaining capacity
        # TODO offer/accept protocol

        # This iterates to balance the remaining capacity equally across flows
        amounts = {flow:0 for flow, _ in traffic}
        #print(amounts)
        change = 1
        while change > 0:
            change = 0

            # TODO, not just +1, do it the (faster) divide way
            for flow, b in traffic:
                flow_src, flow_dst = flow
                current = amounts[flow]
                #assert capacities[flow_dst] <= self.packets_per_slot
                new = bound(0, current+1, min(b.size, remaining, capacities[flow_dst]))
                amounts[flow] = max(new, current)

                # Update tracking vars
                delta = amounts[flow] - current
                change   += delta
                remaining -= delta

        #print(amounts)

        # Send the amounts we decided on
        for flow, amount in amounts.items():
            if amount == 0 and remaining > 0 and False:
                print("%s: flow %s not sending to %s via %s (remaining %s, %s.capacity[%s]= %s)" %
                        (self, flow, dst.id, rotor_id, remaining, dst.id, flow_dst, capacities[flow_dst]))

            self.send(
                    rotor_id = rotor_id,
                    flow     = flow,
                    amount   = amount)

    def compute_capacity(self):
        capacity = dict()
        #capacity[self.id] = self.packets_per_slot
        for flow, b in self.buffers.items():
            src, dst = flow
            if dst == self.id:
                continue

            current = capacity.get(dst, self.packets_per_slot)
            current -= b.size
            capacity[dst] = current

        return capacity


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

