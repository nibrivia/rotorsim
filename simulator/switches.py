import sys
from buffer import *
from helpers import *


class ToRSwitch:
    def __init__(self, name, n_tor, n_rotor, packets_per_slot, logger, verbose):
        # Index by who to send to
        self.id = int(name)
        self.n_tor = n_tor
        self.n_rotor = n_rotor
        self.packets_per_slot = packets_per_slot

        self.buffers = { (src, dst) : Buffer(
            name = "%s.%s->%s" % (self.id, src, dst),
            logger = logger,
            verbose = verbose) for src in range(n_tor) for dst in range(n_tor) }

        # Watch out, some might be (intentional) duplicates
        # each item has the form (tor, link_remaining)
        self.connections = dict()
        self.offers = dict()
        self.capacity = self.compute_capacity()


    def print_capacity(self):
        return "%s\nstate: %s\ncompu: %s" % (self, self.capacity, self.compute_capacity())

    def add_demand_to(self, dst, amount):
        if dst.id != self.id:
            self.buffers[(self.id, dst.id)].add_n(amount)
            self.capacity[dst.id] -= amount

    def send(self, rotor_id, flow, amount):
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

        # Return remaining link capacity
        return link_remaining

    def recv(self, dst_id, amount):
        if dst_id != self.id:
            self.capacity[dst_id] -= amount

    def connect_to(self, rotor_id, tor):
        self.connections[rotor_id] = (tor, self.packets_per_slot)

        # TODO when this is more decentralized
        if False:
            self.offer()
            self.accept()
            self.send_old_indirect()
            self.send_direct()
            self.send_new_indirect()


    def indirect_traffic_to(self, dst):
        # Returns indirect traffic to dst
        return {(s, d) : b for (s, d), b in self.buffers.items()
                    if d == dst.id and s != self.id and b.size > 0}

    def direct_traffic(self, dst):
        # Returns direct traffic except to dst
        return {(s, d) : b for (s, d), b in self.buffers.items()
                    if s == self.id and d != dst.id and b.size > 0}

    def send_old_indirect(self):
        for rotor_id, (dst, remaining) in self.connections.items():

            # All indirect traffic to dst
            buffers = self.indirect_traffic_to(dst)

            # Verify link violations
            total_send = sum(b.size for b in buffers.values())
            assert total_send <= self.n_rotor*self.packets_per_slot, \
                    "%s->%s old indirect %d > capacity" % (self, dst, total_send)

            # Send the data
            sent = 1
            while sent > 0:
                sent = 0
                for flow, b in buffers.items():
                    if b.size == 0:
                        continue
                    if self.connections[rotor_id][1] == 0:
                        break
                    self.send(rotor_id = rotor_id,
                              flow     = flow,
                              amount   = 1)
                    sent += 1

    def send_direct(self):
        # For each connection (some may be intentional duplicates)
        #print()
        for rotor_id, (dst, remaining) in self.connections.items():
            dst, remaining = self.connections[rotor_id]
            #print("         %s to %s via %s remaining %s" % (self, dst, rotor_id, remaining))
            flow = (self.id, dst.id)
            n_sending = bound(0, self.buffers[flow].size, remaining)
            self.send(rotor_id = rotor_id,
                      flow = flow,
                      amount = n_sending)

    def send_new_indirect(self):
        for rotor_id, (dst, remaining) in self.connections.items():
            # Get indirect-able traffic
            traffic = self.direct_traffic(dst)
            capacities = dst.capacity

            # Send what we can along the remaining capacity
            # TODO offer/accept protocol

            # This iterates to balance the remaining capacity equally across flows
            amounts = {flow:0 for flow in traffic}
            #print(amounts)
            change = 1
            while change > 0:
                change = 0

                # TODO, not just +1, do it the (faster) divide way
                for flow, b in traffic.items():
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

