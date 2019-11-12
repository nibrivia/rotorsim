import sys
from buffer import *
from timetracker import *
from helpers import *


PACKETS_PER_SLOT = 5

class ToRSwitch:
    def __init__(self, name, n_tor, n_rotor, logger, verbose):
        # Index by who to send to
        self.id = int(name)
        self.n_tor = n_tor

        """
        self.outgoing =  [Buffer(
                            name = "%s.%s->%s" %(name, name, dst+1),
                            logger = logger,
                            verbose = verbose)
                for dst in range(n_tor)]
        # Index by who send to me
        self.incoming =  [Buffer(
                            name = "%s.%s->%s" % (name, src+1, name),
                            logger = logger,
                            verbose = verbose)
                for src in range(n_tor)]

        # self.indirect[dst][src]
        self.indirect = [[ Buffer(
                            name = "%s.%s->%s" % (name, src+1, dst+1),
                            logger = logger,
                            verbose = verbose)
            for src in range(n_tor)] for dst in range(n_tor)]
        """

        self.buffers = { (src, dst) : Buffer(
            name = "%s.%s->%s" % (self.id, src, dst),
            logger = logger,
            verbose = verbose) for src in range(n_tor) for dst in range(n_tor) }

        # Watch out, some might be (intentional) duplicates
        # each item has the form (tor, link_remaining)
        self.connections = dict()
        self.offers = dict()
        self.capacity = self.compute_capacity()


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
        self.buffers[flow].send_to(dst.buffers[flow], amount)

        # Update link remaining
        link_remaining -= amount
        self.connections[rotor_id] = (dst, link_remaining)

        # Update our + destination capacity
        flow_src, flow_dst = flow
        dst.recv(flow_dst, amount)
        cur_capacity = self.capacity[dst.id]
        self.capacity[dst.id] = min(cur_capacity+amount, PACKETS_PER_SLOT)

        # Return remaining link capacity
        return link_remaining

    def recv(self, dst, amount):
        if dst != self.id:
            self.capacity[dst] -= amount

    def connect_to(self, rotor_id, tor):
        self.connections[rotor_id] = (tor, PACKETS_PER_SLOT)

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
            assert total_send <= PACKETS_PER_SLOT, \
                    "%s->%s old indirect %d > capacity" % (self, dst, total_send)

            # Send the data
            for flow, b in buffers.items():
                self.send(rotor_id = rotor_id,
                          flow     = flow,
                          amount   = b.size)

    def send_direct(self):
        # For each connection (some may be intentional duplicates)
        for rotor_id, (dst, remaining) in self.connections.items():
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
            change = 1
            while change > 0:
                change = 0

                # TODO, not just +1, do it the (faster) divide way
                for flow, b in traffic.items():
                    flow_src, flow_dst = flow
                    current = amounts[flow]
                    assert capacities[flow_dst] <= PACKETS_PER_SLOT
                    new = bound(0, current+1, min(b.size, remaining, capacities[flow_dst]))
                    amounts[flow] = new

                    # Update tracking vars
                    delta = new - current
                    change    += delta
                    remaining -= delta


            # Send the amounts we decided on
            for flow, amount in amounts.items():
                remaining = self.send(
                        rotor_id = rotor_id,
                        flow     = flow,
                        amount   = amount)

    def compute_capacity(self):
        capacity = dict()
        #capacity[self.id] = PACKETS_PER_SLOT
        for flow, b in self.buffers.items():
            src, dst = flow
            if dst == self.id:
                continue

            current = capacity.get(dst, PACKETS_PER_SLOT)
            current -= b.size
            capacity[dst] = current

        return capacity


    def offer(self):
        # TODO offer
        return self.capacity()

    def offer_rx(self, rotor_id, offer, capacity):
        self.offers[rotor_id] = offer
        self.capacities[rotor_id] = capacity

    def __str__(self):
        return "ToR %s" % self.id


"""
class RotorSwitch:
    def __init__(self, tors, name):
        self.name = name
        self.tors = tors

    def init_slot(self, matchings):
        raise Error
        # Reset link availabilities
        self.remaining = {link: PACKETS_PER_SLOT for link in matchings}
        for src, dst in matchings:
            self.tors[src-1].connect_to(dst)
        self.matchings = matchings

    def send_old_indirect(self):
        return

        vprint("      %s" % self)
        # For each matching, look through our buffer, deliver old stuff
        for link in self.matchings:
            ind_i, dst_i = link
            ind,   dst   = self.tors[ind_i], self.tors[dst_i]
            s = sum(b.size for b in ind.indirect[dst_i])
            assert s <= PACKETS_PER_SLOT, "More old indirect than can send %s" % self

            # For this link, find all old indirect traffic who wants to go
            for dta_src, ind_buffer in enumerate(ind.indirect[dst_i]):
                # How much can we send?
                amount = bound(0, ind_buffer.size, self.remaining[link])

                # Actually send
                ind_buffer.send(dst.incoming[dta_src], amount)
                self.remaining[link] -= amount

    def send_direct(self):
        raise Error
        vprint("      %s" % self)

        for link in self.matchings:
            src_i, dst_i = link
            src,   dst   = self.tors[src_i], self.tors[dst_i]

            # How much to send?
            amount = bound(0, src.outgoing[dst_i].size, self.remaining[link])

            # Actually send
            src.outgoing[dst_i].send(dst.incoming[src_i], amount)
            self.remaining[link] -= amount


    def send_new_indirect(self):
        return

        vprint("      %s" % self)

        # Because sending traffic indirectly then allows us to receive more,
        # we need to keep iterating until we can't send any more
        sent = 1
        while sent > 0:
            sent = 0
            # Go through matchings randomly, would be better if fair
            for link in shuffle(self.matchings):
                src_i, ind_i = link
                src,   ind   = self.tors[src_i], self.tors[ind_i]

                # If we still have demand, indirect it somewhere
                for dst_i, src_buffer in shuffle(enumerate(src.outgoing)):
                    available = src.available_to(dst_i)
                    available = min(self.remaining[link], available)
                    amount = bound(0, src_buffer.size, available)

                    src.outgoing[dst_i].send(ind.indirect[dst_i][src_i], amount)
                    self.remaining[link] -= amount
                    sent += amount

    def __str__(self):
        return "Rotor %s" % self.name
"""
