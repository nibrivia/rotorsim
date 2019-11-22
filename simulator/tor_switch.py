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

        self.buffers = { (src, dst) : Buffer(parent = self,
                                             src = src, dst = dst,
                                             logger = logger,
                                             verbose = verbose)
                                 for src in range(n_tor) for dst in range(n_tor)
                                 if src != self.id and dst != self.id}

        for tor in range(n_tor):
            self.buffers[(self.id, tor)] = SourceBuffer(
                    parent = self,
                    src = self.id, dst = tor,
                    logger = logger,
                    verbose = verbose)
            self.buffers[(tor, self.id)] = DestBuffer(
                    src = tor, dst = self.id,
                    logger = logger,
                    verbose = verbose)

        # Watch out, some might be (intentional) duplicates
        # each item has the form (tor, link_remaining)
        self.connections = dict()
        self.capacity = self.compute_capacity()

        # Compute useful buffer sets now
        self.indirect_for = [self.indirect_traffic_for(n) for n in range(n_tor)]
        self.indirect_at  = [self.indirect_traffic_at( n) for n in range(n_tor)]

    def indirect_traffic_for(self, dst_id):
        # Returns indirect traffic to dst
        flows = [(src_id, dst_id) for src_id in range(self.n_tor) if src_id != self.id]
        return [(f, self.buffers[f]) for f in flows]

    def indirect_traffic_at(self, dst_id):
        # Returns direct traffic except to dst
        flows = [(self.id, d_id) for d_id in range(self.n_tor) if d_id != dst_id]
        return [(f, self.buffers[f]) for f in flows]

    def vprint(self, msg="", level = 0):
        if self.verbose:
            pad = "  " * level
            print("%s%s" % (pad, msg))

    def add_demand_to(self, dst, amount):
        if dst.id != self.id:
            self.buffers[(self.id, dst.id)].add_n(amount)
            self.capacity[dst.id] -= amount
            self.tot_demand += amount

    def connect_rotor(self, rotor, handle):
        self.rotors[rotor.id] = handle
        print("%s: connected to %s" % (self, rotor))


    def add_matchings(self, matchings_by_slot_rotor):
        self.matchings_by_slot_rotor = matchings_by_slot_rotor

    def send(self, rotor_id, flow, amount):
        if amount == 0:
            return

        dst, link_remaining = self.connections[rotor_id]

        # Check link capacity
        assert link_remaining >= amount, \
            "%s, flow%s, rotor%s attempting to send %d, but capacity %s" % (
                self, flow, rotor_id, amount, link_remaining)


        # Move the actual packets
        self.buffers[flow].send_to(self.rotors[rotor_id], amount, rotor_id)

        # Update link remaining
        link_remaining -= amount
        self.connections[rotor_id] = (dst, link_remaining)

        # Update our + destination capacity
        flow_src, flow_dst = flow
        #dst.recv(flow_dst, amount)
        self.capacity[flow_dst] += amount

        self.tot_demand -= amount

        # Return remaining link capacity
        return link_remaining


    def recv(self, rotor_id, packets):
        for flow_src, flow_dst, seq_num in packets:
            flow = (flow_src, flow_dst)
            if flow_dst != self.id:
                amount = len(packets)
                self.tot_demand += amount
                self.capacity[flow_dst] -= amount
            self.buffers[flow].recv(packets)


    def new_slot(self):
        print("%.2f" % R.time)
        self.slot_t += 1
        matchings_in_effect = self.matchings_by_slot_rotor[self.slot_t % 3]
        for rotor_id, matchings in enumerate(matchings_in_effect):
            for src, dst in matchings:
                if src.id == self.id:
                    self.connect_to(rotor_id, dst)
        Delay(self.slot_duration, jitter = self.clock_jitter)(self.new_slot)()


    def connect_to(self, rotor_id, tor):
        self.connections[rotor_id] = (tor, self.packets_per_slot)
        print("%s: Connected to %s via %s" % (self, tor, rotor_id))

        # TODO when this is more decentralized
            #self.offer()
            #self.accept()
        self.send_old_indirect(rotor_id)
        self.send_direct(rotor_id)
        self.send_new_indirect(rotor_id) 

    @Delay(0, priority = 1)
    def send_old_indirect(self, rotor_id):
        self.vprint("Old Indirect: %s:%d" % (self, rotor_id), 2)

        # Get connection data
        dst, remaining = self.connections[rotor_id]

        # All indirect traffic to dst
        buffers = self.indirect_for[dst.id]

        # Verify link violations
        if False:
            total_send = sum(b.size for _, b in buffers)
            assert total_send <= self.n_rotor*self.packets_per_slot, \
                    "%s->%s old indirect %d > capacity" % (self, dst, total_send)

        # Send the data
        sent = 1
        while sent > 0 and remaining > 0:
            sent = 0
            for flow, b in buffers:
                if b.size == 0:
                    continue
                self.send(rotor_id = rotor_id,
                          flow     = flow,
                          amount   = 1)
                sent += 1
                remaining -= 1

                if remaining == 0:
                    break

    @Delay(0, priority = 2)
    def send_direct(self, rotor_id):
        self.vprint("Direct: %s:%d" % (self, rotor_id), 2)

        # Get connection data
        dst, remaining = self.connections[rotor_id]
        flow = (self.id, dst.id)
        amount = min(self.buffers[flow].size, remaining)

        self.send(rotor_id = rotor_id,
                  flow = flow,
                  amount = amount)

    @Delay(0, priority = 3)
    def send_new_indirect(self, rotor_id):
        self.vprint("New Indirect: %s:%d" % (self, rotor_id), 2)
        # Get connection data
        dst, remaining = self.connections[rotor_id]

        # Get indirect-able traffic
        traffic = self.indirect_at[dst.id]
        capacities = dst.capacity

        # Send what we can along the remaining capacity
        # TODO offer/accept protocol

        # This iterates to balance the remaining capacity equally across flows
        amounts = [0 for f, _ in traffic]
        #print(amounts)
        change = 1
        while change > 0 and remaining > 0:
            change = 0

            # TODO, not just +1, do it the (faster) divide way
            for i, (flow, b) in enumerate(traffic):
                flow_src, flow_dst = flow
                current = amounts[i]
                #assert capacities[flow_dst] <= self.packets_per_slot
                new = min(current+1, b.size, remaining, capacities[flow_dst])
                amounts[i] = max(new, current)

                # Update tracking vars
                delta = amounts[i] - current
                change    += delta
                remaining -= delta

                if remaining == 0:
                    break

        #print(amounts)

        # Send the amounts we decided on
        for i, amount in enumerate(amounts):
            flow = traffic[i][0]
            if amount == 0 and remaining > 0 and False:
                print("%s: flow %s not sending to %s via %s (remaining %s, %s.capacity[%s]= %s)" %
                        (self, flow, dst.id, rotor_id, remaining, dst.id, flow[1], capacities[flow_dst]))

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


