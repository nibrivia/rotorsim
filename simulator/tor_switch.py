import math
import heapq
from logger import LOG
from helpers import get_port_type, rotor_ports, cache_ports, xpand_ports, vprint, color_str_, pause
from event import Delay, R
from functools import lru_cache
from collections import deque
from flow_generator import FLOWS
from params import PARAMS
from debuglog import DebugLog


class ToRSwitch(DebugLog):
    def __init__(self, name):

        # Stuff about me
        self.id   = int(name)
        self.name = "Tor %d" % self.id

        # ... about others
        self.switches = [None for _ in range(PARAMS.n_switches)]
        self.local_dests = dict()

        # transmit queue an dest
        self.ports_tx  = [None for _ in range(PARAMS.n_switches)]
        self.ports_dst = [None for _ in range(PARAMS.n_switches)]


        # TODO parametrize types of traffic
        tags = ["xpand", "rotor", "rotor-old", "cache"]
        self.buffers_dst_type = [{t: deque() for t in tags} for _ in range(PARAMS.n_tor)]
        self.buffers_dst_type_sizes = [{t: 0 for t in tags} for _ in range(PARAMS.n_tor)]
        self.available_ports = set()

        # cache
        self.have_cache_to = set() # The set of ToR IDs we have a cache link to
        self.will_have_cache_to = set() # The set of ToR IDs we have a cache link to

        # rotor
        self.capacities = [0    for _ in range(PARAMS.n_tor)] # of destination
        self.capacity   = [PARAMS.packets_per_slot for _ in range(PARAMS.n_tor)]

        # xpander
        self.dst_to_port = dict() # routing table
        self.dst_to_tor  = dict() # for virtual queue purposes
        self.tor_to_port = dict() # for individual moment decisions

        # optimizations
        self.nonempty_rotor_dst = set() # non-empty rotor queues
        self.possible_tor_dsts = dict() # "shortcut" map


        self.priorities = dict(
                xpand = ["xpand", "rotor", "cache"],
                rotor = ["rotor", "xpand", "cache"],
                cache = ["cache", "xpand", "rotor"],
                )
        self.pull_fns = dict(
                xpand = self.next_packet_xpand,
                rotor = self.next_packet_rotor,
                #cache = self.next_packet_cache
                )


    # One-time setup
    ################

    def connect_backbone(self, port_id, switch, queue):
        # queue is an object with a .recv that can be called with (packets)
        #vprint("%s: %s connected on :%d" % (self, switch, port_id))
        self.switches[port_id] = switch
        self.ports_tx[port_id] = queue
        self.available_ports.add(port_id)
        queue.empty_callback = self.make_pull(port_id)


    def connect_server(self, server, queue):
        # This will be the next port_id
        self.local_dests[server.id] = len(self.ports_dst)

        self.ports_dst.append(server)
        self.ports_tx.append(queue)


    def add_rotor_matchings(self, matchings_by_slot_rotor):
        self.matchings_by_slot_rotor = [[ None for _ in m]
                for m in matchings_by_slot_rotor]
        for slot, matchings_by_rotor in enumerate(matchings_by_slot_rotor):
            assert len(matchings_by_rotor) == PARAMS.n_rotor, \
                    "Got %s, expected %s" % (
                            len(matchings_by_rotor), PARAMS.n_rotor)
            for rotor_id, matchings in enumerate(matchings_by_rotor):
                for src, dst in matchings:
                    if src.id == self.id:
                        self.matchings_by_slot_rotor[slot][rotor_id] = dst

        self.n_slots = len(matchings_by_slot_rotor)

    def add_xpand_matchings(self, xpand_matchings):
        assert len(xpand_matchings) == PARAMS.n_xpand

        self.xpand_matchings = xpand_matchings

        for port_id, dst_tor in xpand_matchings.items():
            self.ports_dst[port_id] = dst_tor
            self.tor_to_port[dst_tor.id] = port_id


    def set_tor_refs(self, tors):
        self.tors = tors

    def start(self):
        """Call once at setup"""
        # Rotor
        #######

        # This is the first time, we need to connect everyone
        slot_t = 0
        matchings_in_effect = self.matchings_by_slot_rotor[slot_t % PARAMS.n_slots]

        # For all active matchings, connect them up!
        for rotor_id in rotor_ports:
            dst = matchings_in_effect[rotor_id]
            self.connect_to(rotor_id, dst)

        # Set a countdown for the next slot, just like normal
        if PARAMS.slot_duration is not None:
            self.slot_id = 0
            self.new_slice = Delay(PARAMS.slot_duration  + PARAMS.reconfiguration_time, priority = 1000)(self.new_slice)
        #if PARAMS.slice_duration is not None:
        #    self.new_slice = Delay(self.slice_duration + self.reconf_time, priority = 1000)(self.new_slice)
        R.call_in(0, self.make_route, priority = -10)
        R.call_in(0, self.new_slice, priority = -1)
        R.call_in(0, self._send, priority = 10)

        # Expander
        ##########

        # This only iterates over the very beginning of the connections: the rotors
        # for port_id, tor in self.xpand_matchings.items():
        #     self.tor_to_port[tor.id] = port_id


    # Every slice setup
    ###################

    #@property
    #def slice_t(self):
        #assert PARAMS.slice_duration is not None
        #return round(R.time/(PARAMS.slice_duration + PARAMS.reconfiguration_time))

    @property
    def slot_t(self):
        assert PARAMS.slot_duration is not None
        return round(R.time/(PARAMS.slot_duration + PARAMS.reconfiguration_time))

    def new_slice(self):
        """Starts a new slice"""
        # Switch up relevant matching
        #if PARAMS.slice_duration is not None:
        #    slot_t = self.slice_t // PARAMS.n_rotor
        #    matchings_in_effect = self.matchings_by_slot_rotor[self.slot_t % self.n_slots]

        #    rotor_id = self.slice_t % PARAMS.n_rotor
        #    dst = matchings_in_effect[rotor_id]
        #    self.connect_to(rotor_id, dst)

        # If Rotor
        #vprint("%s: capacity %s" % (self, self.capacity))
        if PARAMS.slot_duration is not None:
            self.slot_id = self.slot_t % self.n_slots
            #vprint("%.3f %s switch to slot_id %d" % (R.time, self, self.slot_id))
            matchings_in_effect = self.matchings_by_slot_rotor[self.slot_id]
            for rotor_id in rotor_ports:
                dst = matchings_in_effect[rotor_id]
                self.connect_to(rotor_id, dst)

        # Set a countdown for the next slot
        self.new_slice() # is a delay() object

    #@Delay(0, priority = 10)
    def connect_to(self, port_id, tor):
        """This gets called for every rotor and starts the process for that one"""
        # Set the connection
        #vprint("%s:%d -> %s" % (self, port_id, tor))
        self.ports_dst[port_id] = tor
        self.ports_tx[port_id].resume()
        R.call_in(PARAMS.slot_duration - .002,
                self.disconnect_from, port_id, priority = -1)

        # Get capacities for indirection if rotor
        if port_id < PARAMS.n_rotor:
            self.capacities[port_id] = tor.capacity

        # Start sending
        #self._send(port_id)

    def disconnect_from(self, port_id):
        self.ports_tx[port_id].pause()
        if port_id in self.available_ports:
            self.available_ports.remove(port_id)
        #vprint("%s: available ports: %s" % (self, self.available_ports))



    @property
    @lru_cache(maxsize = None)
    def link_state(self):
        links = dict()
        for port_id in xpand_ports:
            tor = self.ports_dst[port_id]
            links[tor.id] = 1
        return links

    # By having a delay 0 here, this means that every ToR will have gone
    # through its start, which will then mean that we can call link_state
    #@Delay(0, priority = -10)
    def make_route(self, slice_id = None):
        """Builds a routing table"""
        for t in self.tors:
            for dst_id in t.local_dests:
                self.dst_to_tor[dst_id] = t.id

        if PARAMS.n_xpand == 0:
            return

        self.route_tor = [(None, PARAMS.n_tor*1000) for _ in range(PARAMS.n_tor)]
        self.route_tor[self.id] = ([], 0)
        queue = deque()
        queue.append(self)

        #This is a bastardized dijkstra - it assumes all cost are one
        while len(queue) > 0:
            tor    = queue.popleft()
            path, cost = self.route_tor[tor.id]

            # Take the new connection...
            for con_id in tor.link_state:
                cur_path, cur_cost = self.route_tor[con_id]
                con_tor = self.tors[con_id]
                # see if it does better...
                if cost+1 < cur_cost:
                    # update the cost and add back to the queue
                    self.route_tor[con_id] = (path + [con_id], cost+1)
                    queue.append(con_tor)

        self.route = dict()
        for dst_tor_id, (path, _) in enumerate(self.route_tor):
            # Local destination, skip
            if dst_tor_id == self.id:
                continue

            # Figure out what the next path is...
            try:
                next_tor = path[0]
            except:
                print()
                print(self.route_tor)
                print(self, dst_tor_id, path)
                print()
                raise
            next_port_id = self.tor_to_port[next_tor]

            # Write that for each server at our destination
            dst_tor = self.tors[dst_tor_id]
            for dst in dst_tor.local_dests:
                # This is just for expander
                self.dst_to_port[dst] = next_port_id



    # SENDING ALGORITHMS
    ####################

    def next_packet_rotor(self, port_id, dst_tor_id):
        """Sends over a lump"""
        if PARAMS.n_rotor == 0:
            return None
        dst   = self.tors[dst_tor_id]


        #for _dst_tor_id in range(PARAMS.n_tor):
        if dst_tor_id in self.nonempty_rotor_dst:
            assert self.buffers_dst_type_sizes[dst_tor_id]["rotor"] > 0, \
                "%s: %s should be >0 %s" % (self,
                    self.nonempty_rotor_dst, self.buffers_dst_type_sizes[dst_tor_id])
        else:
            assert self.buffers_dst_type_sizes[dst_tor_id]["rotor"] == 0, \
                "%s: %s (nonempty: %s) should be ==0 %s" % (self,
                    dst_tor_id,
                    self.nonempty_rotor_dst, self.buffers_dst_type_sizes[dst_tor_id])


        # Old indirect traffic goes first
        old_queue    = self.buffers_dst_type[dst_tor_id]["rotor-old"]
        old_queue_sz = self.buffers_dst_type_sizes[dst_tor_id]["rotor-old"]
        if old_queue_sz > 0:
            self.capacity[dst_tor_id] += 1
            self.buffers_dst_type_sizes[dst_tor_id]["rotor-old"] -= 1
            return old_queue.popleft()

        # Direct traffic
        dir_queue    = self.buffers_dst_type[dst_tor_id]["rotor"]
        dir_queue_sz = self.buffers_dst_type_sizes[dst_tor_id]["rotor"]
        if dir_queue_sz > 0:
            self.capacity[dst_tor_id] += 1
            if dir_queue_sz == 1:
                #vprint("%s: direct to %s" % (self, dst_tor_id))
                self.nonempty_rotor_dst.remove(dst_tor_id)
            self.buffers_dst_type_sizes[dst_tor_id]["rotor"] -= 1
            return dir_queue.popleft()

        # New indirect
        for ind_target in self.nonempty_rotor_dst:
            new_queue    = self.buffers_dst_type[ind_target]["rotor"]
            new_queue_sz = self.buffers_dst_type_sizes[ind_target]["rotor"]
            #if ind_target == dst_tor_id: # Should already be empty if we're here...
            #    assert len(new_queue) == 0

            if new_queue_sz > 0 and dst.capacity[ind_target] > 0:
                #vprint("%s: sending indirect %s.capacity[%s] = %s" % 
                        #(self, dst_tor_id, dst.capacity)
                #print("%s: indirect to %s (%s)" % (self, ind_target, new_queue_sz))
                self.capacity[ind_target] += 1
                if new_queue_sz == 1:
                    self.nonempty_rotor_dst.remove(ind_target)
                    #print("%s: %s removed -> %s" % (self, ind_target, self.nonempty_rotor_dst))

                self.buffers_dst_type_sizes[ind_target]["rotor"] -= 1
                return new_queue.popleft()

        return None


    def next_packet_xpand(self, port_id, dst_tor_id):
        """Given a connection to a certain destination, give a packet
        that we can either shortcut, or is equivalent, to something we'd
        normally do on expander..."""
        # Get destinations that go that way
        #vprint("%s: xpand :%s -> Tor #%s" % (self, port_id, dst_tor_id))
        #vprint(self.route_tor)
        if dst_tor_id in self.possible_tor_dsts:
            possible_tor_dsts = self.possible_tor_dsts[dst_tor_id]
        else:
            if PARAMS.n_xpand > 0:
                possible_tor_dsts = set(
                        tor_id
                        for tor_id, (path, _) in enumerate(self.route_tor)
                                if dst_tor_id in path)
                #possible_tor_dsts = set(self.dst_to_tor[dst]
                        #for dst, p in self.dst_to_port.items() if p == port_id)
            else:
                # No expander, literally anything is better...
                possible_tor_dsts = set(t for t in range(PARAMS.n_tor))

            self.possible_tor_dsts[dst_tor_id] = possible_tor_dsts
        #vprint(possible_tor_dsts)

        # Get all packets that wanna go that way
        possible_pkts = []
        cur_min = float("inf")
        dst = None
        for d in possible_tor_dsts:
            if self.buffers_dst_type_sizes[d]["xpand"] > 0:
                d_min = self.buffers_dst_type[d]["xpand"][0]._tor_arrival
                if d_min < cur_min:
                    cur_min = d_min
                    dst = d

        # Find the earliest one
        if dst is not None:
            pkt = self.buffers_dst_type[dst]["xpand"].popleft()
            #if pkt.flow_id == PARAMS.flow_print:
                #print("%s: %s -> %s (route: %s" % (self, pkt, dst_tor_id, self.route_tor[dst]))
            self.buffers_dst_type_sizes[dst]["xpand"] -= 1
            return pkt


    # Actual packets moving
    ########################

    def make_pull(self, port_id):
        port_type = get_port_type(port_id)
        def pull():
            #vprint("%s: pull from port %s" % (self, port_id))
            self.available_ports.add(port_id)
            self._send([port_id])
        return pull

    def activate_cache_link(self, port_id, dst_tor_id):
        if self.id == 26:
            vprint("%s: activate :%d -> %s" % (self, port_id, dst_tor_id))
        self.ports_dst[port_id] = self.tors[dst_tor_id]
        self.have_cache_to.add(dst_tor_id)
        self._send()

    def deactivate_cache_link(self, tor_dst_id):
        def deactivate(flow_id):
            vprint("%s: release cache link to %s" % (self, tor_dst_id))
            # TODO mechanism to allow multiple flows to the same dst to use the same cache link
            self.have_cache_to.discard(tor_dst_id)
            self.will_have_cache_to.discard(tor_dst_id)
        return deactivate

    @classmethod
    @lru_cache(maxsize=None)
    def packet_tag(c, cur_tag):
        if cur_tag == "cache":
            if PARAMS.n_cache > 0:
                return "cache"
            if PARAMS.n_rotor > 0:
                return "rotor"
            if PARAMS.n_xpand > 0:
                return "xpand"

        if cur_tag == "rotor":
            if PARAMS.n_rotor > 0:
                return "rotor"
            if PARAMS.n_xpand > 0:
                return "xpand"
            if PARAMS.n_cache > 0:
                return "cache"

        if cur_tag == "xpand":
            if PARAMS.n_xpand > 0:
                return "xpand"
            if PARAMS.n_rotor > 0:
                return "rotor"
            if PARAMS.n_cache > 0:
                return "cache"


    def recv(self, packet):
        """Receives packets for `port_id`"""

        if packet.flow_id == PARAMS.flow_print:
            vprint("%s: %s recv" % (self, packet))

        # Sanity check
        if packet.intended_dest != None:
            assert packet.intended_dest == self.id, \
                "@%.3f %s received %s, was intendd for %s" % (R.time, self, packet, packet.intended_dest)

        # Update hop count
        packet.hop_count += 1
        assert packet.hop_count < 50, "Hop count >50? %s" % packet


        # Deliver locally
        if packet.dst_id in self.local_dests:
            if packet.flow_id == PARAMS.flow_print:
                vprint("%s: %s Local destination" % (self, packet))

            next_port_id = self.local_dests[packet.dst_id]
            self.ports_tx[next_port_id].enq(packet)
        else:
            packet._tor_arrival = R.time
            next_tor_id = self.dst_to_tor[packet.dst_id]



            dst_tag = ToRSwitch.packet_tag(packet.tag)

            # CACHE handling
            if packet.src_id in self.local_dests and dst_tag == "cache" and next_tor_id not in self.will_have_cache_to:
                for port_id in cache_ports:
                    if self.ports_dst[port_id] is None:
                        if self.switches[port_id].request_matching(self, next_tor_id):
                            # Stops us from requesting this again
                            self.will_have_cache_to.add(next_tor_id)
                            R.call_in(15, self.activate_cache_link, port_id, next_tor_id)
                            FLOWS[packet.flow_id].add_callback_done(self.deactivate_cache_link(next_tor_id))
                            break

            # If we don't have a cache yet, make it rotor
            if dst_tag == "cache" and next_tor_id not in self.have_cache_to:
                dst_tag = "rotor"

            # TODO can just enqueue right here?
            #if dst_tag == "cache":
                #vprint("%s %s going to cache" % (self, packet))

            # ROTOR requires some handling...
            # ...adapt our capacity on rx
            if dst_tag == "rotor":
                self.capacity[next_tor_id] -= 1

                # ... if indirect, put it in higher queue...
                if packet.src_id not in self.local_dests:
                    if packet.flow_id == PARAMS.flow_print:
                        vprint("%s: %s is old indirect" % (self, packet))
                    dst_tag = "rotor-old"
                else:
                    self.nonempty_rotor_dst.add(next_tor_id)

            self.buffers_dst_type[next_tor_id][dst_tag].append(packet)
            self.buffers_dst_type_sizes[next_tor_id][dst_tag] += 1


            # debug print
            if packet.flow_id == PARAMS.flow_print:
                vprint("%s: %s Outer destination %s/%s (%d)" % (
                    self, packet, next_tor_id, dst_tag,
                    len(self.buffers_dst_type[next_tor_id][packet.tag])))

            # trigger send loop
            buf = self.buffers_dst_type[next_tor_id][dst_tag]
            sz  = self.buffers_dst_type_sizes[next_tor_id][dst_tag]
            #assert len(buf) == sz, "%s: recv buffer[%s][%s] size %s, recorded %s" % (self, next_tor_id, dst_tag, len(buf), sz)
            self._send()



    def _send(self, ports = None):
        #vprint("%s: _send()" % self)

        if ports is None:
            ports = list(self.available_ports)

        #vprint("%s: available ports: %s" % (self, self.available_ports))
        for priority_i in range(3):
            for free_port in ports:
                port_type = get_port_type(free_port)
                dst = self.ports_dst[free_port]
                if dst is None:
                    continue
                port_dst  = self.ports_dst[free_port].id
                buffers_type = self.buffers_dst_type[port_dst]

                priority_type = self.priorities[port_type][priority_i]
                buf = buffers_type[priority_type]
                sz  = self.buffers_dst_type_sizes[port_dst][priority_type]
                # assert len(buf) == sz, "%s: buffer[%s][%s] size %s, recorded %s" % (self, port_dst, priority_type, len(buf), sz)

                if False and self.id == 32:
                    vprint("%s:   :%s (%s) considering %s/%s (%d)..." % (
                            self,
                            free_port, port_type,
                            port_dst, priority_type,
                            sz
                            #end = ""
                            ))

                pkt = None
                if priority_type in self.pull_fns:
                    # Eventually should all be here, for now, not all implemented...
                    pkt = self.pull_fns[priority_type](port_id = free_port, dst_tor_id = port_dst)
                elif sz > 0:
                    #vprint(" has packets!")
                    pkt = buf.popleft()
                    self.buffers_dst_type_sizes[port_dst][pkt.tag] -= 1

                if pkt is not None and free_port in self.available_ports:
                    pkt.intended_dest = port_dst
                    if pkt.flow_id == PARAMS.flow_print: # or self.id == 16:
                        vprint("%s: sending %s on :%s -> %s" % (self, pkt, free_port, port_dst))
                    self.ports_tx[free_port].enq(pkt)
                    self.available_ports.remove(free_port)
                    pkt_tor_dst = self.dst_to_tor[pkt.dst_id]



    # Printing stuffs
    ################

    @color_str_
    def __str__(self):
        return self.name

