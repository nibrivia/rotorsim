from helpers import vprint, color_str_
from params import PARAMS
from event import R
from flow import Flow, Packet
from debuglog import DebugLog

class Server(DebugLog):
    def __init__(self, server_id, server_name = None):
        self.id = server_id
        self.name = server_name
        self.flows = dict()

    def connect_tor(self, uplink):
        # Uplink should have the right delay and bandwidth
        # This should be set by the network
        self.uplink = uplink

    def recv(self, packet):
        """For reaveiving packets from the outside"""
        #vprint("%s received at %s" % (packet, self))
        flow_id = packet.flow_id
        if flow_id == PARAMS.flow_print:
            vprint("srvr : %s recv on %s" % (packet, self))

        if packet.final_dst != self.id:
            packet.dst_id = packet.final_dst
            packet.src_id = self.id
            self.uplink.enq(packet)
            return

        if flow_id in self.flows:
            # This is okay:
            # maybe a flow is over and stragglers are coming
            if flow_id == PARAMS.flow_print:
                vprint("srvr : %s recv on %s" % (packet, self))
            self.flows[flow_id](packet)
        else:
            pass
            #vprint("srvr : %s doesn't exist on %s..." % (
            #    packet, self))

    def flow_done(self, flow_id):
        del self.flows[flow_id]

    def add_flow(self, flow, receiver):
        if flow.id == PARAMS.flow_print:
            vprint("server: flow %s installed at %s" % (flow, self))
        self.flows[flow.id] = receiver
        flow.add_callback_done(self.flow_done)

    @color_str_
    def __str__(self):
        return "#%d %s" % (self.id, self.name)

