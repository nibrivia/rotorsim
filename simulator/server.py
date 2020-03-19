from helpers import vprint
from params import PARAMS
from event import R
from flow import Flow, Packet

class Server:
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

        if flow_id in self.flows:
            # This is okay, maybe a flow is over and stragglers are coming
            self.flows[flow_id](packet)

    def flow_done(self, flow_id):
        del self.flows[flow_id]

    def add_flow(self, flow, receiver):
        self.flows[flow.id] = receiver
        flow.add_callback_done(self.flow_done)

    def __str__(self):
        return "%s  (%s)" % (self.name, self.id)

