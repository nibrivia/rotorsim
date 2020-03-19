from params import PARAMS
from event import R
from flow import Flow, Packet

class Server:
    def __init__(self, server_id):
        self.id = server_id
        self.flows = dict()

    def connect_tor(self, uplink):
        # We can just bypass the host queue and go straight to the rack
        # The rate limiting is done in the draining Queue
        self.uplink = uplink
        print(uplink)

    def recv(self, packet):
        """For reaveiving packets from the outside"""
        flow_id = packet.flow_id

        if flow_id in self.flows:
            # This is okay, maybe a flow is over and stragglers are coming
            self.flows[flow_id](packet)

    def flow_done(self, flow_id):
        del self.flows[flow_id]

    def add_flow(self, flow, receiver):
        self.flows[flow.id] = receiver
        flow.add_callback_done(self.flow_done)

