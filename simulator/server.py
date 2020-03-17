from params import PARAMS

class Flow:
    """Super class for different flow types"""
    def __init__(self, flow_desc, receiver):
        self.flow_desc = flow_desc
        self.receiver  = receiver

    def set_callback_done(self, fn):
        self.callback_done = fn

    def recv(self, packet):
        if False:
            self.callback_done(self.id)
        raise NotImplementedError

    def start(self):
        pass

class TCPFlow(Flow):
    def __init__(self, flow_desc, receiver):
        super().__init__(flow_desc, receiver)

        # Init our TCP fields
        self.cwnd = 1
        self.rtt  = None

        # TODO
        self.alpha = .5
        self.beta  = .5

        self.in_flight = []

    def recv(self, packet):
        next_packet = None
        self.receiver.recv(next_packet)

    def timeout(self, packet):
        self.cwnd /= 2
        self.send(packet)
        pass


class Server:
    def __init__(self, server_id):
        self.id = server_id
        self.flows = dict()

    def connect_tor(self, rack_port)
        # We can just bypass the host queue and go straight to the rack
        # The rate limiting is done in the draining Queue
        self.out_queue = rack_port

    def incoming(self, packet):
        """For reaveiving packets from the outside"""
        flow_id = packet.flow_id

        if flow_id in self.flows:
            # This is okay, maybe a flow is over and stragglers are coming
            self.flows[flow_id].recv(packet)

    def flow_done(self, flow_id):
        del self.flows[flow_id]

    def start_flow(self, flow_desc):
        flow = TCPFlow(flow, self.out_queue)
        self.flows[flow_desc.flow_id] = flow

        flow.set_callback_done(self.flow_done)

        flow.start()

