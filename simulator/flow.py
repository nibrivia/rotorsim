import math
from logger import LOG
from helpers import vprint
from flow_generator import BYTES_PER_PACKET, N_DONE, N_FLOWS, FLOWS, ML_JOBS, ML_QUEUE
from event import R
from collections import deque

class Packet:
    def __init__(self, src_id, dst_id, seq_num, tag, flow_id,
            #is_last,
            sent_ms,
            size_B = BYTES_PER_PACKET):
        self.src_id  = src_id
        self.dst_id  = dst_id
        self.seq_num = seq_num
        self.tag     = tag
        self.flow_id = flow_id
        self.sent_ms = sent_ms
        #self.is_last = is_last
        self.size_B  = size_B

        self.hop_count = 0

        self.intended_dest = None

    def __str__(self):
        return "%3d[%s->%s]#%d >%s" % (
                self.flow_id, self.src_id, self.dst_id, self.seq_num, self.intended_dest)

class Flow:
    """This runs on a server"""
    def __init__(self, arrival, flow_id, size_bits, src, dst):
        self.id        = flow_id
        self.src       = src
        self.dst       = dst
        #self.ml_id     = ml_id
        self.arrival   = arrival
        self.size_bits = size_bits

        #if size < 15e6*8:
        if size_bits < 1e6:
            self.tag = "xpand"
        elif size_bits < 1e9:
            self.tag = "rotor"
        else:
            self.tag = "cache"

        self.bits_per_packet = BYTES_PER_PACKET*8

        self.bits_left         = size_bits
        self.remaining_packets = math.ceil(size_bits/self.bits_per_packet)
        self.size_packets      = self.remaining_packets
        self.n_sent = 0
        self.n_recv = 0

        self.end = float("nan")

        # Let people know when we're done...
        self.callback_done = []

        self.packets = self.packet_gen()

    def packet_gen(self):
        bits_sent = 0
        seq_num = 0
        while bits_sent < self.size_bits:
            # FIXME UNITS!!
            p_size = min(BYTES_PER_PACKET, self.size_bits - bits_sent)
            yield Packet(
                    src_id = self.src,
                    dst_id = self.dst,
                    seq_num = seq_num,
                    tag = self.tag,
                    sent_ms = R.time,
                    flow_id =self.id,
                    size_B = p_size
                    )
            seq_num += 1


    # Functions for the congestion control class to take care of
    def src_recv(self, packet):
        raise NotImplementedError

    def dst_recv(self, packet):
        raise NotImplementedError

    def start(self):
        raise NotImplementedError

    # Connect up to the host servers
    def add_src_send(self, q):
        self.src_send_q = q

    def add_dst_send(self, q):
        self.dst_send_q = q


    def add_callback_done(self, fn):
        self.callback_done.append(fn)

    def _done(self):
        """Call this when congestion control is done"""
        for fn in self.callback_done:
            fn(self.id)

    def __str__(self):
        return "%s %4d[%3d->%3d]\033[00m" % (self.tag, self.id, self.src, self.dst)

class TCPFlow(Flow):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Init our TCP fields
        self.cwnd   = 1

        self.rtt_ms = 2
        self.rtt_dev_ms = 0

        # TODO reno setting of alpha and beta
        self.alpha = .1
        self.beta  = .2

        self.slow_start = True
        self.timeout_lock = 0

        self.in_flight = set()
        self.acked = set()
        self.retransmit_q = deque()

    # Source
    def start(self):
        self._send_loop()

    def src_recv(self, packet):
        #assert ack_packet.is_ack
        if self.id == 0:
            vprint("%s acked" % (packet))

        # Mark the ack
        self.acked.add(packet.seq_num)

        # Update rtt estimate
        rtt_sample = R.time - packet.sent_ms
        self.rtt_ms = self.alpha*rtt_sample + (1-self.alpha) * self.rtt_ms

        dev_sample = abs(self.rtt_ms - rtt_sample)
        self.rtt_dev_ms = self.beta*dev_sample + (1-self.beta) * self.rtt_dev_ms
        if self.id == 0:
            vprint("rtt/timeout: %.3f/%.3f" % (rtt_sample, self.rtt_ms + 4*self.rtt_dev_ms))


        # Remove from in-flight if necessary
        if packet.seq_num in self.in_flight:
            self.in_flight.remove(packet.seq_num)
            if self.slow_start:
                self.cwnd += 1
            else:
                self.cwnd += 1/self.cwnd
            if self.id == 0:
                vprint("cwnd", self.cwnd)

        self._send_loop()

    def _send_loop(self):
        # Get next packet, send it
        while len(self.in_flight) + 1 <= self.cwnd:
            # What packet?
            if len(self.retransmit_q) > 0:
                p = self.retransmit_q.pop()
                if self.id == 0:
                    vprint("%s retransmit" % p)
            else:
                p = next(self.packets)

            # Check it's not gotten acked...
            if p.seq_num in self.acked:
                continue

            if self.id == 0:
                vprint("%s sent, cwnd: %s/%.1f" % (p, len(self.in_flight)+1, self.cwnd))

            self.in_flight.add(p.seq_num)
            self.src_send_q.enq(p)
            #vprint(self, len(self.in_flight))

            # Setup the timeout
            R.call_in(self.rtt_ms + 4*self.rtt_dev_ms, self.timeout, p)


    def timeout(self, packet):
        if packet.seq_num in self.in_flight:
            if self.id == 0:
                vprint("%s \033[0;31mtimeout\033[0;00m" % packet)
            self.slow_start = False
            self.in_flight.remove(packet.seq_num)

            if R.time > self.timeout_lock:
                if self.id == 0:
                    vprint("%s \033[0;31m MD!!\033[0;00m" % packet)
                self.cwnd = max(1, self.cwnd/2)
                self.timeout_lock = R.time + self.rtt_ms

            self.retransmit_q.appendleft(packet)
            self._send_loop()


    # Destination
    def dst_recv(self, packet):
        # TODO Insta-ack
        self.src_recv(packet)

'''
    def pop_lump(self, n=1):
        #assert self.tag != "xpand", self
        assert self.remaining_packets >= n, \
                "Flow %d does not have %d packets to send" % (self.id, n)

        self.remaining_packets -= n
        self.bits_left -= n*self.bits_per_packet
        self.n_sent += n

        return (self.id, self.dst, n)

    def pop(self, n = 1):
        assert self.remaining_packets >= n, \
                "Flow %d does not have %d packets to send" % (self.id, n)

        p_size = self.bits_per_packet/8
        if self.tag == "xpand" and self.remaining_packets == 1:
            p_size = self.bits_left/8

        p = Packet(self.src, self.dst, self.n_sent,
                self.tag, self.id, self.remaining_packets == 1,
                size = p_size)

        self.remaining_packets -= 1
        assert self.bits_left > 0
        self.bits_left -= p_size*8
        self.n_sent += 1

        return p

    def rx(self, n=1, t = None):
        self.n_recv += n
        assert self.n_recv <= self.n_sent, "%s recv/sent/size %d/%d/%d" % (self, self.n_recv, self.n_sent, self.size_packets)
        assert self.n_recv <= self.size_packets

        if self.n_recv == self.size_packets:
            if t is None:
                self.end = R.time
            else:
                self.end = t

            LOG.log_flow_done(self)

            global ML_JOBS, ML_QUEUE
            if self.ml_id is not None:
                size, pairs, n = ML_JOBS[self.ml_id]
                ML_JOBS[self.ml_id] = (size, pairs, n-1)
                if n == 1:
                    ML_QUEUE.append(self.ml_id)

            global FLOWS, N_DONE
            N_DONE[0] += 1
            del FLOWS[self.id]

            if len(FLOWS) == 0:
                R.stop()

    def send(self, n_packets):
        n_packets = min(n_packets, self.remaining_packets)
    '''
