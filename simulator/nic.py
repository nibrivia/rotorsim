from collections import deque
from helpers import vprint, color_str_
from event import R
from params import PARAMS

class NIC:
    def __init__(self,
            dst_recv,
            name  = "",
            delay_ns = 0,
            bandwidth_Bms = None,
            max_size_bytes = None,
            ):
        # ID
        self.name = name
        if name is None or name == "":
            self.name = "{}"

        # Internal state
        self._queue   = deque()
        self._enabled = True
        self._paused  = False

        self.q_size_B = 0
        self.queue_size_max = max_size_bytes

        # Link params
        self.prop_delay = delay_ns / 1e6
        if bandwidth_Bms is None:
            self.ms_per_byte = 0
        else:
            print(bandwidth_Bms)
            self.ms_per_byte = 1/bandwidth_Bms

        # Destination values
        self.dst_recv = dst_recv

        # Pull hook
        self.empty_callback = None


    def enq(self, packet):
        if packet.flow_id == PARAMS.flow_print:
            vprint("nic  : %s enq  %s" % (packet, self))
        if self.queue_size_max is not None and \
                self.q_size_B + packet.size_B > self.queue_size_max:
            if packet.flow_id == 0:
                vprint("%s dropped, full queue %s" % (packet, self))
            return
        self._queue.appendleft(packet)
        self.q_size_B += packet.size_B
        # Having a delay of 0 makes it so that even if we can send immediately,
        # it waits until the caller is done, making the behavior of enq
        # consistent regardless of current queue size
        R.call_in(0, self._send)

    # FIXME There's a bug here where pause and resume will make this send
    # FIXME faster than it should...
    def pause(self):
        self._paused = True

    def resume(self):
        self._paused = False
        self._enabled = True 
        self._send()

    def _enable(self):
        self._enabled = True
        self._send()

    def _send(self):
        # Currently sending something, or paused, or no packets to send
        if not self._enabled or self._paused:
            return
        if len(self._queue) == 0:
            if self.empty_callback is not None:
                self.empty_callback()
            return

        # Disable
        self._enabled = False

        # Get packet and compute tx time
        pkt = self._queue.pop()
        self.q_size_B -= pkt.size_B
        tx_delay = pkt.size_B * self.ms_per_byte


        if pkt.flow_id == PARAMS.flow_print:
            vprint("queue: %s sent %s tx %.6f lat %.6f" % (pkt, self, tx_delay, self.prop_delay))
        R.call_in(tx_delay, self._enable)
        R.call_in(self.prop_delay + tx_delay, self.dst_recv, pkt)

    @color_str_
    def __str__(self):
        if self.queue_size_max is None:
            frac_full = self.q_size_B
        else:
            frac_full = self.q_size_B / self.queue_size_max
        return "%s [%2d%% / %d pkt]" % (self.name, frac_full*100, len(self._queue))
