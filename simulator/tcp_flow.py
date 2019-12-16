
import csv
import math

from event import R
from packet import Packet

BYTES_PER_PACKET = 1500
HIGH_THPUT_BOUNDARY = 1 # MB

class TCPFlow:
	def __init__(
		self,
		arrival,
		flow_id,
		size,
		src,
		dst,
	):
		self.arrival = arrival
		self.flow_id = flow_id
		self.size    = size # MB
		self.src     = src
		self.dst     = dst

		self.cwnd = 1 * BYTES_PER_PACKET # bytes
		self.sent = []
		self.acked = []
		self.outstanding = 0

		self.completed = float('inf')

		# out_buffer object to send packets through
		self.out_buffer = None

	@property
	def size_in_pkts(self):
		# B = MB * 10^6
		size_in_bytes = self.size * 10**6 
		
		# packet = byte * (packet / byte)
		size_in_packet = size_in_bytes * (1 / BYTES_PER_PACKET)
		return math.ceil(size_in_packet)
	
	@property
	def traffic_left(self):
		return self.size_in_pkts - len(self.sent)

	@property
	def high_thput(self):
		"""
		`self` is a high-throughput flow if its size exceeds
		the high throughput boundary
		"""
		return self.size > HIGH_THPUT_BOUNDARY

	@classmethod
	def make_from_csv(cls, csv_file='flows.csv'):
		flows = []
		with open(csv_file, 'r') as flows_csv:
			reader = csv.reader(flows_csv)
			for i, row in enumerate(reader):
				if i != 0:
					flow_arrival = int(row[0])
					flow_id      = int(row[1])
					flow_size    = int(row[2])
					flow_src     = int(row[3])
					flow_dst     = int(row[4])
					flow = cls(flow_arrival, flow_id, flow_size, flow_src,flow_dst)
					flows.append(flow)
		return flows

	def assign_buffer(self, out_buffer):
		self.out_buffer = out_buffer

	@property
	def id(self):
		return self.flow_id

	def send(self):
		if self.completed != float('inf'):
			return

		# determine number of packets to send depending on cwnd
		num_pkts_to_send = int(self.cwnd // BYTES_PER_PACKET)
		
		# make packets to hand to the sending buffer
		while self.outstanding < num_pkts_to_send:
			for i in range(num_pkts_to_send):

				# check can send 
				if len(self.sent) >= self.size_in_pkts:
					if len(self.sent) - len(self.acked) == 0:
						self.completed = min(self.completed, int(R.time // self.slot_duration))
					return

				packet = Packet(src = self.src,
								dst = self.dst,
								seq_num = len(self.sent) + i,
								flow = self,
								high_thput = self.high_thput)
				# deliver the packets via the out buffer
				self.out_buffer.recv([packet])
				self.sent.append(packet)
				self.outstanding += 1

		return num_pkts_to_send

	def recv(self, packets):
		# record the acked packets
		for acked_packet in packets:			
			self.acked.append(acked_packet)

		# update cwnd and outstanding based on packets acked now
		acked_now = len(packets)
		self.cwnd += (acked_now * BYTES_PER_PACKET / self.cwnd)
		self.outstanding -= acked_now

		# now do send again
		self.send()

	def dump_status(self):
		out = []

		# flow id
		out.append('Flow {}'.format(self.id))

		# flow src, dst
		out.append('{}{} ~> {}'.format(' '*4, self.src, self.dst))

		# flow sending statistics sending
		out.append('{}Size            {} MB'.format(' '*4, self.size))
		out.append('{}Size            {} packets'.format(' '*4, self.size_in_pkts))
		out.append('{}Sent            {} packets'.format(' '*4, len(self.acked)))
		out.append('{}Inflight        {} packets'.format(' '*4, len(self.sent) - len(self.acked)))
		out.append('{}Unsent          {} packets'.format(' '*4, self.traffic_left))
		out.append('{}Arrive          {} slots'.format(' '*4, self.arrival))
		out.append('{}Complete        {} slots'.format(' '*4, self.completed))
		out.append('{}Completion Time {} slots'.format(' '*4, self.completed - self.arrival))

		# output is newline-separated
		print('\n'.join(out))

	