
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

		self.cwnd = 1 # packets
		self.n_sent = 0
		self.acked = []
		self.outstanding = 0
		self.name = "Flow " + str(self.flow_id)

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

	@property
	def id(self):
		return self.flow_id

	def send(self):
		if self.completed != float('inf'):
			return

		# determine number of packets to send depending on cwnd
		
		# make packets to hand to the sending buffer
		while self.outstanding < self.cwnd and self.n_sent < self.size:
			packet = Packet(src = self.src,
							dst = self.dst,
							seq_num = self.n_sent,
							flow = self,
							high_thput = self.high_thput)
			# deliver the packets via the out buffer
			self.src.add_demand_to(self.dst, [packet])
			self.n_sent      += 1
			self.outstanding += 1

	def recv(self, packet):
		# record the acked packets
		self.acked.append(packet)

		# update cwnd and outstanding based on packets acked now
		self.cwnd += (1 / self.cwnd)
		self.outstanding -= 1

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
		out.append('{}Inflight        {} packets'.format(' '*4, self.nsent - len(self.acked)))
		out.append('{}Unsent          {} packets'.format(' '*4, self.traffic_left))
		out.append('{}Arrive          {} slots'.format(' '*4, self.arrival))
		out.append('{}Complete        {} slots'.format(' '*4, self.completed))
		out.append('{}Completion Time {} slots'.format(' '*4, self.completed - self.arrival))

		# output is newline-separated
		print('\n'.join(out))

	def __str__(self):
		return self.name
