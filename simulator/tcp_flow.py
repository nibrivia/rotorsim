
from helpers import print_packet
from event import R

from packet import Packet
import csv

BYTES_PER_PACKET = 1500

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
		self.size    = size # packets
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
	def traffic_left(self):
		return self.size - len(self.sent)

	@classmethod
	def make_from_csv(cls, csv_file='simulator/flows.csv'):
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
		if len(self.sent) >= self.size:
			self.completed = min(self.completed, int(R.time // self.slot_duration))
			return

		# determine number of packets to send depending on cwnd
		num_pkts_to_send = int(self.cwnd // BYTES_PER_PACKET)
		
		# make packets to hand to the sending buffer
		while self.outstanding < num_pkts_to_send:
			for i in range(num_pkts_to_send):
				packet = Packet(src = self.src,
								dst = self.dst,
								seq_num = len(self.sent) + i,
								flow = self)
				# deliver the packets via the out buffer
				# print_packet(packet, ack=False)
				self.out_buffer.recv([packet])
				self.sent.append(packet)
				self.outstanding += 1

		return num_pkts_to_send

	def recv(self, packets):
		# record the acked packets
		for acked_packet in packets:			
			self.acked.append(acked_packet)
			# print_packet(acked_packet, ack=True)

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
		out.append('{}Size       {}'.format(' '*4, self.size))
		out.append('{}Sent       {}'.format(' '*4, len(self.acked)))
		out.append('{}Inflight   {}'.format(' '*4, len(self.sent) - len(self.acked)))
		out.append('{}Unsent     {}'.format(' '*4, self.traffic_left))
		out.append('{}Arrive     {}'.format(' '*4, self.arrival))
		out.append('{}Complete   {}'.format(' '*4, self.completed))
		out.append('{}FCT (slot) {}'.format(' '*4, self.completed - self.arrival))

		# output is newline-separated
		print('\n'.join(out))

	