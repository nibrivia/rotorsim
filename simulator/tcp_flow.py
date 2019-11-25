
import csv

BYTES_PER_PACKET = 1500

class TCPFlow:
	def __init__(
		self,
		arrival,
		flow_id,
		size,
		src,
		dst
	):
		self.arrival = arrival
		self.flow_id = flow_id
		self.size    = size # packets
		self.src     = src
		self.dst     = dst

		self.cwnd = 1 * BYTES_PER_PACKET # bytes
		self.sent = 0


	@property
	def traffic_left(self):
		return self.size - self.sent


	@classmethod
	def from_csv(cls, csv_file='simulator/flows.csv'):
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


	def send_cwnd(self):
		pkts_to_send = self.cwnd // BYTES_PER_PACKET
		self.sent += pkts_to_send
		print('Flow {} sent {} pkts. Have {} left'.format(self.flow_id, self.sent, self.traffic_left))
		return pkts_to_send


	def receive_ack(self):
		self.cwnd += (1 / self.cwnd)

	