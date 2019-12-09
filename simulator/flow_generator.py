
import random
import csv
from itertools import product
import numpy as np

from workloads.websearch import websearch_workload_distribution as websearch


# HELPERS ========================================================

def get_workload_size_func(workload):
	lower = workload.lower()
	if lower == 'websearch':
		return websearch
	else:
		raise Exception('Unrecognized workload {}'.format(workload))


def print_generated_flow(arrival, flow_id, size, src, dst):
	print('Flow {} arriving at slot {} with {} packets to send from ToR {} to ToR {}'.format(\
				flow_id, arrival, size, src, dst))


# MAIN ===========================================================

def generate_flows(
	max_slots,
	num_flows,
	num_tors,
	scale=1000,
	workload='websearch',
	results_file='flows.csv',
):
	# csv header
	fields = [
		'FLOW_ARRIVAL_IN_SLOTS',
		'FLOW_ID',
		'FLOW_SIZE_IN_PKTS',
		'FLOW_SRC',
		'FLOW_DST',
	]

	# get workload generator
	generate_workload = get_workload_size_func(workload)

	# construct tor pairs
	tors = set(range(num_tors))
	tor_pairs = list(set(product(tors, tors)) - { (i, i) for i in tors })

	# model num_flows flow arrivals with a poisson process
	flow_arrivals_per_slot = num_flows / max_slots
	arrivals = list(np.random.poisson(flow_arrivals_per_slot, max_slots))
	while sum(arrivals) != num_flows:
		arrivals = list(np.random.poisson(flow_arrivals_per_slot, max_slots))

	# create flows 
	flows = []
	for arrival_slot, flows_arriving in enumerate(arrivals):
		# take note of flows arriving in this slot
		for _ in range(flows_arriving):
			# get flow size from specified workload
			size = generate_workload(scale)
			# assign the current slot to this flow's arrival
			arrival = arrival_slot
			# src --> dst chosen randomly over tor pairs
			src, dst = random.choice(tor_pairs)
			# assign unique flow id to this flow
			flow_id = len(flows)

			flows.append((arrival, flow_id, size, src, dst))
			# print_generated_flow(arrival, flow_id, size, src, dst)

	# write flows out to csv in increasing arrival order
	with open(results_file, 'w') as csv_file:
		# write csv header
		csv_writer = csv.writer(csv_file) 
		csv_writer.writerow(fields)
		# write flows 
		csv_writer.writerows(flows)
