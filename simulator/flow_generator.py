
import random
import csv
from itertools import product
import numpy as np

from workloads.websearch import websearch_workload_distribution as websearch
from workloads.chen import chen_distribution as chen

from collections import defaultdict


# HELPERS ========================================================

WORKLOAD_FNS = defaultdict(
        websearch = websearch,
        chen      = chen,
        default   = lambda _: Exception('Unrecognized workload {}'.format(workload)))

# MAIN ===========================================================

def generate_flows(
        interflow_arrival_slots,
	num_tors,
        max_slots,
	workload,
	results_file='flows.csv',
):
	# csv header
	fields = [
		'FLOW_ARRIVAL_IN_SLOTS',
		'FLOW_ID',
		'FLOW_SIZE_IN_BYTES',
		'FLOW_SRC',
		'FLOW_DST',
	]

	# get workload generator
	generate_workload = WORKLOAD_FNS[workload]

	# construct tor pairs
	tors = set(range(num_tors))
	tor_pairs = list(set(product(tors, tors)) - { (i, i) for i in tors })

	# model num_flows flow arrivals with a poisson process
	print(interflow_arrival_slots)
	flow_arrivals_per_slot = interflow_arrival_slots*num_tors
	print(flow_arrivals_per_slot)
	arrivals = list(np.random.poisson(flow_arrivals_per_slot, max_slots))
	print(sum(arrivals))

	# create flows 
	flows = []
	for arrival_slot, flows_arriving in enumerate(arrivals):
		# take note of flows arriving in this slot
		for _ in range(flows_arriving):
			# get flow size from specified workload
			size = generate_workload()
			# assign the current slot to this flow's arrival
			arrival = arrival_slot
			# src --> dst chosen randomly over tor pairs
			src, dst = random.choice(tor_pairs)
			# assign unique flow id to this flow
			flow_id = len(flows)

			flows.append((arrival, flow_id, size, src, dst))

	# write flows out to csv in increasing arrival order
	with open(results_file, 'w') as csv_file:
		# write csv header
		csv_writer = csv.writer(csv_file) 
		csv_writer.writerow(fields)
		# write flows 
		csv_writer.writerows(flows)
