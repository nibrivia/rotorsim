
import click
import random
import csv
from itertools import product

from workloads.websearch import websearch_workload_distribution as websearch


# HELPERS ========================================================

def get_workload_size_func(workload):
	lower = workload.lower()
	if lower == 'websearch':
		return websearch
	else:
		raise Exception('Unrecognized workload {}'.format(workload))


# MAIN ===========================================================


@click.command()
@click.option(
	'--max_slots',
	type=int,
)
@click.option(
	'--num_flows',
	type=int,
)
@click.option(
	'--num_tors',
	type=int,
)
@click.option(
	'--scale',
	type=int,
	default='1000'
)
@click.option(
	'--workload',
	type=str,
	default='websearch'
)
@click.option(
	'--results_file',
	type=str,
	default='flows.csv',
)
def main(
	max_slots,
	num_flows,
	num_tors,
	scale,
	workload,
	results_file,
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

	# create flows 
	flows = []
	for flow_id in range(num_flows):
		# get flow size from specified workload
		size = generate_workload(scale)
		# increasing flow arrival time
		arrival = flow_id % max_slots
		# src --> dst chosen randomly over tor pairs
		src, dst = random.choice(tor_pairs)

		flows.append((arrival, flow_id, size, src, dst))

	# sort flows in increasing arrival order
	flows.sort(key = lambda f: f[0])

	# write flows out to csv in increasing arrival order
	with open(results_file, 'w') as csv_file:
		# write csv header
		csv_writer = csv.writer(csv_file) 
		csv_writer.writerow(fields)
		# write flows 
		csv_writer.writerows(flows)


if __name__ == '__main__':
	main()