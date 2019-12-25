import csv
import sys

def stream(input_file, output_file):
    #try:

        flow_data_fields = ["flow", "n_packets", "size", "start", "stop"]
        flow_data    = dict()
        flow_packets = dict()

        print("reading...")
        csv_reader = csv.reader(input_file)
        next(csv_reader)
        for line in csv_reader:
            if csv_reader.line_num % 1e6 == 0:
                print("%3dM" % (csv_reader.line_num/1e6))
            time   = int(line[0])
            flow   = int(line[3])
            packet = int(line[5])

            if flow not in flow_data:
                flow_data[flow] = [flow, 0, 0, time, time]

            flow_data[flow][1] += 1
            flow_data[flow][4] = time

            seen_packets = flow_packets.get(flow, set())
            if packet not in seen_packets:
                flow_data[flow][2] += 1
                seen_packets.add(packet)
                flow_packets[flow] = seen_packets

        print("writing...")
        csv_writer = csv.writer(output_file)
        csv_writer.writerow(flow_data_fields)
        csv_writer.writerows(flow_data.values())



if __name__ == "__main__":
    in_fn  = sys.argv[1]
    out_fn = in_fn.split(".")[0] + "-flows.csv"

    with open(in_fn) as input_file:
        stream(input_file, open(out_fn, "w"))
