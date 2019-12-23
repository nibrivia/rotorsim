class Log:
    def __init__(self, fn = "out.csv"):
        self.fn = fn
        self.file = open(fn, "w")
        self.cache = [] # Use array to avoid n^2 string append

        # Initialize the .csv
        print("time, src, dst, flow, flow_src, flow_dst, packet",
                file = self.file)

    def add_timer(self, timer):
        self.timer = timer

    def log(self, src, dst, packet):
        msg = "%.3f, %s, %s, %s, %s, %s, %s\n" % \
                        (self.timer.time,
                            src, dst,
                            packet.flow, packet.src, packet.dst, packet.seq_num)
        self.cache.append(msg)
        if len(self.cache) > 100:
            self._flush()

    def _flush(self):
        self.file.writelines(self.cache)
        self.cache = []

    def close(self):
        self._flush()
        self.file.close()

