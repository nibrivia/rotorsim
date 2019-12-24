class Log:
    def __init__(self, fn = "out.csv"):
        self.fn = fn
        self.file = open(fn, "w")
        self.cache = [] # Use array to avoid n^2 string append

        # Initialize the .csv
        print("time, src, dst, flow, rotor, packet",
                file = self.file)

    def add_timer(self, timer):
        self.timer = timer

    def log(self, src, dst, rotor, packet):
        msg = "%d,%d,%d,%d,%d,%d\n" % \
                        (self.timer.time*1000,
                            src.id, dst.id,
                            packet.flow.flow_id,
                            rotor.id,
                            packet.seq_num)
        self.cache.append(msg)
        if len(self.cache) > 100:
            self._flush()

    def _flush(self):
        self.file.writelines(self.cache)
        self.cache = []

    def close(self):
        self._flush()
        self.file.close()

