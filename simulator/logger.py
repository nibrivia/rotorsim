class Log:
    # TODO, use an actual logger class, this is just to avoid
    # many open/closes that can significantly degrade performance
    def __init__(self, fn = "out.csv"):
        self.fn = fn
        self.file = open(fn, "w")
        self.cache = [] # Use array to avoid n^2 string append

        # Initialize the .csv
        print("time, src, dst, flow_src, flow_dst, packet",
                file = self.file)

    def add_timer(self, timer):
        self.timer = timer

    def log(self, src, dst, packets):
        msgs = ["%.3f, %s, %s, %s, %s, %s\n" %
                        (self.timer.time,
                            src, dst,
                            p.src, p.dst, p.seq_num)
                    for p in packets]
        self.cache.extend(msgs)
        if len(self.cache) > 100:
            self._flush()

    def _flush(self):
        #print("FLUSH")
        self.file.writelines(self.cache)
        self.cache = []

    def close(self):
        self._flush()
        self.file.close()

