class Log:
    # TODO, use an actual logger class, this is just to avoid
    # many open/closes that can significantly degrade performance
    def __init__(self, fn = "out.csv"):
        self.fn = fn
        self.file = open(fn, "w")
        self.cache = [] # Use array to avoid n^2 string append

        # Initialize the .csv
        print("time, packet_num, src, src_queue, dst, dst_queue, packet",
                file = self.file)

    def log(self, t, src, dst, packets):
        for p in packets:
            msg = ("%.3f, %s, %s, %s, %s, %d\n" %
                    (t,
                        src.owner, src.q_name,
                        dst.owner, dst.q_name,
                        p))
            self.cache.append(msg)
        if len(self.cache) > 10:
            self._flush()

    def _flush(self):
        #print("FLUSH")
        self.file.writelines(self.cache)
        self.cache = []

    def close(self):
        self._flush()
        self.file.close()

