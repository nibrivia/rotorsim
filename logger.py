class Log:
    # TODO, use an actual logger class, this is just to avoid
    # many open/closes that can significantly degrade performance
    def __init__(self, fn = "out.csv"):
        self.fn = fn
        self.file = open(fn, "w")
        self.cache = [] # Use array to avoid n^2 string append

        # Initialize the .csv
        print("time, src, dst, flow, packet",
                file = self.file)

    def log(self, t, src, dst, flow, packets):
        for p in packets:
            msg = ("%.3f, %s, %s, %s, %d\n" %
                    (t, src, dst, flow, p))
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

