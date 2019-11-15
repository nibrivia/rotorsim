class Log:
    # TODO, use an actual logger class, this is just to avoid
    # many open/closes that can significantly degrade performance
    def __init__(self, fn = "out.csv"):
        self.fn = fn
        self.file = open(fn, "w")
        self.cache = [] # Use array to avoid n^2 string append

        # Initialize the .csv
        print("time, src, dst, flow, packet, rotor_id",
                file = self.file)

    def add_timer(self, timer):
        self.timer = timer

    def log(self, src, dst, flow, packets, rotor_id):
        for p in packets:
            msg = ("%.3f, %s, %s, %s, %d, %d\n" %
                    (self.timer.time, src, dst, flow, p, rotor_id))
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

