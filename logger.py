class Log:
    # TODO, use an actual logger class, this is just to avoid
    # many open/closes that can significantly degrade performance
    def __init__(self, fn = "out.csv"):
        self.fn = fn
        self.file = open(fn, "w")
        self.cache = [] # Use array to avoid n^2 string append

        # Initialize the .csv
        print("time, src, src_queue, dst, dst_queue, packet",
                file = self.file)

    def log(self, msg):
        self.cache.append(msg)
        if len(self.cache) > 100000:
            self.flush()

    def flush(self):
        #print("FLUSH")
        self.file.writelines(self.cache)
        self.cache = []


    def close_log(self):
        self.flush()
        self.file.close()


