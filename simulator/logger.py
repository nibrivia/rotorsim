from event import R

class Log:
    def __init__(self, fn = "out.csv"):
        self.cache = [] # Use array to avoid n^2 string append

    def set_fn(self, fn = "out.csv"):
        self.fn = fn

        # Initialize the .csv
        self.file = open(fn, "w")
        print("flow_id,tag,src,dst,start,end,size,fct", file = self.file)

    def log_flow_done(self, flow):
        msg = "%d,%s,%d,%d,%.3f,%.3f,%d,%.3f\n" % (flow.id, flow.tag, flow.src, flow.dst, flow.arrival, flow.end, flow.size, flow.end - flow.arrival)
        self.cache.append(msg)
        if len(self.cache) > 100:
            self._flush()

    def log(self, src, dst, rotor, packet):
        return
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

LOG = Log()

def init_log(fn):
    global LOG
    LOG.set_fn(fn)
