import time, socket, git
import uuid as _uuid
from event import R
import os

class Log:
    def __init__(self, fn = "out.csv"):
        self.cache = [] # Use array to avoid n^2 string append

    def set_fn(self, fn = "out", uuid = None, **params):
        self.sim_id = uuid
        if uuid is None:
            self.sim_id = _uuid.uuid4()

        self.fn = fn
        if fn is None:
            self.fn = str(self.sim_id) + ".csv"

        # Initialize the .csv
        self.file = open(self.fn, "w")
        print(self.fn)

        param_names = [ "uuid",
                "n_tor", "n_switches", "time_limit",
                "n_cache", "n_rotor", "n_xpand",
                "workload", "arrive_at_start", "skewed", "load", "is_ml", "cache_policy",
                "commit", "host", "timestamp"]
        params["commit"]    = git.Repo(search_parent_directories=True).head.object.hexsha
        params["host"]      = socket.gethostname()
        params["timestamp"] = time.time()
        params["uuid"]      = self.sim_id

        values = [str(params[name]) for name in param_names]

        print(",".join(param_names), file = self.file)
        print(",".join(values),      file = self.file)



        print("flow_id,tag,src,dst,start,end,size,sent,fct", file = self.file)

    def log_flow_done(self, flow):
        msg = "%d,%s,%d,%d,%.3f,%.3f,%d,%d,%.3f\n" % (
                flow.id, flow.tag, flow.src, flow.dst, flow.arrival, flow.end, flow.size, flow.size-flow.bits_left, flow.end - flow.arrival)
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
        # rename before close?
        os.rename(self.fn, "done-" + self.fn)
        self.file.close()

LOG = Log()

def init_log(fn, **params):
    global LOG
    LOG.set_fn(fn, **params)
