import concurrent.futures
import subprocess

# Generates cartesian product of parameters
def gen_params(param_space, prefix = ""):
    if len(param_space) == 0:
        yield dict()
        return

    key, values = param_space[0]

    # Recurse if need be
    for param in gen_params(param_space[1:], prefix + "  "):
        for v in values:
            d= {key:v, **param}
            yield d

def len_param_space(param_space):
    if len(param_space) == 0:
        return 1

    key, values = param_space[0]
    return len(values)*len_param_space(param_space[1:])


# Runs a single experiment
def run_experiment(**kwargs):
    cmd = "python3 simulator.py " + " ".join("--%s %s" % (k, v) for k, v in kwargs.items())
    print(cmd)
    subprocess.run(cmd.split(), stdout=subprocess.DEVNULL)
    print(cmd, "done")


# Runs all experiments
def run_experiments(executor, p_space):
    param_space = [(key, value) for key, value in p_space.items()]

    n_experiments = len_param_space(param_space)
    print(n_experiments, "experiments to run")

    for params in gen_params(param_space):
        executor.submit(run_experiment, **params)

params_drain = dict(
        time_limit = [1000],
        n_switches = [37],
        n_tor      = [256],
        workload   = ["chen"],
        n_xpand    = [5],
        load       = [i/8 for i in range(1,8)],
        n_cache    = [16, 0],
        )
params_drain_xpand = {**params_drain, 
        'n_xpand' : [37],
        'n_cache' : [0]
        }
params_opera = dict(
        time_limit = [10000],
        n_switches = [13],
        n_tor      = [108],
        workload   = ["datamining"],
        n_xpand    = [7],
        load       = [.01, .1, .25, .3, .4],
        n_cache    = [0],
        )

params_cache = dict(
        time_limit = [1000],
        n_switches = [37],
        n_tor      = [256],
        workload   = ["chen"],
        n_xpand    = [5],
        load       = [i/8 for i in range(1,8)],
        n_cache    = [0, 8],
        )
params_xpand = {**params_cache,
        'n_xpand'    : [37],
        'n_cache'    : [0],
        }

params_ml = dict(
        time_limit = [10000],
        n_switches = [21],
        n_tor      = [128],
        workload   = ["datamining"],
        load       = [.2, .7],
        is_ml      = [""])
params_ml_cache = {**params_ml,
        'n_xpand' : [5],
        'n_cache' : [0, 8]
        }
params_ml_xpand = {**params_ml,
        'n_xpand' : [21],
        'n_cache' : [0]
        }

params_256 = dict(
        n_tor      = [256],
        n_switches = [37],
        n_xpand    = [5],
        n_cache    = [0, 16]
        )
params_128 = dict(
        n_tor      = [128],
        n_switches = [21],
        n_xpand    = [5],
        n_cache    = [0, 8]
        )
params_96 = dict(
        n_tor      = [96],
        n_switches = [17],
        n_xpand    = [5],
        n_cache    = [0, 6]
        )
                
params_datamining = dict(
        time_limit = [10000],
        workload   = ["datamining"],
        load       = [i/10 for i in range(1,10)]
        )


with concurrent.futures.ProcessPoolExecutor(max_workers = 36) as executor:
    run_experiments(executor, {**params_datamining, **params_256})
    run_experiments(executor, {**params_datamining, **params_128})
    run_experiments(executor, {**params_datamining, **params_96 })


print("done")

