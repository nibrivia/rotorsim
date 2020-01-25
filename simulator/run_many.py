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
def run_experiments(p_space):
    param_space = [(key, value) for key, value in p_space.items()]

    n_experiments = len_param_space(param_space)
    print(n_experiments, "experiments to run")

    with concurrent.futures.ProcessPoolExecutor() as executor:
        for params in gen_params(param_space):
            executor.submit(run_experiment, **params)

params = dict(
        time_limit = [2000],
        n_switches = [33],
        n_tor      = [257],
        workload   = ["chen"],
        n_cache    = [0, 16],
        load       = [.1, .4, .5, .6, .7, .8],
        )

run_experiments(params)


print("done")

