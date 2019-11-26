RoToRnet simulator
==================

For MIT's 6.829 final project.

This currently does a packet-level simulation of RotorNet.

# Run


You can run the program with the following command, it will terminate once
demand is exhausted or the specified number of cycles, whichever is earliest.


```
python3 simulator.py --n_tor 20 --n_rotor 3 --n_cycles 10 --packets_per_slot 1
```

The `--verbose` flag will print our (colorful) descriptions of what's going on.

# Developing

The `master` branch should now be considered stable, create your own branch if
you want to work on this.
