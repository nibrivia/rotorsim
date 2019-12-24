
import random
from collections import Counter

SMALL_FLOW_PROB  = 0.25
MEDIUM_FLOW_PROB = 0.50
LARGE_FLOW_PROB  = 0.25

assert LARGE_FLOW_PROB + SMALL_FLOW_PROB + MEDIUM_FLOW_PROB == 1

def chen_distribution(
    size_of_small_flows =1,    # MB 
    size_of_medium_flows=10,   # MB 
    size_of_large_flows =1000, # MB
):

    # obtain random number from [0, 1]
    random_float = random.random()

    # case: large flow
    if random_float <= LARGE_FLOW_PROB:
        return size_of_large_flows

    # case: small flow
    elif random_float <= SMALL_FLOW_PROB + LARGE_FLOW_PROB:
        return size_of_small_flows

    # case: medium flow
    else:
        return size_of_medium_flows


if __name__ == '__main__':
    # little script to validate that the distribution of sizes
    # matches the expected probabilities defined above
    total = 10**6
    sizes = [chen_distribution() for _ in range(total)]
    counter = Counter(sizes)
    for size, count in sorted(counter.items()):
        fraction = count / total
        print('{}% of flows have size {}'.format(int(100 * fraction), size))


chen_size = \
        SMALL_FLOW_PROB  * 1 +
        MEDIUM_FLOW_PROB * 10 +
        LARGE_FLOW_PROB  * 1000
