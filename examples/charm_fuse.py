from charmnumeric.array import connect, ndarray
from charmnumeric.ast import set_max_depth, charm_fuse
from charmnumeric.ccs import enable_debug
import charmnumeric.linalg as lg
import numpy as np
import time
# enable_debug()
set_max_depth(10)

@charm_fuse
def f():
    v = ndarray(1, 50, np.float64, init_value=-20)
    b = ndarray(1, 50, np.float64, init_value=10)
    # c = ndarray(1, 50, np.float64, init_value=30)

    g1 =  v.abs().scale(2).scale(2).add_constant(29) + b + 32
    # g1 = 151
    g2 = b.log(2).exp()
    # g2 = 27.(..)
    d = g1 + g2
    return d.get()

# @charm_fuse
def g():
    v = ndarray(1, 50, np.float64, init_value=-20)
    b = ndarray(1, 50, np.float64, init_value=10)
    # c = ndarray(1, 50, np.float64, init_value=30)

    g1 =  v.abs().scale(2).scale(2).add_constant(29) + b + 32
    # g1 = 151
    g2 = b.log(2).exp()
    # g2 = 27.(..)
    d = g1 + g2
    return d.get()

if __name__ == '__main__':
    connect("127.0.0.1", 10000)
    s = f()
    start = time.time()
    for(i) in range(100):
        s = f()
    end = time.time()
    print(s)
    print("Time taken(multi line fused): ", end - start)

    s = g()
    start = time.time()
    for(i) in range(100):
        s = g()
    end = time.time()
    print(s)
    print("Time taken(multi line unfused): ", end - start)
