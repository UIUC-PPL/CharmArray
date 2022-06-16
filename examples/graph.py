from charmtiles.array import connect, ndarray
from charmtiles.ast import set_max_depth
from charmtiles.ccs import enable_debug
import charmtiles.linalg as lg
import numpy as np

#enable_debug()
set_max_depth(100)

def f():
    v = ndarray(1, 10, np.float64)
    b = ndarray(1, 10, np.float64, init_value=10)
    c = ndarray(1, 10, np.float64)
    w = c
    for i in range(5):
        y = v + b + w
        z = v - y
        w = 2 * (c - z) + b
    w.evaluate()


if __name__ == '__main__':
    connect("172.17.0.1", 10000)
    s = f()

