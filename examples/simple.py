from charmtiles.array import connect, ndarray
import charmtiles.linalg as lg
from charmtiles.ccs import sync
from charmtiles.ast import set_max_depth
import numpy as np
import gc
#from ctypes import c_long
import sys

set_max_depth(1)

class A(object):
    def __init__(self):
        pass

def f():
    #print(gc.set_threshold(300, 10, 10))

    b = ndarray(1, 100, np.float64)
    #print(c_long.from_address(id(b)).value)
    #print(gc.get_referrers(b))

    #a = ndarray(1, 100, np.float64)
    #for i in range(10):
    for i in range(100):
        z = b + b
        gc.collect()
    #print(z)
    #print(sys.getrefcount(z))
    #print("Z", gc.get_referrers(z), b, z)
    #print("b", gc.get_referrers(b))
    #print(gc.is_tracked(z))
    #print(gc.get_stats())

    #print(z.get())
    #z = a + b
    #z.evaluate()
    #print("Actual =", xnp @ ynp)


if __name__ == '__main__':
    connect("172.17.0.1", 10000)
    s = f()
    sync()

