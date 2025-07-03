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

    b = ndarray(2, (100,100), np.float64, init_value=4.0)
    #print(c_long.from_address(id(b)).value)
    #print(gc.get_referrers(b))

    #a = ndarray(1, 100, np.float64)
    #for i in range(10):
    c = ndarray.where(b, b+2, b < -1)
    # b.evaluate()
    # for i in range(100):
    #     z = b + b
    #     gc.collect()
    print(c.get())
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
    connect("192.168.0.250", 10000)
    s = f()
    sync()

