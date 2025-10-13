from charmnumeric.array import connect, ndarray
from charmnumeric.ast import set_max_depth
from charmnumeric.ccs import enable_debug
import charmnumeric.linalg as lg
import numpy as np

#enable_debug()
set_max_depth(10)

def f():
    v = ndarray(2, [10, 10], np.float64, init_value=20)
    b = ndarray(2, [10, 10], np.float64, init_value=1)
    c = ndarray(2, [10, 10], np.float64, init_value=30)
    # k = v * 2 + b + 3 + c - 32
    # l = k >= 42
    r = b.where(42, 69)
    # g = b.where(v, c)
    # z = ~r
    # g = v.abs() + b + 32
    # k = g + c * 8
    # k = g + 1
    # k = g + 2 * c - 3 * v
    q = r.get()
    print(q)
    # w = c
    # for i in range(5):
    #     y = v + b + w
    #     z = v - y
    #     w = 2 * (c - z) + b
    # w.evaluate()


if __name__ == '__main__':
    connect("172.17.0.1", 10000)
    s = f()

