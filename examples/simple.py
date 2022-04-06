from pyproject.array import connect, ndarray
import numpy as np

def f():
    x = ndarray(2, [1000, 1000], np.float64)
    y = ndarray(2, [1000, 1000], np.float64)

    z = x + y
    z -= x

connect("172.17.0.1", 10000)
z = f()
m = ndarray(2, [10, 10], np.float64)

