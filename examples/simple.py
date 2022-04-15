from pyproject.array import connect, ndarray
import numpy as np

def f():
    x = ndarray(1, 1000, np.float64)
    y = ndarray(1, 1000, np.float64)

    return x @ y


connect("172.17.0.1", 10000)
s = f()
print(s.get())


