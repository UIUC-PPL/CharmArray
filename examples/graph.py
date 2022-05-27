from pyproject.array import connect, ndarray
import pyproject.linalg as lg
import numpy as np

def f():
    v = ndarray(1, 10, np.float64)
    b = ndarray(1, 10, np.float64)
    c = ndarray(1, 10, np.float64)
    y = v + b + c
    z = v - y
    z.evaluate()
    print(z.shape)


if __name__ == '__main__':
    connect("172.17.0.1", 10000)
    s = f()


