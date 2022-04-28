from pyproject.array import connect, ndarray
import pyproject.linalg as lg
import numpy as np

def f():
    x = ndarray(1, 10, np.float64, init_value=1.)
    y = ndarray(1, 10, np.float64, init_value=1.)

    a = ndarray(1, 10, np.float64, init_value=1.)
    b = ndarray(1, 10, np.float64, init_value=1.)

    a = x @ y
    r = lg.axpy(a, x, y, multiplier=-1.)


if __name__ == '__main__':
    connect("172.17.0.1", 10000)
    s = f()


