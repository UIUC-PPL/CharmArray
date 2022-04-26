from pyproject.array import connect, ndarray
import numpy as np

def f():
    x = ndarray(1, 1000, np.float64, init_value=1.)
    y = ndarray(1, 1000, np.float64, init_value=1.)

    a = ndarray(1, 100, np.float64, init_value=1.)
    b = ndarray(1, 100, np.float64, init_value=1.)

    return (x @ y) / (a @ b)


if __name__ == '__main__':
    connect("172.17.0.1", 10000)
    s = f()
    print(s.get())


