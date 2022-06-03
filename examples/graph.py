from pyproject.array import connect, ndarray
import pyproject.linalg as lg
import numpy as np

def f():
    v = ndarray(1, 10, np.float64)
    b = ndarray(1, 10, np.float64, init_value=10)
    c = ndarray(1, 10, np.float64)
    w = c
    for i in range(5):
        y = v + b + w
        z = v - y
        w = 2 * (c - z) + b
    w.command_buffer.plot_graph()
    #print(w.shape)
    #print(z.shape)
    #print(y.shape)


if __name__ == '__main__':
    #connect("172.17.0.1", 10000)
    s = f()

