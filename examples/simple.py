from pyproject.array import connect, ndarray
import pyproject.linalg as lg
import numpy as np

def f():
    vnp = np.array([2, 1, 1], dtype=np.float64)
    Anp = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.float64)
    bnp = np.array([1, 0], dtype=np.float64)
    v = ndarray(1, 3, np.float64, nparr=vnp)
    b = ndarray(1, 2, np.float64, nparr=bnp)
    A = ndarray(2, (2, 3), np.float64, nparr=Anp)
    Av = A @ v
    Avnorm = Av @ b
    print(Avnorm.get())
    #print("Actual =", xnp @ ynp)


if __name__ == '__main__':
    connect("172.17.0.1", 10000)
    s = f()


