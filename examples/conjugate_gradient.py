from pyproject.array import connect, ndarray
import pyproject.linalg as lg
import numpy as np

def solve(A, b):
    x = ndarray(1, 100, np.float64)
    r = b - A @ x
    p = r.copy()
    rsold = r @ r

    for i in range(100):
        Ap = A @ p
        alpha = rsold / (p @ Ap)

        x = lg.axpy(alpha, p, x)
        r = lg.axpy(alpha, Ap, r, multiplier=-1.)

        rsnew = r @ r

        if np.sqrt(rsnew.get()) < 1e-8:
            print("Converged in %i iterations" % (i + 1))
            break

        p = lg.axpy(rsnew / rsold, p, r)
        rsold = rsnew

    return x

if __name__ == '__main__':
    connect("172.17.0.1", 10000)

    A = ndarray(2, (100, 100), np.float64)
    b = ndarray(1, 100, np.float64)
    x = solve(A, b)


