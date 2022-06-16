from charmtiles.array import connect, ndarray
import charmtiles.linalg as lg
from charmtiles.ccs import enable_debug
import numpy as np

import time

enable_debug()

def solve(A, b):
    x = ndarray(1, 1000, np.float64)
    r = b - A @ x
    p = r.copy()
    rsold = r @ r

    for i in range(1000):
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

    A = ndarray(2, (1000, 1000), np.float64, init_value=1.)
    b = ndarray(1, 1000, np.float64, init_value=1.)

    start = time.time()
    x = solve(A, b)
    print("Execution time = %.6f" % (time.time() - start))


