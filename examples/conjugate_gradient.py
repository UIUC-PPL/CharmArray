from charmtiles.array import connect, ndarray
import charmtiles.linalg as lg
from charmtiles.ccs import enable_debug, sync
from charmtiles.ast import set_max_depth
import numpy as np

import time

#enable_debug()
set_max_depth(10)

def solve(A, b, x):
    r = b - A @ x
    p = r.copy()
    rsold = r @ r

    for i in range(100):
        Ap = A @ p
        alpha = rsold / (p @ Ap)

        x = lg.axpy(alpha, p, x)
        r = lg.axpy(alpha, Ap, r, multiplier=-1.)

        rsnew = r @ r

        #if np.sqrt(rsnew.get()) < 1e-8:
        #    print("Converged in %i iterations" % (i + 1))
        #    break

        p = lg.axpy(rsnew / rsold, p, r)
        rsold = rsnew

    return x

if __name__ == '__main__':
    connect("172.17.0.1", 10000)

    A = ndarray(2, (1000, 1000), np.float64)
    b = ndarray(1, 1000, np.float64)
    x = ndarray(1, 1000, np.float64)

    #d = (b @ x).get()

    start = time.time()
    x = solve(A, b, x)
    x.evaluate()
    sync()
    print("Execution time = %.6f" % (time.time() - start))
