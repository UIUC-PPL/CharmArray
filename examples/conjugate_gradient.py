from charmnumeric.array import connect, ndarray
import charmnumeric.linalg as lg
from charmnumeric.ccs import enable_debug, sync
from charmnumeric.ast import set_max_depth
import numpy as np
import time

set_max_depth(10)

def solve(A, b, x):
    r = b - A @ x
    p = r.copy()
    rsold = r @ r

    for _ in range(10):
        Ap = A @ p
        alpha = rsold / (p @ Ap)

        x = alpha * p + x
        r = alpha * Ap - r
        rsnew = r @ r

        p = (rsnew / rsold) * p + r
        rsold = rsnew

    return x

if __name__ == '__main__':
    connect("172.17.0.1", 10000)

    n = int(1e4)

    A = ndarray(2, (n, n), np.float64, init_value = 1e-6)
    b = ndarray(1, n, np.float64, init_value = 1e-6)
    x = ndarray(1, n, np.float64, init_value = 1e-6)

    # Pre-Compilation
    _ = solve(A, b, x)
    __ = _.get()
    print(__)

    start = time.time()
    x = solve(A, b, x)
    x_charm = x.get()
    print("Execution time (Charm) = %.6f s" % (time.time() - start))

    # Initialize all arrays to 1
    A = np.ones((n, n), dtype=np.float64) * 1e-6
    b = np.ones(n, dtype=np.float64) * 1e-6
    x = np.ones(n, dtype=np.float64) * 1e-6

    start = time.time()
    x_np = solve(A, b, x)
    print("Execution time (NumPy) = %.6f s" % (time.time() - start))

    if np.allclose(x_np, x_charm, atol=1e-5):
        print("[SUCCESS]")
    else:
        print("[FAIL]")
