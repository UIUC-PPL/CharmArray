from charmnumeric.array import connect, ndarray
from charmnumeric.ast import set_max_depth
import numpy as np
import time

set_max_depth(10)

def generate_2D(N, corners=True):
    if corners:
        print(
            "Generating %dx%d 2-D adjacency system with corners..."
            % (N**2, N**2)
        )
        A = np.zeros((N**2, N**2)) + 8 * np.eye(N**2)
    else:
        print(
            "Generating %dx%d 2-D adjacency system without corners..."
            % (N**2, N**2)
        )
        A = np.zeros((N**2, N**2)) + 4 * np.eye(N**2)
    # These are the same for both cases
    off_one = np.full(N**2 - 1, -1, dtype=np.float64)
    A += np.diag(off_one, k=1)
    A += np.diag(off_one, k=-1)
    off_N = np.full(N * (N - 1), -1, dtype=np.float64)
    A += np.diag(off_N, k=N)
    A += np.diag(off_N, k=-N)
    # If we have corners then we have four more cases
    if corners:
        off_N_plus = np.full(N * (N - 1) - 1, -1, dtype=np.float64)
        A += np.diag(off_N_plus, k=N + 1)
        A += np.diag(off_N_plus, k=-(N + 1))
        off_N_minus = np.full(N * (N - 1) + 1, -1, dtype=np.float64)
        A += np.diag(off_N_minus, k=N - 1)
        A += np.diag(off_N_minus, k=-(N - 1))
    # Then we can generate a random b matrix
    b = np.random.rand(N**2)
    return A, b


def solve(A, b, x_cp):
    x = x_cp.copy()
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
    connect("127.0.0.1", 10000)

    n = 50

    A_np, b_np = generate_2D(n)
    A = ndarray(2, (n**2, n**2), np.float64, nparr = A_np)
    b = ndarray(1, n**2, np.float64, nparr = b_np)
    x = ndarray(1, A.shape[1], np.float64, init_value = 0)

    # Pre-Compilation
    _ = solve(A, b, x)
    __ = _.get()
    print(__)

    start = time.time()
    x = solve(A, b, x)
    x_charm = x.get()
    print("Execution time (Charm) = %.6f s" % (time.time() - start))


    x = np.zeros(A_np.shape[1], dtype=np.float64)
    start = time.time()
    x_np = solve(A_np, b_np, x)
    print("Execution time (NumPy) = %.6f s" % (time.time() - start))

    if np.allclose(x_np, x_charm, atol=1e-5):
        print("[SUCCESS]")
    else:
        print("[FAIL]")
