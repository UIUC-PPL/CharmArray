"""Conjugate Gradient solver using charmnumeric.

Solves the linear system  A x = b  where A is symmetric positive-definite.
We build a simple SPD matrix  A = I + ones  (the identity plus a constant
matrix) so the answer is easy to verify with NumPy.
"""

from charmnumeric.charmnumeric import create_array
from charmtyles.core import execute
from charmtyles.interface import CCSInterface
import numpy as np

N = 128
max_iter = 2

# --- Build a symmetric positive-definite matrix A = (N+1)*I + ones --------
# This is SPD because eigenvalues are (N+1) (multiplicity N-1) and (2N+1).
A = create_array((N, N), dtype=np.float64)
for i in range(N):
    A[i, :] = 1.0          # all ones
    A[i, i] = N + 1.0      # add N to diagonal -> diag = N+1

# --- Right-hand side b = [1, 2, ..., N] -----------------------------------
# (Not an eigenvector of A, so CG needs multiple iterations.)
b = create_array((N,), dtype=np.float64)
for i in range(N):
    b[i:i+1] = float(i + 1)

# --- CG iteration ---------------------------------------------------------
# x0 = 0, r0 = b - A*x0 = b, p0 = r0
x = create_array((N,), dtype=np.float64)

r = create_array((N,), dtype=np.float64)
r[:] = b

p = create_array((N,), dtype=np.float64)
p[:] = b

rtr = r @ r          # r^T r  (dot product via matvec on column vec)

interface = CCSInterface()
interface.connect('192.168.1.115', 1234, 4)

tol = 1e-12
check_every = 1

for k in range(max_iter):
    Ap = A @ p                # matrix-vector product
    pAp = p @ Ap              # p^T A p
    alpha = rtr / pAp               # step length (scalar)

    x = x + alpha * p               # update solution
    r = r - alpha * Ap              # update residual

    rtr_new = r @ r           # new r^T r
    beta = rtr_new / rtr            # improvement ratio
    p = r + beta * p                # update search direction

    rtr = rtr_new

    if (k + 1) % check_every == 0 or k == 0:
        rtr_val = rtr.get(interface)
        print(f"  iter {k+1}: rtr = {rtr_val.item():.6e}")
        if rtr_val.item() < tol:
            print(f"  Converged at iteration {k+1}")
            break

execute(interface)

result = x.get(interface)
print("CG solution x:")
print(result.flatten())

# --- Verify against NumPy --------------------------------------------------
A_np = np.ones((N, N), dtype=np.float64)
np.fill_diagonal(A_np, N + 1.0)
b_np = np.arange(1, N + 1, dtype=np.float64)
x_expected = np.linalg.solve(A_np, b_np)

print("\nExpected (numpy):")
print(x_expected)
print("\nMatch:", np.allclose(result.flatten(), x_expected))
