"""Jacobi iterative solver for a linear system  A x = b.

Uses the splitting  A = D + R  where D = diag(A) and R = A - D.
Each iteration:   x_{k+1} = D^{-1} (b - R x_k)

We build a diagonally dominant SPD matrix so convergence is guaranteed,
then verify the result against NumPy's direct solve.
"""

from charmnumeric.charmnumeric import create_array
from charmnumeric.operations import diag
from charmtyles.core import execute
from charmtyles.interface import CCSInterface
import numpy as np

interface = CCSInterface()
interface.connect('192.168.1.115', 1234, 4)

N = 128
max_iter = 50

# --- Build a diagonally dominant matrix A ------------------------------------
# A = 2*N*I + ones (all off-diagonal entries are 1, diagonal entries are 2*N+1)
# This is SPD and diagonally dominant, so Jacobi converges.
A = create_array((N, N), dtype=np.float64)
A[:, :] = 1.0                          # all ones
A_diag_vec = create_array((N,), dtype=np.float64)
A_diag_vec[:] = 2.0 * N                # extra diagonal weight
D_mat = diag(A_diag_vec)               # diagonal matrix
execute(interface)

A = A + D_mat                           # A = ones + 2N*I  (diag = 2N+1)
execute(interface)

# --- Right-hand side b -------------------------------------------------------
b = create_array((N,), dtype=np.float64)
b[:] = 1.0
execute(interface)

# --- Extract diagonal and compute D_inv (element-wise reciprocal) ------------
d = diag(A)                            # extract diagonal → 1D vector
execute(interface)

d_np = d.get(interface)
# Compute D_inv as a vector on the host and upload it
d_inv_np = 1.0 / d_np
D_inv = create_array((N,), dtype=np.float64)
D_inv[:] = 0.0
execute(interface)
# Set each element (for now, construct from the known constant diagonal)
# Since all diagonal entries are (2N+1), D_inv is a constant vector
d_inv_val = 1.0 / (2.0 * N + 1.0)
D_inv = D_inv + d_inv_val
execute(interface)

# --- Jacobi iteration --------------------------------------------------------
# x_{k+1} = D^{-1} * (b - (A x_k - D x_k))
#          = D^{-1} * (b - A x_k + d * x_k)
#          = D^{-1} * (b - A x_k) + (1 - D^{-1} * d) ... simplified below
#
# Direct form: x = D_inv * (b - R x)  where R = A - diag(A)
# Equivalently: x = D_inv * (b - A x + d * x)

x = create_array((N,), dtype=np.float64)
x[:] = 0.0                             # initial guess
execute(interface)

for it in range(max_iter):
    Ax = A @ x                         # matrix-vector product
    r = b - Ax                         # residual
    # x_new = x + D_inv * r  (Jacobi update: x + D^{-1}(b - Ax))
    x = x + D_inv * r

# --- Retrieve and verify -----------------------------------------------------
result = x.get(interface)

# NumPy reference solution
A_np = np.ones((N, N), dtype=np.float64) + 2.0 * N * np.eye(N, dtype=np.float64)
b_np = np.ones(N, dtype=np.float64)
x_np = np.linalg.solve(A_np, b_np)

print(f"Jacobi iteration ({max_iter} iterations, N={N})")
print(f"  x[0:5]     = {result[:5]}")
print(f"  x_np[0:5]  = {x_np[:5]}")
print(f"  Max error   = {np.max(np.abs(result - x_np)):.2e}")
print(f"  Converged   = {np.allclose(result, x_np, atol=1e-6)}")
