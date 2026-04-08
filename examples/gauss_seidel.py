"""Red-Black Gauss-Seidel solver for 2D Poisson equation using charmnumeric.

Solves  -laplacian(phi) = f  on a unit square with zero Dirichlet BCs.
Uses red-black ordering via strided slicing for parallel-safe updates.
"""

from charmnumeric.charmnumeric import create_array
from charmtyles.core import execute
from charmtyles.interface import CCSInterface
import numpy as np

interface = CCSInterface()
interface.connect('192.168.1.115', 1234, 4)

N = 512
h = 1.0 / (N - 1)
h2 = h ** 2
max_iter = 10

# --- Setup arrays ---
phi = create_array((N, N), dtype=np.float64)
phi[:, :] = 0.0

f = create_array((N, N), dtype=np.float64)
f[:, :] = 0.0
execute(interface)

# Point source in the middle
f[N // 2, N // 2] = -100.0
execute(interface)

# --- Red-Black Gauss-Seidel iterations ---
for it in range(max_iter):
    # RED points: (i+j) even
    # Pattern 1: odd rows, odd cols
    phi[1:-1:2, 1:-1:2] = 0.25 * (
        phi[0:-2:2, 1:-1:2] + phi[2::2, 1:-1:2]
        + phi[1:-1:2, 0:-2:2] + phi[1:-1:2, 2::2]
        - h2 * f[1:-1:2, 1:-1:2]
    )
    # Pattern 2: even rows, even cols
    phi[2:-1:2, 2:-1:2] = 0.25 * (
        phi[1:-2:2, 2:-1:2] + phi[3::2, 2:-1:2]
        + phi[2:-1:2, 1:-2:2] + phi[2:-1:2, 3::2]
        - h2 * f[2:-1:2, 2:-1:2]
    )

    # BLACK points: (i+j) odd
    # Pattern 1: odd rows, even cols
    phi[1:-1:2, 2:-1:2] = 0.25 * (
        phi[0:-2:2, 2:-1:2] + phi[2::2, 2:-1:2]
        + phi[1:-1:2, 1:-2:2] + phi[1:-1:2, 3::2]
        - h2 * f[1:-1:2, 2:-1:2]
    )
    # Pattern 2: even rows, odd cols
    phi[2:-1:2, 1:-1:2] = 0.25 * (
        phi[1:-2:2, 1:-1:2] + phi[3::2, 1:-1:2]
        + phi[2:-1:2, 0:-2:2] + phi[2:-1:2, 2::2]
        - h2 * f[2:-1:2, 1:-1:2]
    )

    #execute(interface)

result = phi.get(interface)
print(f"Gauss-Seidel ({max_iter} iterations, N={N})")
print(f"  phi max = {np.max(result):.6f}")
print(f"  phi min = {np.min(result):.6f}")
