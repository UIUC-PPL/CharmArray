"""Conjugate Gradient solver using charmnumeric.

Solves the linear system  A x = b  where A is symmetric positive-definite.
We build a simple SPD matrix  A = I + ones  (the identity plus a constant
matrix) so the answer is easy to verify with NumPy.
"""

from charmnumeric.charmnumeric import create_array
from charmtyles.core import execute
from charmtyles.interface import CCSInterface
import numpy as np

N = 65

# --- Build a symmetric positive-definite matrix A = (N+1)*I + ones --------
# This is SPD because eigenvalues are (N+1) (multiplicity N-1) and (2N+1).
h = (N - 1) // 2 + 1
x = create_array((h, h), dtype=np.float64)
y = create_array((N, N), dtype=np.float64)
y[:] = 1.0
x[1:-1, 1:-1] = y[2:-2:2, 2:-2:2]

interface = CCSInterface()
interface.connect('192.168.1.115', 1234, 4)

res = x.get(interface)
expected = np.zeros((h, h), dtype=np.float64)
expected[1:-1, 1:-1] = np.ones((N, N), dtype=np.float64)[2:-2:2, 2:-2:2]

print("Result x:")
print(res.flatten())
print("Expected x:")
print(expected.flatten())
assert np.array_equal(res, expected), "charmnumeric result does not match NumPy"
