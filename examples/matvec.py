from charmnumeric.charmnumeric import create_array
from charmtyles.core import plot_execution_state, execute
from charmtyles.interface import CCSInterface
import numpy as np
import sys

# --- Matrix-vector multiply example ---
# Supports two modes:
#   --cross   : cross-partition matvec (1D vector on 1D partition)
#   (default) : same-partition matvec (2D column vector on 2D partition)

cross_partition = '--cross' in sys.argv

M, N = 48, 48

A = create_array((M, N), dtype=np.float64)

# Set A to a known pattern: A[i, j] = i + j
for i in range(M):
    A[i, :] = float(i)
col_vals = create_array((M, N), dtype=np.float64)
for j in range(N):
    col_vals[:, j] = float(j)
A = A + col_vals

if cross_partition:
    # Cross-partition: 1D vector on a separate 1D partition
    x = create_array((N,), dtype=np.float64)
    x[0:N] = 1.0
else:
    # Same-partition: 2D column vector on the 2D partition
    x = create_array((N, 1), dtype=np.float64)
    x[:, 0] = 1.0

# Compute y = A @ x using matvec
y = A.matvec(x)

# Connect to backend and execute
interface = CCSInterface()
interface.connect('192.168.1.114', 1234, 4)
execute(interface)

# Retrieve results
result = y.get(interface)
print("y = A @ x:")
print(result)

# Verify against numpy
A_np = np.zeros((M, N), dtype=np.float64)
for i in range(M):
    for j in range(N):
        A_np[i, j] = i + j
x_np = np.ones((N,), dtype=np.float64)
y_expected = A_np @ x_np

print("\nExpected (numpy):")
print(y_expected)
print("\nMatch:", np.allclose(result.flatten(), y_expected))
