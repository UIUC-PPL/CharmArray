"""Test matvec with sliced arrays (no temporary alignment copies).

Creates a large matrix and vector, then performs matvec on slices
to verify that the slice-aware cross-partition matmul produces
correct results directly from view metadata.
"""

import charmnumeric as cnp
from charmtyles.core import execute
import numpy as np

interface = cnp.CharmNumericInterface()
interface.connect('192.168.1.114', 1234, 4)

M, N = 128, 128

# Build a matrix with a known pattern: A[i, j] = 1
A = cnp.ones((M, N), dtype=cnp.float64)

# Build a 1D vector: x[i] = i + 1
x = cnp.arange(1, N + 1, dtype=cnp.float64)

# NumPy reference
A_np = np.ones(M * N, dtype=np.float64).reshape(M, N)
x_np = np.arange(1, N + 1, dtype=np.float64)

# ------------------------------------------------------------------
# Test 1: Full matvec (baseline, no slicing)
# ------------------------------------------------------------------
y_full = A.matvec(x)
execute(interface)
result_full = y_full.get(interface)
expected_full = A_np @ x_np
print("Test 1: Full matvec")
print(f"  Match: {np.allclose(result_full.flatten(), expected_full)}")
if not np.allclose(result_full.flatten(), expected_full):
    print(f"  Max error: {np.max(np.abs(result_full.flatten() - expected_full))}")

# ------------------------------------------------------------------
# Test 2: Row slice — A[0:32, :] @ x
# ------------------------------------------------------------------
A_row_slice = A[0:32, :]
y_row = A_row_slice.matvec(x)
execute(interface)
result_row = y_row.get(interface)
expected_row = A_np[0:32, :] @ x_np
print("\nTest 2: Row slice A[0:32, :] @ x")
print(f"  Match: {np.allclose(result_row.flatten(), expected_row)}")
if not np.allclose(result_row.flatten(), expected_row):
    print(f"  Max error: {np.max(np.abs(result_row.flatten() - expected_row))}")

# ------------------------------------------------------------------
# Test 3: Column slice — A[:, 16:48] @ x[16:48]
# ------------------------------------------------------------------
A_col_slice = A[:, 16:48]
x_col_slice = x[16:48]
y_col = A_col_slice.matvec(x_col_slice)
execute(interface)
result_col = y_col.get(interface)
expected_col = A_np[:, 16:48] @ x_np[16:48]
print("\nTest 3: Column slice A[:, 16:48] @ x[16:48]")
print(f"  Match: {np.allclose(result_col.flatten(), expected_col)}")
if not np.allclose(result_col.flatten(), expected_col):
    print(f"  Max error: {np.max(np.abs(result_col.flatten() - expected_col))}")

# ------------------------------------------------------------------
# Test 4: Both row and column slice — A[8:40, 10:50] @ x[10:50]
# ------------------------------------------------------------------
A_both_slice = A[8:40, 10:50]
x_both_slice = x[10:50]
y_both = A_both_slice.matvec(x_both_slice)
execute(interface)
result_both = y_both.get(interface)
expected_both = A_np[8:40, 10:50] @ x_np[10:50]
print("\nTest 4: Both slices A[8:40, 10:50] @ x[10:50]")
print(f"  Match: {np.allclose(result_both.flatten(), expected_both)}")
if not np.allclose(result_both.flatten(), expected_both):
    print(f"  Max error: {np.max(np.abs(result_both.flatten() - expected_both))}")

# ------------------------------------------------------------------
# Test 5: Non-tile-aligned slice — A[3:37, 5:47] @ x[5:47]
#   (tile size is 16, so 3 and 5 are misaligned)
# ------------------------------------------------------------------
A_misaligned = A[3:37, 5:47]
x_misaligned = x[5:47]
y_mis = A_misaligned.matvec(x_misaligned)
execute(interface)
result_mis = y_mis.get(interface)
expected_mis = A_np[3:37, 5:47] @ x_np[5:47]
print("\nTest 5: Non-tile-aligned A[3:37, 5:47] @ x[5:47]")
print(f"  Match: {np.allclose(result_mis.flatten(), expected_mis)}")
if not np.allclose(result_mis.flatten(), expected_mis):
    print(f"  Max error: {np.max(np.abs(result_mis.flatten() - expected_mis))}")

# ------------------------------------------------------------------
# Test 6: 3D array with dimension dropping — A3d[0, :, :] @ x
# ------------------------------------------------------------------
B = 2  # batch size
A3d = cnp.ones((B, M, N), dtype=cnp.float64)

A3d_slice = A3d[0, :, :]  # shape (1, M, N) — singleton dim 0
y_3d = A3d_slice.matvec(x)
execute(interface)
result_3d = y_3d.get(interface)

A3d_np = np.ones((B, M, N), dtype=np.float64)
expected_3d = A3d_np[0, :, :] @ x_np
print("\nTest 6: 3D dim-drop A3d[0, :, :] @ x")
print(f"  Match: {np.allclose(result_3d.flatten(), expected_3d)}")
if not np.allclose(result_3d.flatten(), expected_3d):
    print(f"  Max error: {np.max(np.abs(result_3d.flatten() - expected_3d))}")

# ------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------
all_pass = all([
    np.allclose(result_full.flatten(), expected_full),
    np.allclose(result_row.flatten(), expected_row),
    np.allclose(result_col.flatten(), expected_col),
    np.allclose(result_both.flatten(), expected_both),
    np.allclose(result_mis.flatten(), expected_mis),
    np.allclose(result_3d.flatten(), expected_3d),
])
print(f"\n{'All tests passed!' if all_pass else 'SOME TESTS FAILED'}")
