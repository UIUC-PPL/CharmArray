"""Test matrix-matrix multiplication (SUMMA) with full and sliced arrays.

Creates matrices and performs matmatmul in various configurations,
verifying against NumPy reference results.
"""

from charmnumeric.charmnumeric import create_array
from charmtyles.core import execute
from charmtyles.interface import CCSInterface
import numpy as np

interface = CCSInterface()
interface.connect('192.168.1.115', 1234, 4)

M, K, N = 128, 128, 128

# Build matrices with known patterns
# A[i, j] = i + j + 1, B[i, j] = i - j + 1
A = create_array((M, K), dtype=np.float64)
B = create_array((K, N), dtype=np.float64)
A = A + 1
B = B + 1
# for i in range(M):
#     for j in range(K):
#         A[i:i+1, j:j+1] = float(i + j + 1)
# for i in range(K):
#     for j in range(N):
#         B[i:i+1, j:j+1] = float(i - j + 1)

# NumPy reference
A_np = np.ones((M, K), dtype=np.float64)
B_np = np.ones((K, N), dtype=np.float64)

execute(interface)

# ------------------------------------------------------------------
# Test 1: Full matmul — A @ B
# ------------------------------------------------------------------
C = A @ B
execute(interface)
result = C.get(interface)
expected = A_np @ B_np
print("Test 1: Full matmul A @ B")
print(f"  Match: {np.allclose(result, expected)}")
if not np.allclose(result, expected):
    print(f"  Max error: {np.max(np.abs(result - expected))}")

# ------------------------------------------------------------------
# Test 2: Non-square — A(128×64) @ B(64×256)
# ------------------------------------------------------------------
M2, K2, N2 = 128, 64, 256
A2 = create_array((M2, K2), dtype=np.float64)
B2 = create_array((K2, N2), dtype=np.float64)
A2 = A2 + 1  # all ones
B2 = B2 + 1  # all ones
execute(interface)

C2 = A2 @ B2
execute(interface)
result2 = C2.get(interface)
A2_np = np.ones((M2, K2), dtype=np.float64)
B2_np = np.ones((K2, N2), dtype=np.float64)
expected2 = A2_np @ B2_np
print("\nTest 2: Non-square (128x64) @ (64x256)")
print(f"  Match: {np.allclose(result2, expected2)}")
if not np.allclose(result2, expected2):
    print(f"  Max error: {np.max(np.abs(result2 - expected2))}")

# ------------------------------------------------------------------
# Test 3: Row-sliced A — A[0:32, :] @ B
# ------------------------------------------------------------------
A_row = A[0:32, :]
C3 = A_row @ B
execute(interface)
result3 = C3.get(interface)
expected3 = A_np[0:32, :] @ B_np
print("\nTest 3: Row-sliced A[0:32, :] @ B")
print(f"  Match: {np.allclose(result3, expected3)}")
if not np.allclose(result3, expected3):
    print(f"  Max error: {np.max(np.abs(result3 - expected3))}")

# ------------------------------------------------------------------
# Test 4: Col-sliced B — A @ B[:, 16:48]
# ------------------------------------------------------------------
B_col = B[:, 16:48]
C4 = A @ B_col
execute(interface)
result4 = C4.get(interface)
expected4 = A_np @ B_np[:, 16:48]
print("\nTest 4: Col-sliced A @ B[:, 16:48]")
print(f"  Match: {np.allclose(result4, expected4)}")
if not np.allclose(result4, expected4):
    print(f"  Max error: {np.max(np.abs(result4 - expected4))}")

# ------------------------------------------------------------------
# Test 5: Both sliced — A[8:40, 10:50] @ B[10:50, 5:45]
# ------------------------------------------------------------------
A_both = A[8:40, 10:50]
B_both = B[10:50, 5:45]
C5 = A_both @ B_both
execute(interface)
result5 = C5.get(interface)
expected5 = A_np[8:40, 10:50] @ B_np[10:50, 5:45]
print("\nTest 5: Both sliced A[8:40, 10:50] @ B[10:50, 5:45]")
print(f"  Match: {np.allclose(result5, expected5)}")
if not np.allclose(result5, expected5):
    print(f"  Max error: {np.max(np.abs(result5 - expected5))}")

# ------------------------------------------------------------------
# Test 6: Non-tile-aligned — A[3:37, 5:47] @ B[5:47, 7:39]
# ------------------------------------------------------------------
A_mis = A[3:37, 5:47]
B_mis = B[5:47, 7:39]
C6 = A_mis @ B_mis
execute(interface)
result6 = C6.get(interface)
expected6 = A_np[3:37, 5:47] @ B_np[5:47, 7:39]
print("\nTest 6: Non-tile-aligned A[3:37, 5:47] @ B[5:47, 7:39]")
print(f"  Match: {np.allclose(result6, expected6)}")
if not np.allclose(result6, expected6):
    print(f"  Max error: {np.max(np.abs(result6 - expected6))}")

# ------------------------------------------------------------------
# Test 7: Chain — (A @ B) @ x (matmatmul then matvec)
# ------------------------------------------------------------------
x = create_array((N,), dtype=np.float64)
x = x + 1  # all ones
execute(interface)

C_chain = A @ B
y = C_chain.matvec(x)
execute(interface)
result7 = y.get(interface)
x_np = np.ones(N, dtype=np.float64)
expected7 = (A_np @ B_np) @ x_np
print("\nTest 7: Chain (A @ B) @ x")
print(f"  Match: {np.allclose(result7.flatten(), expected7)}")
if not np.allclose(result7.flatten(), expected7):
    print(f"  Max error: {np.max(np.abs(result7.flatten() - expected7))}")

# ------------------------------------------------------------------
# Test 8: 3D dim-drop — A3d[0, :, :] @ B
# ------------------------------------------------------------------
Batch = 2
A3d = create_array((Batch, M, K), dtype=np.float64)
A3d = A3d + 1  # all ones
execute(interface)

A3d_slice = A3d[0, :, :]  # shape (1, M, K) — singleton dim 0
C8 = A3d_slice @ B
execute(interface)
result8 = C8.get(interface)
A3d_np = np.ones((Batch, M, K), dtype=np.float64)
expected8 = A3d_np[0, :, :] @ B_np
print("\nTest 8: 3D dim-drop A3d[0, :, :] @ B")
print(f"  Match: {np.allclose(result8, expected8)}")
if not np.allclose(result8, expected8):
    print(f"  Max error: {np.max(np.abs(result8 - expected8))}")

# ------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------
all_pass = all([
    np.allclose(result, expected),
    np.allclose(result2, expected2),
    np.allclose(result3, expected3),
    np.allclose(result4, expected4),
    np.allclose(result5, expected5),
    np.allclose(result6, expected6),
    np.allclose(result7.flatten(), expected7),
    np.allclose(result8, expected8),
])
print(f"\n{'All tests passed!' if all_pass else 'SOME TESTS FAILED'}")
