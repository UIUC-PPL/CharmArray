"""Test diag function with numpy.diag semantics.

Tests both modes:
  1D → 2D: construct diagonal matrix from vector
  2D → 1D: extract diagonal from matrix

Also tests the k offset parameter for super/sub-diagonals.
"""

from charmnumeric.charmnumeric import create_array
from charmnumeric.operations import diag
from charmtyles.core import execute
from charmtyles.interface import CCSInterface
import numpy as np

interface = CCSInterface()
interface.connect('192.168.1.114', 1234, 4)

N = 128

# ------------------------------------------------------------------
# Test 1: 1D → 2D — diag(v) constructs diagonal matrix (k=0)
# ------------------------------------------------------------------
v = create_array((N,), dtype=np.float64)
v = v + 1  # all ones
execute(interface)

D = diag(v)
execute(interface)
result1 = D.get(interface)
v_np = np.ones(N, dtype=np.float64)
expected1 = np.diag(v_np)
print("Test 1: diag(v) — 1D->2D, k=0")
print(f"  Match: {np.allclose(result1, expected1)}")
if not np.allclose(result1, expected1):
    print(f"  Max error: {np.max(np.abs(result1 - expected1))}")

# ------------------------------------------------------------------
# Test 2: 2D → 1D — diag(A) extracts main diagonal (k=0)
# ------------------------------------------------------------------
A = create_array((N, N), dtype=np.float64)
A = A + 1  # all ones
execute(interface)

d = diag(A)
execute(interface)
result2 = d.get(interface)
A_np = np.ones((N, N), dtype=np.float64)
expected2 = np.diag(A_np)
print("\nTest 2: diag(A) — 2D->1D, k=0")
print(f"  Match: {np.allclose(result2, expected2)}")
if not np.allclose(result2, expected2):
    print(f"  Max error: {np.max(np.abs(result2 - expected2))}")

# ------------------------------------------------------------------
# Test 3: 1D → 2D with k=1 (super-diagonal)
# ------------------------------------------------------------------
v2 = create_array((N,), dtype=np.float64)
v2 = v2 + 2
execute(interface)

D3 = diag(v2, k=1)
execute(interface)
result3 = D3.get(interface)
v2_np = np.full(N, 2.0, dtype=np.float64)
expected3 = np.diag(v2_np, k=1)
print("\nTest 3: diag(v, k=1) — 1D->2D, super-diagonal")
print(f"  Match: {np.allclose(result3, expected3)}")
if not np.allclose(result3, expected3):
    print(f"  Max error: {np.max(np.abs(result3 - expected3))}")

# ------------------------------------------------------------------
# Test 4: 1D → 2D with k=-1 (sub-diagonal)
# ------------------------------------------------------------------
D4 = diag(v2, k=-1)
execute(interface)
result4 = D4.get(interface)
expected4 = np.diag(v2_np, k=-1)
print("\nTest 4: diag(v, k=-1) — 1D->2D, sub-diagonal")
print(f"  Match: {np.allclose(result4, expected4)}")
if not np.allclose(result4, expected4):
    print(f"  Max error: {np.max(np.abs(result4 - expected4))}")

# ------------------------------------------------------------------
# Test 5: 2D → 1D with k=1 (extract super-diagonal)
# ------------------------------------------------------------------
d5 = diag(A, k=1)
execute(interface)
result5 = d5.get(interface)
expected5 = np.diag(A_np, k=1)
print("\nTest 5: diag(A, k=1) — 2D->1D, extract super-diagonal")
print(f"  Match: {np.allclose(result5, expected5)}")
if not np.allclose(result5, expected5):
    print(f"  Max error: {np.max(np.abs(result5 - expected5))}")

# ------------------------------------------------------------------
# Test 6: 2D → 1D with k=-1 (extract sub-diagonal)
# ------------------------------------------------------------------
d6 = diag(A, k=-1)
execute(interface)
result6 = d6.get(interface)
expected6 = np.diag(A_np, k=-1)
print("\nTest 6: diag(A, k=-1) — 2D->1D, extract sub-diagonal")
print(f"  Match: {np.allclose(result6, expected6)}")
if not np.allclose(result6, expected6):
    print(f"  Max error: {np.max(np.abs(result6 - expected6))}")

# ------------------------------------------------------------------
# Test 7: Non-square matrix — extract diagonal
# ------------------------------------------------------------------
M, K = 64, 128
B = create_array((M, K), dtype=np.float64)
B = B + 3
execute(interface)

d7 = diag(B)
execute(interface)
result7 = d7.get(interface)
B_np = np.full((M, K), 3.0, dtype=np.float64)
expected7 = np.diag(B_np)
print("\nTest 7: diag(B) — non-square (64x128), k=0")
print(f"  Match: {np.allclose(result7, expected7)}")
if not np.allclose(result7, expected7):
    print(f"  Max error: {np.max(np.abs(result7 - expected7))}")

# ------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------
all_pass = all([
    np.allclose(result1, expected1),
    np.allclose(result2, expected2),
    np.allclose(result3, expected3),
    np.allclose(result4, expected4),
    np.allclose(result5, expected5),
    np.allclose(result6, expected6),
    np.allclose(result7, expected7),
])
print(f"\n{'All tests passed!' if all_pass else 'SOME TESTS FAILED'}")
