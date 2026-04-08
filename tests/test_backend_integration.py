from __future__ import annotations

import numpy as np
import pytest

from charmnumeric.operations import diag
from charmnumeric.charmnumeric import create_array


pytestmark = pytest.mark.integration


def test_elementwise_expression_roundtrip(interface):
    a = create_array((32,), dtype=np.float32)
    b = create_array((32,), dtype=np.float32)

    a[:] = 1.5
    b[:] = -0.5

    got = (2.0 * (a + b + 3.0)).get(interface)
    want = np.full((32,), 8.0, dtype=np.float32)

    np.testing.assert_allclose(got, want)


def test_strided_slice_assignment_roundtrip(interface):
    n = 17
    coarse_n = (n - 1) // 2 + 1

    coarse = create_array((coarse_n, coarse_n), dtype=np.float64)
    fine = create_array((n, n), dtype=np.float64)

    coarse[:, :] = 0.0
    fine[:, :] = 1.0
    coarse[1:-1, 1:-1] = fine[2:-2:2, 2:-2:2]

    got = coarse.get(interface)
    want = np.zeros((coarse_n, coarse_n), dtype=np.float64)
    want[1:-1, 1:-1] = np.ones((n, n), dtype=np.float64)[2:-2:2, 2:-2:2]

    np.testing.assert_array_equal(got, want)


@pytest.mark.parametrize("k", [0, 1, -1])
def test_diag_vector_matches_numpy(interface, k):
    vector = create_array((8,), dtype=np.float64)
    vector[:] = 2.0

    got = diag(vector, k=k).get(interface)
    want = np.diag(np.full(8, 2.0, dtype=np.float64), k=k)

    np.testing.assert_allclose(got, want)


def test_diag_matrix_extract_matches_numpy(interface):
    matrix = create_array((6, 10), dtype=np.float64)
    matrix[:, :] = 3.0

    got = diag(matrix, k=2).get(interface)
    want = np.diag(np.full((6, 10), 3.0, dtype=np.float64), k=2)

    np.testing.assert_allclose(got, want)
