from __future__ import annotations

import numpy as np
import pytest

from charmnumeric.operations import diag
from charmnumeric.charmnumeric import arange, create_array


pytestmark = pytest.mark.integration


def test_elementwise_expression_roundtrip(interface):
    a = create_array((32,), dtype=np.float32)
    b = create_array((32,), dtype=np.float32)

    a[:] = 1.5
    b[:] = -0.5

    got = (2.0 * (a + b + 3.0)).get(interface)
    want = np.full((32,), 8.0, dtype=np.float32)

    np.testing.assert_allclose(got, want)


@pytest.mark.parametrize("shape", [(8,), (4, 4), (4, 4, 4)])
def test_elementwise_add_roundtrip_all_ranks(interface, shape):
    lhs = create_array(shape, dtype=np.float32)
    rhs = create_array(shape, dtype=np.float32)

    lhs[...] = 1.0
    rhs[...] = 1.0

    got = (lhs + rhs).get(interface)
    want = np.full(shape, 2.0, dtype=np.float32)

    np.testing.assert_allclose(got, want)


def test_shifted_slice_expression_get_matches_numpy(interface):
    src = arange(512, dtype=np.float32)

    got = (src[100:300] + src[150:350]).get(interface)
    host = np.arange(512, dtype=np.float32)
    want = host[100:300] + host[150:350]

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


def test_jacobi3d_single_step_matches_numpy(interface):
    u = create_array((4, 4, 4), dtype=np.float32)

    u[0, :, :] = 1.0
    u[-1, :, :] = 1.0
    u[:, 0, :] = 1.0
    u[:, -1, :] = 1.0
    u[:, :, 0] = 1.0
    u[:, :, -1] = 1.0

    u[1:-1, 1:-1, 1:-1] = (1.0 / 6.0) * (
        u[:-2, 1:-1, 1:-1]
        + u[2:, 1:-1, 1:-1]
        + u[1:-1, :-2, 1:-1]
        + u[1:-1, 2:, 1:-1]
        + u[1:-1, 1:-1, :-2]
        + u[1:-1, 1:-1, 2:]
    )

    got = u.get(interface)

    want = np.zeros((4, 4, 4), dtype=np.float32)
    want[0, :, :] = 1.0
    want[-1, :, :] = 1.0
    want[:, 0, :] = 1.0
    want[:, -1, :] = 1.0
    want[:, :, 0] = 1.0
    want[:, :, -1] = 1.0
    want[1:-1, 1:-1, 1:-1] = (1.0 / 6.0) * (
        want[:-2, 1:-1, 1:-1]
        + want[2:, 1:-1, 1:-1]
        + want[1:-1, :-2, 1:-1]
        + want[1:-1, 2:, 1:-1]
        + want[1:-1, 1:-1, :-2]
        + want[1:-1, 1:-1, 2:]
    )

    np.testing.assert_allclose(got, want)


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
