from charmtyles.operation import fusible_op, nonfusible_op
from .charmnumeric import ArrayOperation, Array, ArrayView, create_array
import numpy as np

def tanh(x):
    return fusible_op(x, operation=ArrayOperation.tanh, shape=x.shape, dtype=x.dtype)

def exp(x):
    return fusible_op(x, operation=ArrayOperation.exp, shape=x.shape, dtype=x.dtype)

def tile(x, reps):
    if isinstance(reps, int):
        reps = (reps,)
    reps = tuple(reps)

    # Pad reps or input shape so they match in length (output ndims)
    out_ndims = max(x.ndims, len(reps))
    padded_shape = (1,) * (out_ndims - x.ndims) + x.shape
    padded_reps = (1,) * (out_ndims - len(reps)) + reps

    out_shape = tuple(s * r for s, r in zip(padded_shape, padded_reps))
    return nonfusible_op(x, *padded_reps, operation=ArrayOperation.tile,
                         shape=out_shape, dtype=x.dtype)

def zeros(shape, dtype=np.float32):
    arr = create_array(shape, dtype=dtype)
    return arr

def norm2(x):
    """Compute the squared L2 norm of a 1D array.

    Returns a scalar Array containing dot(x, x) = sum(x_i^2).
    Take the square root of the result after ``.get()`` to obtain
    the Euclidean norm equivalent to ``np.linalg.norm(x)``.
    """
    if not isinstance(x, (Array, ArrayView)):
        raise TypeError("norm requires an Array or ArrayView")
    if x.ndims != 1:
        raise ValueError(f"norm requires a 1D array, got {x.ndims}D")
    return x.dot(x)


def diag(v, k=0):
    """Construct a diagonal matrix or extract a diagonal.

    If *v* is 1-D, return a 2-D array with *v* on the *k*-th diagonal.
    If *v* is 2-D, return the *k*-th diagonal as a 1-D array.
    Semantics match ``numpy.diag``.
    """
    if not isinstance(v, (Array, ArrayView)):
        raise TypeError("diag requires an Array or ArrayView")

    if v.ndims == 1:
        n = v.shape[0] + abs(k)
        return nonfusible_op(v, k, operation=ArrayOperation.diag,
                             shape=(n, n), dtype=v.dtype)
    elif v.ndims == 2:
        m, n = v.shape[0], v.shape[1]
        if k >= 0:
            diag_len = max(0, min(m, n - k))
        else:
            diag_len = max(0, min(m + k, n))
        if diag_len == 0:
            raise ValueError(f"k={k} is out of range for shape ({m}, {n})")
        return nonfusible_op(v, k, operation=ArrayOperation.diag,
                             shape=(diag_len,), dtype=v.dtype)
    else:
        raise ValueError(f"diag requires a 1D or 2D array, got {v.ndims}D")
