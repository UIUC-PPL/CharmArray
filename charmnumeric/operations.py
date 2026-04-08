import numpy as np

from charmtyles.operation import fusible_op, nonfusible_op

from .charmnumeric import (
    Array,
    ArrayOperation,
    ArrayView,
    _coerce_scalar,
    _is_scalar_like,
    arange,
    array,
    asarray,
    copy,
    copyto,
    create_array,
    empty,
    empty_like,
    eye,
    full,
    full_like,
    identity,
    ones,
    ones_like,
    zeros,
    zeros_like,
)


def _coerce_binary_operand(value):
    if isinstance(value, (Array, ArrayView)):
        return value
    if _is_scalar_like(value):
        return _coerce_scalar(value)
    return asarray(value)


def tanh(x):
    if _is_scalar_like(x):
        return np.tanh(_coerce_scalar(x))
    x = asarray(x)
    return fusible_op(x, operation=ArrayOperation.tanh, shape=x.shape, dtype=x.dtype)


def exp(x):
    if _is_scalar_like(x):
        return np.exp(_coerce_scalar(x))
    x = asarray(x)
    return fusible_op(x, operation=ArrayOperation.exp, shape=x.shape, dtype=x.dtype)


def tile(x, reps):
    if _is_scalar_like(x):
        return array(np.tile(_coerce_scalar(x), reps))

    x = asarray(x)
    if isinstance(reps, int):
        reps = (reps,)
    reps = tuple(int(rep) for rep in reps)

    out_ndims = max(x.ndims, len(reps))
    padded_shape = (1,) * (out_ndims - x.ndims) + x.shape
    padded_reps = (1,) * (out_ndims - len(reps)) + reps

    out_shape = tuple(s * r for s, r in zip(padded_shape, padded_reps))
    return nonfusible_op(
        x,
        *padded_reps,
        operation=ArrayOperation.tile,
        shape=out_shape,
        dtype=x.dtype,
    )


def add(x1, x2):
    if _is_scalar_like(x1) and _is_scalar_like(x2):
        return _coerce_scalar(x1) + _coerce_scalar(x2)
    return _coerce_binary_operand(x1) + _coerce_binary_operand(x2)


def subtract(x1, x2):
    if _is_scalar_like(x1) and _is_scalar_like(x2):
        return _coerce_scalar(x1) - _coerce_scalar(x2)
    return _coerce_binary_operand(x1) - _coerce_binary_operand(x2)


def multiply(x1, x2):
    if _is_scalar_like(x1) and _is_scalar_like(x2):
        return _coerce_scalar(x1) * _coerce_scalar(x2)
    return _coerce_binary_operand(x1) * _coerce_binary_operand(x2)


def divide(x1, x2):
    if _is_scalar_like(x1) and _is_scalar_like(x2):
        return _coerce_scalar(x1) / _coerce_scalar(x2)
    return _coerce_binary_operand(x1) / _coerce_binary_operand(x2)


def matmul(x1, x2):
    if _is_scalar_like(x1) or _is_scalar_like(x2):
        raise ValueError("matmul does not support scalar operands")
    return asarray(x1) @ asarray(x2)


def dot(x1, x2):
    if _is_scalar_like(x1) and _is_scalar_like(x2):
        return _coerce_scalar(x1) * _coerce_scalar(x2)

    lhs = _coerce_binary_operand(x1)
    rhs = _coerce_binary_operand(x2)
    if _is_scalar_like(lhs) or _is_scalar_like(rhs):
        return lhs * rhs
    return lhs.dot(rhs)


def norm2(x):
    """Compute the squared L2 norm of a 1D array."""

    x = asarray(x)
    if x.ndims != 1:
        raise ValueError(f"norm2 requires a 1D array, got {x.ndims}D")
    return x.dot(x)


def diag(v, k=0):
    """Construct a diagonal matrix or extract a diagonal."""

    v = asarray(v)

    if v.ndims == 1:
        n = v.shape[0] + abs(k)
        return nonfusible_op(v, k, operation=ArrayOperation.diag, shape=(n, n), dtype=v.dtype)
    if v.ndims == 2:
        rows, cols = v.shape
        if k >= 0:
            diag_len = max(0, min(rows, cols - k))
        else:
            diag_len = max(0, min(rows + k, cols))
        if diag_len == 0:
            raise ValueError(f"k={k} is out of range for shape ({rows}, {cols})")
        return nonfusible_op(
            v,
            k,
            operation=ArrayOperation.diag,
            shape=(diag_len,),
            dtype=v.dtype,
        )
    raise ValueError(f"diag requires a 1D or 2D array, got {v.ndims}D")


__all__ = [
    "add",
    "arange",
    "array",
    "asarray",
    "copy",
    "copyto",
    "create_array",
    "diag",
    "divide",
    "dot",
    "empty",
    "empty_like",
    "exp",
    "eye",
    "full",
    "full_like",
    "identity",
    "matmul",
    "multiply",
    "norm2",
    "ones",
    "ones_like",
    "subtract",
    "tanh",
    "tile",
    "zeros",
    "zeros_like",
]
