import numpy as np

from . import random
from .charmnumeric import (
    Array,
    ArrayView,
    DType,
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
from .interface import CharmNumericInterface, LocalCluster
from .operations import add, diag, divide, dot, exp, matmul, multiply, norm2, subtract, tanh, tile


__version__ = "0.1.dev"

ndarray = Array
float32 = np.float32
float64 = np.float64
int32 = np.int32
int64 = np.int64
dtype = np.dtype
newaxis = None
pi = np.pi
e = np.e

__all__ = [
    "__version__",
    "Array",
    "ArrayView",
    "CharmNumericInterface",
    "DType",
    "LocalCluster",
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
    "dtype",
    "e",
    "empty",
    "empty_like",
    "exp",
    "eye",
    "float32",
    "float64",
    "full",
    "full_like",
    "identity",
    "int32",
    "int64",
    "matmul",
    "multiply",
    "ndarray",
    "newaxis",
    "norm2",
    "ones",
    "ones_like",
    "pi",
    "random",
    "subtract",
    "tanh",
    "tile",
    "zeros",
    "zeros_like",
]
