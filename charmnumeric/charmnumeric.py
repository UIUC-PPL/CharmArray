from charmtyles.core import (
    OverlapType,
    Region,
    FrontendObject,
    FrontendObjectView,
    create_object,
    execute,
    _shape_size,
)
from charmtyles.operation import Operation, fusible_op, nonfusible_op
from charmtyles.interface import to_bytes
from ._native_region import NativeArrayRegion
import numpy as np

_FULL_REGION_CACHE = {}


class DType(object):
    """Element type encoding shared with C++ wire protocol."""
    FLOAT32 = 0
    FLOAT64 = 1
    INT32 = 2
    INT64 = 3

    _from_numpy = {
        np.dtype('float32'): FLOAT32,
        np.dtype('float64'): FLOAT64,
        np.dtype('int32'): INT32,
        np.dtype('int64'): INT64,
    }

    @staticmethod
    def from_numpy(np_dtype):
        """Convert a numpy dtype to wire encoding."""
        return DType._from_numpy.get(np.dtype(np_dtype), DType.FLOAT32)

    _to_numpy = {
        FLOAT32: np.dtype('float32'),
        FLOAT64: np.dtype('float64'),
        INT32: np.dtype('int32'),
        INT64: np.dtype('int64'),
    }

    @staticmethod
    def to_numpy(wire_dtype):
        """Convert a wire dtype encoding back to numpy dtype string."""
        return DType._to_numpy.get(wire_dtype, np.dtype('float32'))

    _promote = {}

    @staticmethod
    def promote(a, b):
        """Return the promoted wire dtype for operands *a* and *b*."""
        key = (min(a, b), max(a, b))
        return DType._promote[key]


DType._promote = {
    (DType.FLOAT32, DType.FLOAT32): DType.FLOAT32,
    (DType.FLOAT32, DType.FLOAT64): DType.FLOAT64,
    (DType.FLOAT32, DType.INT32):   DType.FLOAT64,
    (DType.FLOAT32, DType.INT64):   DType.FLOAT64,
    (DType.FLOAT64, DType.FLOAT64): DType.FLOAT64,
    (DType.FLOAT64, DType.INT32):   DType.FLOAT64,
    (DType.FLOAT64, DType.INT64):   DType.FLOAT64,
    (DType.INT32,   DType.INT32):   DType.INT32,
    (DType.INT32,   DType.INT64):   DType.INT64,
    (DType.INT64,   DType.INT64):   DType.INT64,
}


class ArrayOperation(Operation):
    add = 0
    sub = 1
    mul = 2
    div = 3
    matmul = 4
    tanh = 5
    exp = 6
    tile = 7
    reduce = 8
    matmatmul = 9
    diag = 10


class ArrayRegion(Region):
    __slots__ = (
        'start', 'stop', 'step', '_shape', '_hash',
        '_serialized', '_native'
    )

    def __init__(self, start, stop, step):
        super().__init__()
        self.start = tuple(start)
        self.stop = tuple(stop)
        self.step = tuple(step)
        self._shape = tuple(
            (self.stop[i] - self.start[i] + self.step[i] - 1) // self.step[i]
            for i in range(len(self.start))
        )
        self._hash = hash((self.start, self.stop, self.step))
        self._serialized = None
        self._native = NativeArrayRegion(self.start, self.stop, self.step, False)

    def __str__(self):
        return f"({self.start}, {self.stop}, {self.step})"

    def __hash__(self):
        return self._hash

    def __eq__(self, value):
        if not isinstance(value, Region):
            return False
        if self.is_global or value.is_global:
            return True
        return self.start == value.start and self.stop == value.stop and self.step == value.step

    def serialize(self):
        if self._serialized is not None:
            return self._serialized

        payload = bytearray()
        payload.extend(to_bytes(1 if self.is_global else 0, 'i'))
        if not self.is_global:
            payload.extend(to_bytes(len(self.start), 'i'))
            for i in range(len(self.start)):
                payload.extend(to_bytes(self.start[i], 'i'))
                payload.extend(to_bytes(self.stop[i], 'i'))
                payload.extend(to_bytes(self.step[i], 'i'))
        self._serialized = bytes(payload)
        return self._serialized

    def shape(self):
        return self._shape

    def compose(self, inner):
        """Compose an inner sub-region within this (outer) region.

        outer.compose(inner) returns the absolute region such that
        outer[inner] == root[result].  For example:
          outer = [8:40, 10:50], inner = [0:16, 0:20]
          result = [8:24, 10:30]
        """
        if inner.start == (0,) * len(inner.start) and inner.step == (1,) * len(inner.step):
            if inner.stop == self._shape:
                return self
        ndims = len(inner.start)
        new_start = [0] * ndims
        new_stop = [0] * ndims
        new_step = [0] * ndims
        for d in range(ndims):
            new_start[d] = self.start[d] + inner.start[d] * self.step[d]
            new_stop[d] = self.start[d] + inner.stop[d] * self.step[d]
            new_step[d] = self.step[d] * inner.step[d]
        return ArrayRegion(new_start, new_stop, new_step)

    def overlaps(self, other):
        if not isinstance(other, ArrayRegion):
            raise TypeError(f"ArrayRegion overlap requires ArrayRegion, got {type(other).__name__}")
        return self._native.overlaps(other._native)

    def covers(self, other):
        if not isinstance(other, ArrayRegion):
            raise TypeError(f"ArrayRegion cover check requires ArrayRegion, got {type(other).__name__}")
        return self._native.covers(other._native)

    def intersect(self, other):
        if not isinstance(other, ArrayRegion):
            raise TypeError(f"ArrayRegion intersection requires ArrayRegion, got {type(other).__name__}")
        result = self._native.intersect(other._native)
        if result is None:
            return None
        start, stop, step = result
        return ArrayRegion(start, stop, step)


def _full_region(shape):
    key = tuple(shape)
    region = _FULL_REGION_CACHE.get(key)
    if region is None:
        ndims = len(key)
        region = ArrayRegion(
            start=(0,) * ndims,
            stop=key,
            step=(1,) * ndims,
        )
        _FULL_REGION_CACHE[key] = region
    return region


def _parse_key(key, shape, ndims):
    """Normalize a __getitem__/__setitem__ key into an ArrayRegion."""
    if not isinstance(key, tuple):
        key = (key,)
    if len(key) < ndims:
        key = key + tuple(slice(0, shape[i], 1) for i in range(len(key), ndims))

    start = [0] * ndims
    stop = [0] * ndims
    step = [1] * ndims

    for i, item in enumerate(key):
        if isinstance(item, slice):
            start_i = 0 if item.start is None else item.start
            stop_i = shape[i] if item.stop is None else item.stop
            step_i = 1 if item.step is None else item.step
        else:
            start_i = item
            stop_i = item + 1
            step_i = 1

        if start_i < 0:
            start_i += shape[i]
        if stop_i <= 0:
            stop_i += shape[i]

        start[i] = start_i
        stop[i] = stop_i
        step[i] = step_i

    return ArrayRegion(start, stop, step)


def _broadcast_shape(self_shape, self_size, other):
    """Return the broadcast result shape between self and other."""
    if isinstance(other, (Array, ArrayView)):
        if self_size == 1 and other.size() != 1:
            return other.shape
    return self_shape


def _result_dtype(self, other):
    """Compute the promoted result dtype following numpy rules."""
    if isinstance(other, (Array, ArrayView)) and other.dtype is not None and self.dtype is not None:
        wire = DType.promote(self.wire_dtype(), other.wire_dtype())
        return DType.to_numpy(wire)
    return self.dtype


def _array_add(self, other):
    return fusible_op(self, other, operation=ArrayOperation.add,
                      shape=_broadcast_shape(self.shape, self.size(), other),
                      dtype=_result_dtype(self, other))

def _array_radd(self, other):
    return fusible_op(self, other, operation=ArrayOperation.add,
                      shape=self.shape, dtype=_result_dtype(self, other))

def _array_sub(self, other):
    return fusible_op(self, other, operation=ArrayOperation.sub,
                      shape=_broadcast_shape(self.shape, self.size(), other),
                      dtype=_result_dtype(self, other))

def _array_neg(self):
    return fusible_op(self, -1, operation=ArrayOperation.mul,
                      shape=self.shape, dtype=self.dtype)

def _array_rsub(self, other):
    return fusible_op(other, self, operation=ArrayOperation.sub,
                      shape=self.shape, dtype=_result_dtype(self, other))

def _array_mul(self, other):
    return fusible_op(self, other, operation=ArrayOperation.mul,
                      shape=_broadcast_shape(self.shape, self.size(), other),
                      dtype=_result_dtype(self, other))

def _array_rmul(self, other):
    return fusible_op(self, other, operation=ArrayOperation.mul,
                      shape=self.shape, dtype=_result_dtype(self, other))

def _array_truediv(self, other):
    return fusible_op(self, other, operation=ArrayOperation.div,
                      shape=_broadcast_shape(self.shape, self.size(), other),
                      dtype=_result_dtype(self, other))

def _array_rtruediv(self, other):
    return fusible_op(other, self, operation=ArrayOperation.div,
                      shape=self.shape, dtype=_result_dtype(self, other))

def _array_matmul(self, other):
    if isinstance(other, (Array, ArrayView)) and self.ndims == 1 and other.ndims == 1:
        if self.shape[0] != other.shape[0]:
            raise ValueError(f"Shape mismatch for dot product: ({self.shape[0]},) @ ({other.shape[0]},)")
        return nonfusible_op(self, other, operation=ArrayOperation.reduce,
                             shape=(1,), dtype=_result_dtype(self, other))
    if self.ndims == 3:
        # 3D with singleton dim → dimension-dropped matmul
        dropped = [i for i in range(3) if self.shape[i] == 1]
        if len(dropped) == 1:
            if isinstance(other, (Array, ArrayView)) and other.ndims == 1:
                return _matvec(self, other)
            if isinstance(other, (Array, ArrayView)) and other.ndims >= 2:
                return _matmatmul(self, other)
        raise ValueError(f"3D matmul requires exactly one singleton dim, got shape {self.shape}")
    if self.ndims != 2:
        raise ValueError(f"matmul requires a 2D matrix, got {self.ndims}D")
    if isinstance(other, (Array, ArrayView)) and other.ndims == 1:
        return _matvec(self, other)
    if isinstance(other, (Array, ArrayView)) and other.ndims >= 2:
        if other.ndims == 2 and other.shape[1] == 1:
            temp = create_array((other.shape[0],), dtype=other.dtype)
            temp[:] = other
            return _matvec(self, temp)
        return _matmatmul(self, other)
    raise ValueError(f"matmul requires a 2D operand, got {other.ndims}D")


def _matvec(mat_obj, vec):
    """Matrix-vector multiply.

    mat_obj and vec can be Array or ArrayView. Each operand's region is
    serialized inline, so the C++ handler always knows the sub-region.
    """
    if not isinstance(vec, (Array, ArrayView)):
        raise ValueError("matvec requires an Array vector")

    if vec.ndims == 2 and vec.shape[1] == 1:
        temp = create_array((vec.shape[0],), dtype=vec.dtype)
        temp[:] = vec
        return _matvec(mat_obj, temp)

    if vec.ndims != 1:
        raise ValueError("matvec requires a 1D vector (N,) or 2D column vector (N, 1)")

    if mat_obj.ndims == 2:
        mat_rows, mat_cols = mat_obj.shape[0], mat_obj.shape[1]
    elif mat_obj.ndims == 3:
        dropped = [i for i in range(3) if mat_obj.shape[i] == 1]
        if len(dropped) != 1:
            raise ValueError(
                f"3D matvec requires exactly one singleton dimension, "
                f"got shape {mat_obj.shape}")
        remaining = [i for i in range(3) if i not in dropped]
        mat_rows = mat_obj.shape[remaining[0]]
        mat_cols = mat_obj.shape[remaining[1]]
    else:
        raise ValueError(f"matvec requires a 2D or 3D matrix, got {mat_obj.ndims}D")

    if mat_cols != vec.shape[0]:
        raise ValueError(
            f"Shape mismatch: matrix cols {mat_cols} != vector size {vec.shape[0]}")

    return nonfusible_op(mat_obj, vec, operation=ArrayOperation.matmul,
                         shape=(mat_rows,), dtype=_result_dtype(mat_obj, vec))


def _matmatmul_shape(obj):
    """Return (rows, cols) for a matmatmul operand, handling 2D and dim-dropped 3D."""
    if obj.ndims == 2:
        return obj.shape[0], obj.shape[1]
    if obj.ndims == 3:
        dropped = [i for i in range(3) if obj.shape[i] == 1]
        if len(dropped) != 1:
            raise ValueError(
                f"3D matmatmul requires exactly one singleton dim, got shape {obj.shape}")
        remaining = [i for i in range(3) if i not in dropped]
        return obj.shape[remaining[0]], obj.shape[remaining[1]]
    raise ValueError(f"matmatmul requires 2D or 3D operand, got {obj.ndims}D")


def _matmatmul(lhs, rhs):
    """Matrix-matrix multiply with inline region serialization.

    lhs and rhs can be Array or ArrayView (2D or dimension-dropped 3D).
    Each operand's region is serialized inline so the C++ handler knows
    the sub-region directly.
    """
    if not isinstance(rhs, (Array, ArrayView)):
        raise ValueError("matmatmul requires Array operands")

    lhs_rows, lhs_cols = _matmatmul_shape(lhs)
    rhs_rows, rhs_cols = _matmatmul_shape(rhs)

    if lhs_cols != rhs_rows:
        raise ValueError(
            f"Shape mismatch: ({lhs_rows},{lhs_cols}) @ ({rhs_rows},{rhs_cols})")

    return nonfusible_op(lhs, rhs, operation=ArrayOperation.matmatmul,
                         shape=(lhs_rows, rhs_cols),
                         dtype=_result_dtype(lhs, rhs))


class Array(FrontendObject):
    def __init__(self, **kwargs):
        # Extract Array-specific kwargs before forwarding to FrontendObject
        dtype = kwargs.pop('dtype', None)
        self.dtype = dtype if isinstance(dtype, np.dtype) else (np.dtype(dtype) if dtype is not None else None)
        self._wire_dtype = DType.from_numpy(self.dtype) if self.dtype is not None else 0
        shape = kwargs.pop('shape', None)
        super().__init__(**kwargs)
        self._shape = tuple(shape) if shape is not None else tuple(self.region.shape())
        self.ndims = len(self._shape)
        self._size = _shape_size(self._shape)
        # Ensure region is a concrete ArrayRegion (not global_region) so that
        # fusion shape checks and dependency tracking work correctly.
        if self.region.is_global and self._shape is not None:
            self.region = _full_region(self._shape)

    def wire_dtype(self):
        return self._wire_dtype

    @property
    def shape(self):
        return self._shape

    def size(self):
        return self._size

    def get(self, interface):
        execute(interface)
        data = interface.get(self.ndims, self.name, self.size(), dtype=self.dtype)
        if self.ndims > 1:
            return data.reshape(self._shape)
        return data

    def get_region(self, region, **kwargs):
        kwargs.setdefault('shape', region.shape())
        kwargs.setdefault('dtype', self.dtype)
        kwargs.setdefault('wire_dtype', self._wire_dtype)
        kwargs.setdefault('size', _shape_size(kwargs['shape']))
        return ArrayView(self, region, **kwargs)

    def __getitem__(self, key):
        region = _parse_key(key, self._shape, self.ndims)
        region_shape = region.shape()
        return self.get_region(region, shape=region_shape, dtype=self.dtype,
                               wire_dtype=self._wire_dtype, size=_shape_size(region_shape))

    def __setitem__(self, key, rhs):
        region = _parse_key(key, self._shape, self.ndims)
        self.set_region(region, rhs, shape=region.shape(), dtype=self.dtype)

    __add__ = _array_add
    __radd__ = _array_radd
    __sub__ = _array_sub
    __neg__ = _array_neg
    __rsub__ = _array_rsub
    __mul__ = _array_mul
    __rmul__ = _array_rmul
    __truediv__ = _array_truediv
    __rtruediv__ = _array_rtruediv
    __matmul__ = _array_matmul

    def dot(self, other):
        return _array_matmul(self, other)

    def matvec(self, vec):
        return _matvec(self, vec)


class ArrayView(FrontendObjectView):
    """Array-specific view with arithmetic operators and slicing."""

    def get_region(self, region, **kwargs):
        kwargs.setdefault('shape', region.shape())
        kwargs.setdefault('dtype', self.dtype)
        kwargs.setdefault('wire_dtype', self.wire_dtype())
        kwargs.setdefault('size', _shape_size(kwargs['shape']))
        return ArrayView(self, region, **kwargs)

    def get(self, interface):
        execute(interface)
        data = interface.get(self.ndims, self.name, self.size(), dtype=self.dtype)
        if self.ndims > 1:
            return data.reshape(self.shape)
        return data

    def __getitem__(self, key):
        region = _parse_key(key, self.shape, self.ndims)
        region_shape = region.shape()
        return self.get_region(region, shape=region_shape, dtype=self.dtype,
                               wire_dtype=self.wire_dtype(), size=_shape_size(region_shape))

    def __setitem__(self, key, rhs):
        region = _parse_key(key, self.shape, self.ndims)
        self.set_region(region, rhs, shape=region.shape(), dtype=self.dtype)

    __add__ = _array_add
    __radd__ = _array_radd
    __sub__ = _array_sub
    __neg__ = _array_neg
    __rsub__ = _array_rsub
    __mul__ = _array_mul
    __rmul__ = _array_rmul
    __truediv__ = _array_truediv
    __rtruediv__ = _array_rtruediv
    __matmul__ = _array_matmul

    def dot(self, other):
        return _array_matmul(self, other)

    def matvec(self, vec):
        return _matvec(self, vec)

def create_array(shape, dtype, **kwargs):
    shape = tuple(shape)
    return create_object(
        Array,
        region=ArrayRegion(start=(0,) * len(shape), stop=shape, step=(1,) * len(shape)),
        dtype=dtype,
        **kwargs,
    )
