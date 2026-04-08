import numbers

import numpy as np

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


_FULL_REGION_CACHE = {}
_DEFAULT_DTYPE = np.dtype("float64")


class _CallableInt(int):
    """Int-like object that still supports legacy ``obj.size()`` calls."""

    def __new__(cls, value):
        return int.__new__(cls, int(value))

    def __call__(self):
        return int(self)


class DType(object):
    """Element type encoding shared with the C++ wire protocol."""

    FLOAT32 = 0
    FLOAT64 = 1
    INT32 = 2
    INT64 = 3

    _from_numpy = {
        np.dtype("float32"): FLOAT32,
        np.dtype("float64"): FLOAT64,
        np.dtype("int32"): INT32,
        np.dtype("int64"): INT64,
    }

    @staticmethod
    def from_numpy(np_dtype):
        """Convert a supported numpy dtype to the wire encoding."""

        dtype = _normalize_dtype(np_dtype)
        return DType._from_numpy[dtype]

    _to_numpy = {
        FLOAT32: np.dtype("float32"),
        FLOAT64: np.dtype("float64"),
        INT32: np.dtype("int32"),
        INT64: np.dtype("int64"),
    }

    @staticmethod
    def to_numpy(wire_dtype):
        """Convert a wire dtype encoding back to a numpy dtype."""

        if wire_dtype not in DType._to_numpy:
            raise TypeError(f"Unsupported wire dtype {wire_dtype!r}")
        return DType._to_numpy[wire_dtype]

    @staticmethod
    def promote(a, b):
        """Return the promoted wire dtype for operands *a* and *b*."""

        return DType.from_numpy(np.result_type(DType.to_numpy(a), DType.to_numpy(b)))


def _normalize_dtype(dtype, *, allow_none=False):
    if dtype is None:
        if allow_none:
            return None
        return _DEFAULT_DTYPE

    np_dtype = np.dtype(dtype)
    if np_dtype not in DType._from_numpy:
        raise TypeError(
            "charmnumeric only supports float32, float64, int32, and int64; "
            f"got {np_dtype}"
        )
    return np_dtype


def _normalize_shape(shape):
    if isinstance(shape, numbers.Integral):
        normalized = (int(shape),)
    else:
        normalized = tuple(int(dim) for dim in shape)

    for dim in normalized:
        if dim < 0:
            raise ValueError(f"negative dimensions are not allowed: {normalized}")
    return normalized


def _coerce_scalar(value):
    if isinstance(value, np.ndarray):
        if value.ndim != 0:
            return value
        value = value.item()
    elif isinstance(value, np.generic):
        value = value.item()

    if isinstance(value, bool):
        return int(value)
    if isinstance(value, numbers.Integral):
        return int(value)
    if isinstance(value, numbers.Real):
        return float(value)
    return value


def _is_scalar_like(value):
    if isinstance(value, np.ndarray):
        return value.ndim == 0
    if isinstance(value, np.generic):
        return True
    return isinstance(value, (bool, numbers.Real))


def _apply_ndmin(shape, ndmin):
    ndmin = int(ndmin)
    if ndmin < 0:
        raise ValueError("ndmin must be non-negative")
    if len(shape) >= ndmin:
        return tuple(shape)
    return (1,) * (ndmin - len(shape)) + tuple(shape)


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
        "start",
        "stop",
        "step",
        "_shape",
        "_hash",
        "_serialized",
        "_native",
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
        return (
            self.start == value.start
            and self.stop == value.stop
            and self.step == value.step
        )

    def serialize(self):
        if self._serialized is not None:
            return self._serialized

        payload = bytearray()
        payload.extend(to_bytes(1 if self.is_global else 0, "i"))
        if not self.is_global:
            payload.extend(to_bytes(len(self.start), "i"))
            for i in range(len(self.start)):
                payload.extend(to_bytes(self.start[i], "i"))
                payload.extend(to_bytes(self.stop[i], "i"))
                payload.extend(to_bytes(self.step[i], "i"))
        self._serialized = bytes(payload)
        return self._serialized

    def shape(self):
        return self._shape

    def compose(self, inner):
        """Compose an inner sub-region within this outer region."""

        if (
            inner.start == (0,) * len(inner.start)
            and inner.step == (1,) * len(inner.step)
            and inner.stop == self._shape
        ):
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
            raise TypeError(
                f"ArrayRegion overlap requires ArrayRegion, got {type(other).__name__}"
            )
        return self._native.overlaps(other._native)

    def covers(self, other):
        if not isinstance(other, ArrayRegion):
            raise TypeError(
                f"ArrayRegion cover check requires ArrayRegion, got {type(other).__name__}"
            )
        return self._native.covers(other._native)

    def intersect(self, other):
        if not isinstance(other, ArrayRegion):
            raise TypeError(
                f"ArrayRegion intersection requires ArrayRegion, got {type(other).__name__}"
            )
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


def _expand_key(key, ndims):
    if key is Ellipsis:
        key = (Ellipsis,)
    elif not isinstance(key, tuple):
        key = (key,)

    normalized = []
    saw_ellipsis = False
    explicit_dims = sum(1 for item in key if item is not Ellipsis and item is not None)

    for item in key:
        if item is Ellipsis:
            if saw_ellipsis:
                raise IndexError("an index can only have a single ellipsis")
            saw_ellipsis = True
            fill = ndims - explicit_dims
            if fill < 0:
                raise IndexError("too many indices for array")
            normalized.extend(slice(None) for _ in range(fill))
        elif item is None:
            raise NotImplementedError("np.newaxis is not supported yet")
        else:
            normalized.append(item)

    if len(normalized) > ndims:
        raise IndexError(
            f"too many indices for array: array is {ndims}-dimensional, "
            f"but {len(normalized)} were indexed"
        )

    if len(normalized) < ndims:
        normalized.extend(slice(None) for _ in range(ndims - len(normalized)))

    return tuple(normalized)


def _parse_key(key, shape, ndims):
    """Normalize a ``__getitem__`` or ``__setitem__`` key into an ArrayRegion."""

    key = _expand_key(key, ndims)
    start = [0] * ndims
    stop = [0] * ndims
    step = [1] * ndims

    for i, item in enumerate(key):
        if isinstance(item, slice):
            step_i = 1 if item.step is None else int(item.step)
            if step_i == 0:
                raise ValueError("slice step cannot be zero")
            if step_i < 0:
                raise NotImplementedError("negative slicing is not supported yet")
            start_i, stop_i, step_i = item.indices(shape[i])
        elif isinstance(item, numbers.Integral):
            start_i = int(item)
            if start_i < 0:
                start_i += shape[i]
            if start_i < 0 or start_i >= shape[i]:
                raise IndexError(
                    f"index {item} is out of bounds for axis {i} with size {shape[i]}"
                )
            stop_i = start_i + 1
            step_i = 1
        else:
            raise TypeError(
                "only integers, slices, and ellipsis are valid charmnumeric indices"
            )

        start[i] = start_i
        stop[i] = stop_i
        step[i] = step_i

    return ArrayRegion(start, stop, step)


def _broadcast_shape(lhs_shape, rhs):
    if isinstance(rhs, (Array, ArrayView)):
        rhs_shape = rhs.shape
    else:
        return tuple(lhs_shape)

    try:
        return tuple(np.broadcast_shapes(lhs_shape, rhs_shape))
    except ValueError as exc:
        raise ValueError(
            f"operands could not be broadcast together with shapes "
            f"{lhs_shape} and {rhs_shape}"
        ) from exc


def _operand_dtype(value):
    if isinstance(value, (Array, ArrayView)):
        return value.dtype
    # Scalars do not promote the array dtype (matches numpy value-based casting:
    # a Python float does not upcast a float32 array to float64).
    return None


def _result_dtype(lhs, rhs):
    other_dtype = _operand_dtype(rhs)
    if other_dtype is None:
        return lhs.dtype
    return _normalize_dtype(np.result_type(lhs.dtype, other_dtype))


def _coerce_operand(value):
    if isinstance(value, (Array, ArrayView)):
        return value
    if _is_scalar_like(value):
        return _coerce_scalar(value)
    if isinstance(value, (str, bytes)):
        return value

    try:
        host = np.asarray(value)
    except Exception:
        return value

    if host.ndim == 0:
        return _coerce_scalar(host.item())
    return asarray(host)


def _array_add(self, other):
    other = _coerce_operand(other)
    return fusible_op(
        self,
        other,
        operation=ArrayOperation.add,
        shape=_broadcast_shape(self.shape, other),
        dtype=_result_dtype(self, other),
    )


def _array_radd(self, other):
    other = _coerce_operand(other)
    return fusible_op(
        other,
        self,
        operation=ArrayOperation.add,
        shape=_broadcast_shape(self.shape, other),
        dtype=_result_dtype(self, other),
    )


def _array_sub(self, other):
    other = _coerce_operand(other)
    return fusible_op(
        self,
        other,
        operation=ArrayOperation.sub,
        shape=_broadcast_shape(self.shape, other),
        dtype=_result_dtype(self, other),
    )


def _array_neg(self):
    return fusible_op(
        self,
        -1,
        operation=ArrayOperation.mul,
        shape=self.shape,
        dtype=self.dtype,
    )


def _array_rsub(self, other):
    other = _coerce_operand(other)
    return fusible_op(
        other,
        self,
        operation=ArrayOperation.sub,
        shape=_broadcast_shape(self.shape, other),
        dtype=_result_dtype(self, other),
    )


def _array_mul(self, other):
    other = _coerce_operand(other)
    return fusible_op(
        self,
        other,
        operation=ArrayOperation.mul,
        shape=_broadcast_shape(self.shape, other),
        dtype=_result_dtype(self, other),
    )


def _array_rmul(self, other):
    other = _coerce_operand(other)
    return fusible_op(
        other,
        self,
        operation=ArrayOperation.mul,
        shape=_broadcast_shape(self.shape, other),
        dtype=_result_dtype(self, other),
    )


def _array_truediv(self, other):
    other = _coerce_operand(other)
    return fusible_op(
        self,
        other,
        operation=ArrayOperation.div,
        shape=_broadcast_shape(self.shape, other),
        dtype=_result_dtype(self, other),
    )


def _array_rtruediv(self, other):
    other = _coerce_operand(other)
    return fusible_op(
        other,
        self,
        operation=ArrayOperation.div,
        shape=_broadcast_shape(self.shape, other),
        dtype=_result_dtype(self, other),
    )


def _array_matmul(self, other):
    other = _coerce_operand(other)
    if not isinstance(other, (Array, ArrayView)):
        raise ValueError("matmul does not support scalar operands")

    if self.ndims == 1 and other.ndims == 1:
        if self.shape[0] != other.shape[0]:
            raise ValueError(
                f"Shape mismatch for dot product: ({self.shape[0]},) @ ({other.shape[0]},)"
            )
        return nonfusible_op(
            self,
            other,
            operation=ArrayOperation.reduce,
            shape=(1,),
            dtype=_result_dtype(self, other),
        )

    if self.ndims == 3:
        dropped = [i for i in range(3) if self.shape[i] == 1]
        if len(dropped) == 1:
            if other.ndims == 1:
                return _matvec(self, other)
            if other.ndims >= 2:
                return _matmatmul(self, other)
        raise ValueError(
            f"3D matmul requires exactly one singleton dim, got shape {self.shape}"
        )

    if self.ndims != 2:
        raise ValueError(f"matmul requires a 2D matrix, got {self.ndims}D")

    if other.ndims == 1:
        return _matvec(self, other)
    if other.ndims >= 2:
        if other.ndims == 2 and other.shape[1] == 1:
            temp = create_array((other.shape[0],), dtype=other.dtype)
            temp[:] = other
            return _matvec(self, temp)
        return _matmatmul(self, other)

    raise ValueError(f"matmul requires a 2D operand, got {other.ndims}D")


def _matvec(mat_obj, vec):
    """Matrix-vector multiply."""

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
                "3D matvec requires exactly one singleton dimension, "
                f"got shape {mat_obj.shape}"
            )
        remaining = [i for i in range(3) if i not in dropped]
        mat_rows = mat_obj.shape[remaining[0]]
        mat_cols = mat_obj.shape[remaining[1]]
    else:
        raise ValueError(f"matvec requires a 2D or 3D matrix, got {mat_obj.ndims}D")

    if mat_cols != vec.shape[0]:
        raise ValueError(
            f"Shape mismatch: matrix cols {mat_cols} != vector size {vec.shape[0]}"
        )

    return nonfusible_op(
        mat_obj,
        vec,
        operation=ArrayOperation.matmul,
        shape=(mat_rows,),
        dtype=_result_dtype(mat_obj, vec),
    )


def _matmatmul_shape(obj):
    """Return ``(rows, cols)`` for a matmatmul operand."""

    if obj.ndims == 2:
        return obj.shape[0], obj.shape[1]
    if obj.ndims == 3:
        dropped = [i for i in range(3) if obj.shape[i] == 1]
        if len(dropped) != 1:
            raise ValueError(
                f"3D matmatmul requires exactly one singleton dim, got shape {obj.shape}"
            )
        remaining = [i for i in range(3) if i not in dropped]
        return obj.shape[remaining[0]], obj.shape[remaining[1]]
    raise ValueError(f"matmatmul requires 2D or 3D operand, got {obj.ndims}D")


def _matmatmul(lhs, rhs):
    """Matrix-matrix multiply with inline region serialization."""

    if not isinstance(rhs, (Array, ArrayView)):
        raise ValueError("matmatmul requires Array operands")

    lhs_rows, lhs_cols = _matmatmul_shape(lhs)
    rhs_rows, rhs_cols = _matmatmul_shape(rhs)

    if lhs_cols != rhs_rows:
        raise ValueError(
            f"Shape mismatch: ({lhs_rows},{lhs_cols}) @ ({rhs_rows},{rhs_cols})"
        )

    return nonfusible_op(
        lhs,
        rhs,
        operation=ArrayOperation.matmatmul,
        shape=(lhs_rows, rhs_cols),
        dtype=_result_dtype(lhs, rhs),
    )


class Array(FrontendObject):
    __array_priority__ = 1000

    def __init__(self, **kwargs):
        dtype = _normalize_dtype(kwargs.pop("dtype", None), allow_none=True)
        self.dtype = dtype
        self._wire_dtype = DType.from_numpy(self.dtype) if self.dtype is not None else 0

        shape = kwargs.pop("shape", None)
        if shape is not None:
            shape = _normalize_shape(shape)

        super().__init__(**kwargs)

        self._shape = tuple(shape) if shape is not None else tuple(self.region.shape())
        self.ndims = len(self._shape)
        self._size = _shape_size(self._shape)

        if self.region.is_global and self._shape is not None:
            self.region = _full_region(self._shape)

    def wire_dtype(self):
        return self._wire_dtype

    @property
    def shape(self):
        return self._shape

    @property
    def ndim(self):
        return self.ndims

    @property
    def size(self):
        return _CallableInt(self._size)

    @property
    def itemsize(self):
        return 0 if self.dtype is None else self.dtype.itemsize

    @property
    def nbytes(self):
        return int(self.size) * self.itemsize

    def __len__(self):
        if self.ndims == 0:
            raise TypeError("len() of unsized object")
        return self._shape[0]

    def __bool__(self):
        raise ValueError(
            "The truth value of a charmnumeric array is ambiguous. "
            "Use .get(interface) to materialize it first."
        )

    def __array__(self, dtype=None, copy=None):
        raise TypeError(
            "charmnumeric arrays are lazy frontend objects. "
            "Call .get(interface) before converting to numpy."
        )

    def __repr__(self):
        dtype_name = None if self.dtype is None else self.dtype.name
        return f"Array(shape={self.shape}, dtype={dtype_name}, name={self.name})"

    def get(self, interface):
        execute(interface)
        data = interface.get(self.ndims, self.name, self.size(), dtype=self.dtype)
        if self.ndims > 1:
            return data.reshape(self._shape)
        return data

    def get_region(self, region, **kwargs):
        kwargs.setdefault("shape", region.shape())
        kwargs.setdefault("dtype", self.dtype)
        kwargs.setdefault("wire_dtype", self._wire_dtype)
        kwargs.setdefault("size", _shape_size(kwargs["shape"]))
        return ArrayView(self, region, **kwargs)

    def __getitem__(self, key):
        region = _parse_key(key, self._shape, self.ndims)
        region_shape = region.shape()
        return self.get_region(
            region,
            shape=region_shape,
            dtype=self.dtype,
            wire_dtype=self._wire_dtype,
            size=_shape_size(region_shape),
        )

    def __setitem__(self, key, rhs):
        region = _parse_key(key, self._shape, self.ndims)
        rhs = _coerce_assignment_operand(rhs, region.shape(), self.dtype)
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

    def copy(self):
        result = empty(self.shape, dtype=self.dtype)
        result[...] = self
        return result

    def fill(self, value):
        self[...] = value

    def astype(self, dtype, copy=True):
        dtype = _normalize_dtype(dtype)
        if dtype != self.dtype:
            raise NotImplementedError(
                "dtype conversion for existing charmnumeric arrays is not implemented yet"
            )
        return self.copy() if copy else self


class ArrayView(FrontendObjectView):
    """Array-specific view with arithmetic operators and slicing."""

    __array_priority__ = 1000

    @property
    def ndim(self):
        return self.ndims

    @property
    def size(self):
        return _CallableInt(self._size)

    @property
    def itemsize(self):
        return 0 if self.dtype is None else self.dtype.itemsize

    @property
    def nbytes(self):
        return int(self.size) * self.itemsize

    def __len__(self):
        if self.ndims == 0:
            raise TypeError("len() of unsized object")
        return self.shape[0]

    def __bool__(self):
        raise ValueError(
            "The truth value of a charmnumeric array is ambiguous. "
            "Use .get(interface) to materialize it first."
        )

    def __array__(self, dtype=None, copy=None):
        raise TypeError(
            "charmnumeric arrays are lazy frontend objects. "
            "Call .get(interface) before converting to numpy."
        )

    def __repr__(self):
        dtype_name = None if self.dtype is None else self.dtype.name
        return f"ArrayView(shape={self.shape}, dtype={dtype_name}, name={self.name})"

    def get_region(self, region, **kwargs):
        kwargs.setdefault("shape", region.shape())
        kwargs.setdefault("dtype", self.dtype)
        kwargs.setdefault("wire_dtype", self.wire_dtype())
        kwargs.setdefault("size", _shape_size(kwargs["shape"]))
        return ArrayView(self, region, **kwargs)

    def get(self, interface):
        materialized = empty(self.shape, dtype=self.dtype)
        materialized[...] = self
        return materialized.get(interface)

    def __getitem__(self, key):
        region = _parse_key(key, self.shape, self.ndims)
        region_shape = region.shape()
        return self.get_region(
            region,
            shape=region_shape,
            dtype=self.dtype,
            wire_dtype=self.wire_dtype(),
            size=_shape_size(region_shape),
        )

    def __setitem__(self, key, rhs):
        region = _parse_key(key, self.shape, self.ndims)
        rhs = _coerce_assignment_operand(rhs, region.shape(), self.dtype)
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

    def copy(self):
        result = empty(self.shape, dtype=self.dtype)
        result[...] = self
        return result

    def fill(self, value):
        self[...] = value

    def astype(self, dtype, copy=True):
        dtype = _normalize_dtype(dtype)
        if dtype != self.dtype:
            raise NotImplementedError(
                "dtype conversion for existing charmnumeric arrays is not implemented yet"
            )
        return self.copy() if copy else self


def create_array(shape, dtype=_DEFAULT_DTYPE, **kwargs):
    shape = _normalize_shape(shape)
    dtype = _normalize_dtype(dtype)
    return create_object(
        Array,
        region=ArrayRegion(start=(0,) * len(shape), stop=shape, step=(1,) * len(shape)),
        dtype=dtype,
        shape=shape,
        **kwargs,
    )


def _assign_host_data(target, values):
    host = np.asarray(values, dtype=target.dtype)
    if host.shape != target.shape:
        raise ValueError(f"cannot assign data with shape {host.shape} into shape {target.shape}")

    if host.size == 0:
        return target

    first = host.reshape(-1)[0]
    if host.size == 1 or np.all(host == first):
        target[...] = _coerce_scalar(first)
        return target

    for index in np.ndindex(host.shape):
        target[index] = _coerce_scalar(host[index])
    return target


def _array_from_host_data(values, dtype=None, copy=True, ndmin=0):
    requested_dtype = _normalize_dtype(dtype, allow_none=True)

    if copy:
        host = np.array(values, dtype=requested_dtype, copy=True)
    else:
        host = np.asarray(values, dtype=requested_dtype)

    while host.ndim < ndmin:
        host = np.expand_dims(host, axis=0)

    if host.ndim == 0:
        raise NotImplementedError("0D charmnumeric arrays are not supported yet")

    host_dtype = _normalize_dtype(host.dtype)
    result = create_array(host.shape, dtype=host_dtype)
    return _assign_host_data(result, host)


def _shape_dtype_from_like(obj):
    if isinstance(obj, (Array, ArrayView)):
        return obj.shape, obj.dtype

    host = np.asarray(obj)
    if host.ndim == 0:
        raise NotImplementedError("0D charmnumeric arrays are not supported yet")
    return host.shape, _normalize_dtype(host.dtype)


def _coerce_assignment_operand(rhs, shape, dtype):
    if isinstance(rhs, (Array, ArrayView)):
        return rhs

    if _is_scalar_like(rhs):
        return _coerce_scalar(rhs)

    host = np.asarray(rhs, dtype=_normalize_dtype(dtype))
    if host.ndim == 0:
        return _coerce_scalar(host.item())

    try:
        host = np.broadcast_to(host, shape)
    except ValueError as exc:
        raise ValueError(
            f"could not broadcast input from shape {host.shape} into shape {shape}"
        ) from exc

    return _array_from_host_data(host, dtype=host.dtype, copy=False)


def empty(shape, dtype=_DEFAULT_DTYPE):
    return create_array(shape, dtype=dtype)


def zeros(shape, dtype=_DEFAULT_DTYPE):
    return empty(shape, dtype=dtype)


def full(shape, fill_value, dtype=None):
    shape = _normalize_shape(shape)

    if _is_scalar_like(fill_value):
        target_dtype = _normalize_dtype(dtype, allow_none=True)
        if target_dtype is None:
            target_dtype = _normalize_dtype(np.asarray(fill_value).dtype)
        result = empty(shape, dtype=target_dtype)
        result[...] = _coerce_scalar(fill_value)
        return result

    host = np.asarray(fill_value, dtype=_normalize_dtype(dtype, allow_none=True))
    if host.ndim == 0:
        return full(shape, host.item(), dtype=dtype)

    target_dtype = _normalize_dtype(host.dtype)
    try:
        host = np.broadcast_to(host, shape)
    except ValueError as exc:
        raise ValueError(
            f"could not broadcast fill_value from shape {host.shape} into shape {shape}"
        ) from exc

    return _array_from_host_data(host, dtype=target_dtype, copy=False)


def ones(shape, dtype=_DEFAULT_DTYPE):
    return full(shape, 1, dtype=dtype)


def empty_like(a, dtype=None, shape=None):
    base_shape, base_dtype = _shape_dtype_from_like(a)
    target_shape = base_shape if shape is None else _normalize_shape(shape)
    target_dtype = base_dtype if dtype is None else _normalize_dtype(dtype)
    return empty(target_shape, dtype=target_dtype)


def zeros_like(a, dtype=None, shape=None):
    base_shape, base_dtype = _shape_dtype_from_like(a)
    target_shape = base_shape if shape is None else _normalize_shape(shape)
    target_dtype = base_dtype if dtype is None else _normalize_dtype(dtype)
    return zeros(target_shape, dtype=target_dtype)


def ones_like(a, dtype=None, shape=None):
    base_shape, base_dtype = _shape_dtype_from_like(a)
    target_shape = base_shape if shape is None else _normalize_shape(shape)
    target_dtype = base_dtype if dtype is None else _normalize_dtype(dtype)
    return ones(target_shape, dtype=target_dtype)


def full_like(a, fill_value, dtype=None, shape=None):
    base_shape, base_dtype = _shape_dtype_from_like(a)
    target_shape = base_shape if shape is None else _normalize_shape(shape)
    target_dtype = base_dtype if dtype is None else _normalize_dtype(dtype)
    return full(target_shape, fill_value, dtype=target_dtype)


def asarray(a, dtype=None):
    requested_dtype = _normalize_dtype(dtype, allow_none=True)

    if isinstance(a, (Array, ArrayView)):
        if requested_dtype is None or requested_dtype == a.dtype:
            return a
        raise NotImplementedError(
            "dtype conversion for existing charmnumeric arrays is not implemented yet"
        )

    return _array_from_host_data(a, dtype=requested_dtype, copy=False)


def array(a, dtype=None, copy=True, ndmin=0):
    requested_dtype = _normalize_dtype(dtype, allow_none=True)

    if isinstance(a, (Array, ArrayView)):
        target_dtype = a.dtype if requested_dtype is None else requested_dtype
        if target_dtype != a.dtype:
            raise NotImplementedError(
                "dtype conversion for existing charmnumeric arrays is not implemented yet"
            )

        target_shape = _apply_ndmin(a.shape, ndmin)
        if not copy and target_shape == a.shape:
            return a

        result = empty(target_shape, dtype=target_dtype)
        result[...] = a
        return result

    return _array_from_host_data(a, dtype=requested_dtype, copy=copy, ndmin=ndmin)


def copy(a, order="K"):
    return array(a, copy=True)


def copyto(dst, src):
    if not isinstance(dst, (Array, ArrayView)):
        raise TypeError("copyto destination must be a charmnumeric Array or ArrayView")
    dst[...] = src
    return dst


def arange(start, stop=None, step=1, dtype=None):
    requested_dtype = _normalize_dtype(dtype, allow_none=True)
    host = np.arange(start, stop=stop, step=step, dtype=requested_dtype)
    return _array_from_host_data(host, dtype=host.dtype, copy=False)


def eye(N, M=None, k=0, dtype=_DEFAULT_DTYPE):
    rows = int(N)
    cols = rows if M is None else int(M)
    result = zeros((rows, cols), dtype=dtype)

    row = max(0, -int(k))
    col = max(0, int(k))
    length = max(0, min(rows - row, cols - col))
    for offset in range(length):
        result[row + offset, col + offset] = 1
    return result


def identity(n, dtype=_DEFAULT_DTYPE):
    return eye(n, dtype=dtype)
