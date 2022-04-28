import sys
import numpy as np
from pyproject.ccs import to_bytes, from_bytes, send_command_raw, send_command, \
    send_command_async, connect, get_creation_command, get_operation_command, \
    get_fetch_command, Handlers

def create_ndarray(name, ndim, shape, dtype):
    return ndarray(ndim, shape, dtype, name=name)

class ndarray:
    def __init__(self, ndim, shape, dtype, init_value=None, name=None):
        """
        This is the wrapper class for AUM array objects.
        The argument 'name' should be None except when wrapping
        an array that already exists on the AUM backend server
        """
        if ndim > 2:
            raise NotImplementedError("Arrays of dimensionality greater than"
                                      "2 not supported yet")
        self.dtype = dtype
        self.ndim = ndim
        self.itemsize = np.dtype(dtype).itemsize
        self.init_value = init_value
        if isinstance(shape, np.ndarray) or isinstance(shape, list) or \
                isinstance(shape, tuple):
            self.shape = np.asarray(shape, dtype=np.int32)
        else:
            self.shape = np.asarray([shape], dtype=np.int32)
        self.size = np.prod(self.shape)
        if name:
            self.name = name
        else:
            cmd = get_creation_command(self)
            self.name = send_command(Handlers.creation_handler, cmd)

    def __del__(self):
        cmd = to_bytes(self.name, 'l')
        send_command_async(Handlers.delete_handler, cmd)

    def __len__(self):
        return self.shape[0]

    def __str__(self):
        pass

    def __repr__(self):
        pass

    def __neg__(self):
        return self * -1

    def __add__(self, other):
        if (self.shape != other.shape).any():
            raise RuntimeError("Shape mismatch %s and %s" % (self.shape,
                                                             other.shape))
        cmd = get_operation_command("+", [self, other])
        res = send_command(Handlers.operation_handler, cmd)
        return create_ndarray(res, self.ndim, self.shape, self.dtype)

    def __radd__(self, other):
        return self + other

    def __sub__(self, other):
        if (self.shape != other.shape).any():
            raise RuntimeError("Shape mismatch %s and %s" % (self.shape,
                                                             other.shape))
        cmd = get_operation_command("-", [self, other])
        res = send_command(Handlers.operation_handler, cmd)
        return create_ndarray(res, self.ndim, self.shape, self.dtype)

    def __rsub__(self, other):
        return -1 * (self - other)

    def __mul__(self, other):
        if self.ndim > 0 or (isinstance(other, ndarray) and other.ndim > 0):
            RuntimeError("Cannote multiply two arrays")
        if isinstance(other, ndarray):
            op = '*'
        elif isinstance(other, float) or isinstance(other, int):
            op = '*s'
        cmd = get_operation_command(op, [self, other])
        res = send_command(Handlers.operation_handler, cmd)
        return create_ndarray(res, self.ndim, self.shape, self.dtype)

    def __rmul__(self, other):
        return self * other

    def __truediv__(self, other):
        if self.ndim > 0 or other.ndim > 0:
            RuntimeError("Cannote divide two arrays")
        if isinstance(other, ndarray):
            op = '/'
        elif isinstance(other, float) or isinstance(other, int):
            op = '/s'
        cmd = get_operation_command(op, [self, other])
        res = send_command(Handlers.operation_handler, cmd)
        return create_ndarray(res, self.ndim, self.shape, self.dtype)

    def __matmul__(self, other):
        if self.ndim == 2 and other.ndim == 2:
            res_ndim = 2
            if self.shape[1] != other.shape[0]:
                raise RuntimeError("Shape mismatch")
            res_shape = np.array([self.shape[0], other.shape[1]],
                                 dtype=np.int32)
        elif self.ndim == 2 and other.ndim == 1:
            res_ndim = 1
            if self.shape[1] != other.shape[0]:
                raise RuntimeError("Shape mismatch")
            res_shape = np.array([self.shape[0]], dtype=np.int32)
        elif self.ndim == 1 and other.ndim == 1:
            res_ndim = 0
            if self.shape[0] != other.shape[0]:
                raise RuntimeError("Shape mismatch")
            res_shape = np.array([], dtype=np.int32)
        else:
            raise RuntimeError("Dimension mismatch")

        cmd = get_operation_command("@", [self, other])
        res = send_command(Handlers.operation_handler, cmd)
        return create_ndarray(res, res_ndim, res_shape, self.dtype)

    def get(self):
        cmd = get_fetch_command(self)
        if self.ndim == 0:
            total_size = self.itemsize
            data_bytes = send_command_raw(Handlers.fetch_handler, cmd, reply_size=total_size)
            return from_bytes(data_bytes, np.dtype(self.dtype).char)
        else:
            total_size = self.size * self.itemsize
            data_ptr = send_command(Handlers.fetch_handler, cmd, reply_size=total_size)
            data = cast(memoryview, data_ptr)
            return np.frombuffer(data, np.dtype(self.dtype)).copy()

    def copy(self):
        cmd = get_operation_command("copy", [self])
        res = send_command(Handlers.operation_handler, cmd)
        return create_ndarray(res, self.ndim, self.shape, self.dtype)

