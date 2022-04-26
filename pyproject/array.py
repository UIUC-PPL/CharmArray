import sys
import struct
import numpy as np
from pyccs import Server

server = None

OPCODES = {'+': 1, '-': 2, '*': 3, '/': 4, '@': 5, 'copy': 6}

def to_bytes(value, dtype='i'):
    return struct.pack(dtype, value)

def from_bytes(bvalue, dtype='i'):
    return struct.unpack(dtype, bvalue)[0]

def create_ndarray(name, ndim, shape, dtype):
    return ndarray(ndim, shape, dtype, name=name)

def send_command_raw(handler, msg, reply_size):
    server.send_request(handler, 0, msg)
    return server.receive_response(reply_size)

def send_command(handler, msg, reply_size=8):
    return from_bytes(send_command_raw(handler, msg, reply_size), 'l')

def send_command_async(handler, msg):
    server.send_request(handler, 0, msg)

def connect(server_ip, server_port):
    global server
    server = Server(server_ip, server_port)
    server.connect()

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
        if isinstance(shape, np.ndarray) or isinstance(shape, list):
            self.shape = np.asarray(shape, dtype=np.int32)
        else:
            self.shape = np.asarray([shape], dtype=np.int32)
        self.size = np.prod(self.shape)
        self._creation_handler = b'aum_creation'
        self._binop_handler = b'aum_binop'
        self._monop_handler = b'aum_monop'
        self._fetch_handler = b'aum_fetch'
        self._delete_handler = b'aum_delete'
        self._exit_handler = b'aum_exit'
        if name:
            self.name = name
        else:
            cmd = self._get_creation_command()
            self.name = send_command(self._creation_handler, cmd)

    def _get_creation_command(self):
        """
        Generate array creation CCS command
        """
        cmd = to_bytes(self.ndim, 'i')
        cmd += to_bytes(self.init_value is not None, '?')
        for s in self.shape:
            cmd += to_bytes(s, 'l')
        if self.init_value is not None:
            cmd += to_bytes(self.init_value, 'd')
        return cmd

    def _get_fetch_command(self):
        """
        Generate CCS command to fetch entire array data
        """
        cmd = to_bytes(self.name, 'l')
        return cmd

    def _get_operation_command(self, operation, other):
        """
        Generate CCS command for operation
        """
        if operation not in OPCODES:
            raise NotImplementedError("Operation %s not supported"
                                      % operation)
        opcode = OPCODES.get(operation)
        cmd = to_bytes(opcode, 'i')
        cmd += to_bytes(self.name, 'l')
        cmd += to_bytes(other.name, 'l')
        return cmd

    def _get_copy_command(self):
        """
        Generate CCS command for copy
        """
        opcode = OPCODES.get('copy')
        cmd = to_bytes(opcode, 'i')
        cmd += to_bytes(self.name, 'l')
        return cmd

    def __del__(self):
        cmd = to_bytes(self.name, 'l')
        send_command_async(self._delete_handler, cmd)

    def __len__(self):
        return self.shape[0]

    def __str__(self):
        pass

    def __repr__(self):
        pass

    def __add__(self, other):
        if (self.shape != other.shape).any():
            raise RuntimeError("Shape mismatch %s and %s" % (self.shape,
                                                             other.shape))
        cmd = self._get_operation_command("+", other)
        res = send_command(self._binop_handler, cmd)
        return create_ndarray(res, self.ndim, self.shape, self.dtype)

    def __radd__(self, other):
        return self + other

    def __sub__(self, other):
        if (self.shape != other.shape).any():
            raise RuntimeError("Shape mismatch %s and %s" % (self.shape,
                                                             other.shape))
        cmd = self._get_operation_command("-", other)
        res = send_command(self._binop_handler, cmd)
        return create_ndarray(res, self.ndim, self.shape, self.dtype)

    def __rsub__(self, other):
        pass

    def __mul__(self, other):
        pass

    def __rmul__(self, other):
        return self * other

    def __truediv__(self, other):
        if self.ndim > 0 or other.ndim > 0:
            RuntimeError("Cannote divide two arrays")
        cmd = self._get_operation_command("/", other)
        res = send_command(self._binop_handler, cmd)
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

        cmd = self._get_operation_command("@", other)
        res = send_command(self._binop_handler, cmd)
        return create_ndarray(res, res_ndim, res_shape, self.dtype)

    def get(self):
        cmd = self._get_fetch_command()
        if self.ndim == 0:
            total_size = self.itemsize
            data_bytes = send_command_raw(self._fetch_handler, cmd, reply_size=total_size)
            return from_bytes(data_bytes, np.dtype(self.dtype).char)
        else:
            total_size = self.size * self.itemsize
            data_ptr = send_command(self._fetch_handler, cmd, reply_size=total_size)
            data = cast(memoryview, data_ptr)
            return np.frombuffer(data, np.dtype(self.dtype)).copy()

    def copy(self):
        cmd = self._get_copy_command()
        res = send_command(self._monop_handler, cmd)
        return create_ndarray(res, self.ndim, self.shape, self.dtype)

