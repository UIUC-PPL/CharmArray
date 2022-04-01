import sys
import numpy as np
from pyccs import Server

server = None

def create_ndarray(name, ndim, shape, dtype):
    return ndarray(ndim, shape, dtype, name)

def send_command(handler, msg, reply_size=8):
    server.send_request(handler, 0, msg)
    return int.from_bytes(server.receive_response(reply_size), "little")

def connect(server_ip, server_port):
    global server
    server = Server(server_ip, server_port)
    server.connect()

class ndarray:
    def __init__(self, ndim, shape, dtype, name=None):
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
        self.shape = np.asarray(shape, dtype=np.int32)
        self.size = np.prod(self.shape)
        self._creation_handler = b'aum_creation'
        self._operation_handler = b'aum_operation'
        self._fetch_handler = b'aum_fetch'
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
        cmd = int(self.ndim).to_bytes(4, "little")
        for s in self.shape:
            cmd += int(s).to_bytes(8, "little")
        return cmd

    def _get_fetch_command(self):
        """
        Generate CCS command to fetch entire array data
        """
        pass

    def _get_operation_command(self, operation, other):
        """
        Generate CCS command for operation
        """
        pass

    def __del__(self):
        pass

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
        res = send_command(self._operation_handler, cmd)
        return create_ndarray(res, self.ndim, self.shape, self.dtype)

    def __radd__(self, other):
        return self + other

    def __sub__(self, other):
        if (self.shape != other.shape).any():
            raise RuntimeError("Shape mismatch %s and %s" % (self.shape,
                                                             other.shape))
        cmd = self._get_operation_command("-", other)
        res = send_command(self._operation_handler, cmd)
        return create_ndarray(res, self.ndim, self.shape, self.dtype)

    def __rsub__(self, other):
        pass

    def __mul__(self, other):
        pass

    def __rmul__(self, other):
        return self * other

    def __matmul__(self, other):
        cmd = self._get_operation_command("@", other)
        res = send_command(self._operation_handler, cmd)
        return create_ndarray(res, res_ndim, res_shape, self.dtype)

    def to_numpy(self):
        total_size = self.size * self.dtype.itemsize
        cmd = self._get_fetch_command()
        data_ptr = send_command(self._fetch_handler, cmd, reply_size=total_size)
        data = cast(memoryview, data_ptr)
        return np.frombuffer(data, np.dtype(self.dtype)).copy()

