import sys
import warnings
import numpy as np
from charmtiles.ast import get_max_depth, ASTNode
from charmtiles.ccs import to_bytes, from_bytes, send_command_raw, send_command, \
    send_command_async, connect, get_creation_command, \
    get_name, get_fetch_command, Handlers, OPCODES, is_debug


def create_ndarray(ndim, dtype, shape=None, name=None, command_buffer=None):
    return ndarray(ndim, dtype=dtype, shape=shape, name=name,
                   command_buffer=command_buffer)


def from_numpy(nparr):
    return ndarray(nparr.ndim, dtype=nparr.dtype, shape=nparr.shape,
                   nparr=nparr)


class ndarray:
    def __init__(self, ndim, shape=None, dtype=np.float64, init_value=None,
                 nparr=None, name=None, command_buffer=None):
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
        self.command_buffer = command_buffer
        if isinstance(shape, np.ndarray) or isinstance(shape, list) or \
                isinstance(shape, tuple):
            self._shape = np.asarray(shape, dtype=np.int32)
        elif shape is not None:
            self._shape = np.asarray([shape], dtype=np.int32)
        else:
            self._shape = np.zeros(self.ndim, dtype=np.int32)
        self.valid = False
        if command_buffer is None:
            self.valid = True
            if name:
                self.name = name
                self.command_buffer = ASTNode(self.name, 0, [self])
            else:
                self.name = get_name()
                if nparr is not None:
                    buf = nparr.tobytes()
                else:
                    buf = None
                cmd = get_creation_command(self, self.name, self._shape, buf=buf)
                if not send_command(Handlers.creation_handler, cmd,
                                    reply_type='?'):
                    warnings.warn("Error creating array on server", RuntimeWarning)
                self.command_buffer = ASTNode(self.name, 0, [self])
        else:
            self.name = name
            max_depth = get_max_depth()
            if self.command_buffer.depth > max_depth:
                if is_debug():
                    print("Maximum AST depth exceeded for %i, "
                          "flushing buffer" % self.name)
                self._flush_command_buffer()

    @property
    def shape(self):
        self._flush_command_buffer()
        return self._shape

    def __del__(self):
        if self.valid:
            cmd = to_bytes(self.name, 'L')
            send_command_async(Handlers.delete_handler, cmd)

    def __len__(self):
        return self.shape[0]

    def __str__(self):
        print(self.get())

    #def __repr__(self):
    #    #self._flush_command_buffer()
    #    # FIXME add repr
    #    pass

    def __setitem__(self, key, value):
        if not isinstance(key, slice) or key.start != None or \
                key.stop != None or key.step != None:
            raise ValueError("Can't set items or slices")
        self.cmd_buffer = ASTNode(res, OPCODES.get('setitem'), [self, value])

    def __neg__(self):
        return self * -1

    def __add__(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('+'), [self, other])
        return create_ndarray(self.ndim, self.dtype,
                              name=res, command_buffer=cmd_buffer)

    def __radd__(self, other):
        return self + other

    def __sub__(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('-'), [self, other])
        return create_ndarray(self.ndim, self.dtype,
                              name=res, command_buffer=cmd_buffer)

    def __rsub__(self, other):
        return -1 * (self - other)

    def __mul__(self, other):
        if self.ndim > 0 or (isinstance(other, ndarray) and other.ndim > 0):
            RuntimeError("Cannote multiply two arrays")
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('*'), [self, other])
        return create_ndarray(self.ndim, self.dtype,
                              name=res, command_buffer=cmd_buffer)

    def __rmul__(self, other):
        return self * other

    def __truediv__(self, other):
        if self.ndim > 0 or other.ndim > 0:
            RuntimeError("Cannote divide two arrays")
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('/'), [self, other])
        return create_ndarray(self.ndim, self.dtype,
                              name=res, command_buffer=cmd_buffer)

    def __matmul__(self, other):
        if self.ndim == 2 and other.ndim == 2:
            res_ndim = 2
        elif self.ndim == 2 and other.ndim == 1:
            res_ndim = 1
        elif self.ndim == 1 and other.ndim == 1:
            res_ndim = 0
        else:
            raise RuntimeError("Dimension mismatch")
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('@'), [self, other])
        return create_ndarray(res_ndim, self.dtype,
                              name=res, command_buffer=cmd_buffer)

    def _flush_command_buffer(self):
        # send the command to server
        # finally set command buffer to array name
        debug = is_debug()
        if debug:
            self.command_buffer.plot_graph()
        if self.valid:
            return
        validated_arrays = {self.name : self}
        cmd = self.command_buffer.get_command(validated_arrays)
        reply_size = 0
        for name, arr in validated_arrays.items():
            reply_size += 8 + 8 * arr.ndim
        if not debug:
            metadata = send_command_raw(Handlers.operation_handler,
                                        cmd, reply_size=reply_size)
            # traverse metadata
            offset = 0
            for i in range(len(validated_arrays)):
                name = from_bytes(metadata[offset : offset + 8], 'L')
                offset += 8
                arr = validated_arrays[name]
                for d in range(arr.ndim):
                    arr._shape[d] = from_bytes(metadata[offset : offset + 8], 'L')
                    offset += 8
                arr.validate()
        else:
            for name, arr in validated_arrays.items():
                arr.validate()
        self.validate()

    def get(self):
        self._flush_command_buffer()
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

    def evaluate(self):
        self._flush_command_buffer()

    def validate(self):
        self.valid = True
        self.command_buffer = ASTNode(self.name, 0, [self])

    def copy(self):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('copy'), [self])
        return create_ndarray(self.ndim, self.dtype,
                              name=res, command_buffer=cmd_buffer)

