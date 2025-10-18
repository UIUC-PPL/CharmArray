import sys
import warnings
import numpy as np
import weakref
import sys
from charmnumeric.ast import get_max_depth, ASTNode
from charmnumeric.ccs import to_bytes, from_bytes, send_command_raw, send_command, \
    send_command_async, connect, get_creation_command, \
    get_epoch, get_name, get_fetch_command, Handlers, OPCODES, is_debug


deletion_buffer = b''
deletion_buffer_size = 0
doDeferredDeletions = False
deferred_deletion_buffer = b''
deferred_deletion_buffer_size = 0


def create_ndarray(ndim, dtype, shape=None, name=None, command_buffer=None, is_scalar=False):
    z = ndarray(ndim, dtype=dtype, shape=shape, name=name,
                   command_buffer=command_buffer, is_scalar=is_scalar)
    return z


def from_numpy(nparr):
    return ndarray(nparr.ndim, dtype=nparr.dtype, shape=nparr.shape,
                   nparr=nparr)

def isScalarResult(a, b):
    return a.is_scalar and b.is_scalar

def getDimShape(a, b):
    if isinstance(b, float) or isinstance(b, int):
        return [a.ndim, a.shape.copy()]
    elif isinstance(a, float) or isinstance(a, int):
        return [b.ndim, b.shape.copy()]
    elif a.is_scalar:
        return [b.ndim, b.shape.copy()]
    else:
        return [a.ndim, a.shape.copy()]

class ndarray:
    def __init__(self, ndim, shape=None, dtype=np.float64, init_value=None,
                 nparr=None, name=None, command_buffer=None, is_scalar=False):
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
        self.is_scalar = is_scalar
        if isinstance(shape, np.ndarray) or isinstance(shape, list) or \
                isinstance(shape, tuple):
            self.shape = np.asarray(shape, dtype=np.int32)
        elif shape is not None:
            self.shape = np.asarray([shape], dtype=np.int32)
        else:
            self.shape = np.zeros(self.ndim, dtype=np.int32)
        self.valid = False
        if command_buffer is None:
            self.valid = True
            if name:
                self.name = name
                #self.command_buffer = None
                self.command_buffer = ASTNode(self.name, 0, [weakref.proxy(self)])
            else:
                self.name = get_name()
                if nparr is not None:
                    buf = nparr.tobytes()
                else:
                    buf = None
                cmd = get_creation_command(self, self.name, self.shape, buf=buf)
                send_command_async(Handlers.creation_handler, cmd)
                #self.command_buffer = None
                self.command_buffer = ASTNode(self.name, 0, [weakref.proxy(self)])
        else:
            self.name = name
            max_depth = get_max_depth()
            if self.command_buffer.depth >= max_depth:
                if is_debug():
                    print("Maximum AST depth exceeded for %i, "
                          "flushing buffer" % self.name)
                self._flush_command_buffer(hasExceededMaxAstDepth=True)

    def __del__(self):
        global doDeferredDeletions
        if doDeferredDeletions:
            global deferred_deletion_buffer, deferred_deletion_buffer_size
            if self.valid:
                deferred_deletion_buffer += to_bytes(self.name, 'L')
                deferred_deletion_buffer_size += 1
        else:
            global deletion_buffer, deletion_buffer_size
            if self.valid:
                deletion_buffer += to_bytes(self.name, 'L')
                deletion_buffer_size += 1

    def __len__(self):
        return self.shape[0]

    def __neg__(self):
        return self * -1

    def __add__(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('+'), [self, other])
        ndim, shape = getDimShape(self, other)
        return create_ndarray(ndim, self.dtype, shape=shape,
                              name=res, command_buffer=cmd_buffer, is_scalar=isScalarResult(self, other))

    def __radd__(self, other):
        return self + other

    def __sub__(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('-'), [self, other])
        ndim, shape = getDimShape(self, other)
        return create_ndarray(ndim, self.dtype, shape=shape,
                              name=res, command_buffer=cmd_buffer, is_scalar=isScalarResult(self, other))


    def __rsub__(self, other):
        return -1 * (self - other)
    
    def __lt__(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('<'), [self, other])
        ndim, shape = getDimShape(self, other)
        return create_ndarray(ndim, self.dtype, shape=shape,
                              name=res, command_buffer=cmd_buffer, is_scalar=isScalarResult(self, other))

    
    def __rlt__(self, other):
        return self >= other
    
    def __gt__(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('>'), [self, other])
        ndim, shape = getDimShape(self, other)
        return create_ndarray(ndim, self.dtype, shape=shape,
                              name=res, command_buffer=cmd_buffer, is_scalar=isScalarResult(self, other))

    
    def __rgt__(self, other):
        return self <= other
    
    def __le__(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('<='), [self, other])
        ndim, shape = getDimShape(self, other)
        return create_ndarray(ndim, self.dtype, shape=shape,
                              name=res, command_buffer=cmd_buffer, is_scalar=isScalarResult(self, other))

    
    def __rle__(self, other):
        return self > other
    
    def __ge__(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('>='), [self, other])
        ndim, shape = getDimShape(self, other)
        return create_ndarray(ndim, self.dtype, shape=shape,
                              name=res, command_buffer=cmd_buffer, is_scalar=isScalarResult(self, other))

    
    def __rge__(self, other):
        return self < other
    
    def __eq__(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('=='), [self, other])
        ndim, shape = getDimShape(self, other)
        return create_ndarray(ndim, self.dtype, shape=shape,
                              name=res, command_buffer=cmd_buffer, is_scalar=isScalarResult(self, other))

    
    def __req__(self, other):
        return self == other
    
    def __ne__(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('!='), [self, other])
        ndim, shape = getDimShape(self, other)
        return create_ndarray(ndim, self.dtype, shape=shape,
                              name=res, command_buffer=cmd_buffer, is_scalar=isScalarResult(self, other))

    
    def __rne__(self, other):
        return self != other
    
    def __and__(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('&'), [self, other])
        ndim, shape = getDimShape(self, other)
        return create_ndarray(ndim, self.dtype, shape=shape,
                              name=res, command_buffer=cmd_buffer, is_scalar=isScalarResult(self, other))

    
    def __rand__(self, other):
        return self & other
    
    def __or__(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('|'), [self, other])
        ndim, shape = getDimShape(self, other)
        return create_ndarray(ndim, self.dtype, shape=shape,
                              name=res, command_buffer=cmd_buffer, is_scalar=isScalarResult(self, other))

    
    def __ror__(self, other):
        return self | other
    
    def __invert__(self):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('!'), [self])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer, is_scalar=self.is_scalar)

    def __mul__(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('*'), [self, other])
        ndim, shape = getDimShape(self, other)
        return create_ndarray(ndim, self.dtype, shape=shape,
                              name=res, command_buffer=cmd_buffer, is_scalar=isScalarResult(self, other))


    def __rmul__(self, other):
        return self * other

    def __truediv__(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('/'), [self, other])
        ndim, shape = getDimShape(self, other)
        return create_ndarray(ndim, self.dtype, shape=shape,
                              name=res, command_buffer=cmd_buffer, is_scalar=isScalarResult(self, other))

    
    def __rtruediv__(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('/'), [1., self/other])
        ndim, shape = getDimShape(self, other)
        return create_ndarray(ndim, self.dtype, shape=shape,
                              name=res, command_buffer=cmd_buffer, is_scalar=isScalarResult(self, other))


    def __matmul__(self, other):
        is_scalar = False
        if self.ndim == 2 and other.ndim == 2:
            res_ndim = 2
            shape = np.array([self.shape[0], other.shape[1]], dtype=np.int32)
        elif self.ndim == 2 and other.ndim == 1:
            res_ndim = 1
            shape = np.array([self.shape[0]], dtype=np.int32)
        elif self.ndim == 1 and other.ndim == 1:
            res_ndim = 1
            shape = np.array([1], dtype=np.int32)
            is_scalar = True
        else:
            raise RuntimeError("Dimension mismatch")
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('@'), [self, other])
        return create_ndarray(res_ndim, self.dtype, shape=shape,
                              name=res, command_buffer=cmd_buffer, is_scalar=is_scalar)

    def _flush_command_buffer(self, hasExceededMaxAstDepth=False):
        # send the command to server
        # finally set command buffer to array name
        global deletion_buffer, deletion_buffer_size, deferred_deletion_buffer, deferred_deletion_buffer_size
        debug = is_debug()
        if debug:
            self.command_buffer.plot_graph()
        if self.valid:
            return
        cmd = self.command_buffer.get_command(self.ndim, self.shape, is_scalar=self.is_scalar, hasExceededMaxAstDepth=hasExceededMaxAstDepth)
        if not debug:
            cmd = to_bytes(deletion_buffer_size, 'I') + deletion_buffer + to_bytes(deferred_deletion_buffer_size, 'I') + deferred_deletion_buffer + cmd
            cmd = to_bytes(get_epoch(), 'i') + to_bytes(len(cmd), 'I') + cmd
            send_command_async(Handlers.operation_handler, cmd)
            deletion_buffer = b''
            deletion_buffer_size = 0
        self.validate()

    def get(self):
        self._flush_command_buffer()
        cmd = get_fetch_command(self)
        if self.ndim == 0:
            total_size = self.itemsize
            data_bytes = send_command_raw(Handlers.fetch_handler, cmd, reply_size=total_size)
            return from_bytes(data_bytes, np.dtype(self.dtype).char)
        else:
            total_size = self.itemsize
            for i in self.shape:
                total_size*=i
            data_ptr = send_command_raw(Handlers.fetch_handler, cmd, reply_size=int(total_size))
            return np.frombuffer(data_ptr, np.dtype(self.dtype)).copy().reshape(self.shape)

    def evaluate(self):
        self._flush_command_buffer()

    def validate(self):
        global doDeferredDeletions
        self.valid = True
        doDeferredDeletions = True
        self.command_buffer = ASTNode(self.name, 0, [weakref.proxy(self)])
        doDeferredDeletions = False

    def copy(self):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('copy'), [self])
        return create_ndarray(self.ndim, self.dtype,shape=self.shape.copy(), name=res, command_buffer=cmd_buffer, is_scalar=self.is_scalar)

    def where(self, other, third):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('where'), [other, third, self])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer, is_scalar=self.is_scalar)
    
    def exp(self):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('exp'), [self])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)
    
    def log(self, base = np.e):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('log'), [self], args=[base])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)
    
    def log10(self):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('log'), [self], args = [10])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)
    
    def log2(self):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('log'), [self], args = [2])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)
    
    def abs(self):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('abs'), [self])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)

    def negate(self):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('negate'), [self])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)
    
    def square(self):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('sqare'), [self])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)
    
    def sqrt(self):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('sqrt'), [self])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)
    
    def reciprocal(self):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('reciprocal'), [self])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)

    def sin(self):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('sin'), [self])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)

    def cos(self):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('cos'), [self])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)

    def relu(self):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('relu'), [self])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)
    
    def scale(self, scalar):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('scale'), [self], args=[scalar])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)
    
    def add_constant(self, constant):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('add_constant'), [self], args=[constant])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)

    def add(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('add'), [self, other])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)
    
    def subtract(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('subtract'), [self, other])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)

    def multiply(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('multiply'), [self, other])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)
    
    def divide(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('divide'), [self, other])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)

    def modulo(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('modulo'), [self, other])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)

    def power(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('power'), [self, other])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)

    def max(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('max'), [self, other])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)

    def min(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('min'), [self, other])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)

    def greater_than(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('greater_than'), [self, other])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)
    
    def less_than(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('less_than'), [self, other])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)
    
    def equal(self, other, epsilon=1e-5):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('equal'), [self, other], args=[epsilon])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)
    
    def atan2(self, other):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('atan2'), [self, other])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)

    def weighted_average(self, other, w1, w2):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('weighted_average'), [self, other],
                             args=[w1, w2])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)

    def any(self):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('any'), [self])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)
    def all(self):
        res = get_name()
        cmd_buffer = ASTNode(res, OPCODES.get('all'), [self])
        return create_ndarray(self.ndim, self.dtype, shape=self.shape.copy(),
                              name=res, command_buffer=cmd_buffer)

