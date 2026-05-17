import sys
import struct
import numpy as np
from pyccs import Server
from charmnumeric.ccs import OPCODES, get_name, send_command, Handlers
from charmnumeric.array import create_ndarray
from charmnumeric.ast import ASTNode


def axpy(a, x, y):
    res = get_name()
    cmd_buffer = ASTNode(res, OPCODES.get('axpy'), [x, y], args=[a])
    return create_ndarray(x.ndim, x.dtype, x.shape,
                          name=res, command_buffer=cmd_buffer)
