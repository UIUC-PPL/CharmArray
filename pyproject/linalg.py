import sys
import struct
import numpy as np
from pyccs import Server
from pyproject.ccs import OPCODES, get_name, send_command, Handlers
from pyproject.array import create_ndarray
from pyproject.ast import ASTNode


def axpy(a, x, y, multiplier=None):
    operands = [a, x, y]
    if multiplier is not None:
        operands.append(multiplier)
        operation = 'axpy_multiplier'
    else:
        operation = 'axpy'
    res = get_name()
    cmd_buffer = ASTNode(res, OPCODES.get(operation), operands)
    return create_ndarray(x.ndim, x.dtype,
                          name=res, command_buffer=cmd_buffer)


