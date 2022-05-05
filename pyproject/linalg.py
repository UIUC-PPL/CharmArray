import sys
import struct
import numpy as np
from pyccs import Server
from pyproject.ccs import get_name, send_command, get_operation_command, Handlers
from pyproject.array import create_ndarray


def axpy(a, x, y, multiplier=None):
    operands = [a, x, y]
    if multiplier is not None:
        operands.append(multiplier)
        operation = 'axpy_multiplier'
    else:
        operation = 'axpy'
    res = get_name()
    cmd = get_operation_command(operation, res, operands)
    send_command(Handlers.operation_handler, cmd)
    return create_ndarray(res, x.ndim, x.shape, x.dtype)

