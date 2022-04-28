import sys
import struct
import numpy as np
from pyccs import Server
from pyproject.ccs import send_command, get_operation_command, Handlers
from pyproject.array import create_ndarray


def axpy(a, x, y, multiplier=None):
    operands = [a, x, y]
    if multiplier is not None:
        operands.append(multiplier)
        operation = 'axpy_multiplier'
    else:
        operation = 'axpy'
    cmd = get_operation_command(operation, operands)
    res = send_command(Handlers.operation_handler, cmd)
    return create_ndarray(res, x.ndim, x.shape, x.dtype)

