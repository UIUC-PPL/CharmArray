import struct
from pyccs import Server
from pyproject import array

server = None

OPCODES = {'+': 1, '-': 2, '*': 3 ,'/': 4, '@': 5, 'copy': 6, 'axpy': 7,
           'axpy_multiplier': 8, '*s': 9, '/s': 10}

def to_bytes(value, dtype='i'):
    return struct.pack(dtype, value)

def from_bytes(bvalue, dtype='i'):
    return struct.unpack(dtype, bvalue)[0]

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

def get_creation_command(arr):
    """
    Generate array creation CCS command
    """
    cmd = to_bytes(arr.ndim, 'i')
    cmd += to_bytes(arr.init_value is not None, '?')
    for s in arr.shape:
        cmd += to_bytes(int(s), 'l')
    if arr.init_value is not None:
        cmd += to_bytes(arr.init_value, 'd')
    return cmd

def get_fetch_command(arr):
    """
    Generate CCS command to fetch entire array data
    """
    cmd = to_bytes(arr.name, 'l')
    return cmd

def get_operation_command(operation, operands):
    """
    Generate CCS command for operation
    """
    if operation not in OPCODES:
        raise NotImplementedError("Operation %s not supported"
                                  % operation)
    opcode = OPCODES.get(operation)
    cmd = to_bytes(opcode, 'i')
    for x in operands:
        if isinstance(x, array.ndarray):
            cmd += to_bytes(x.name, 'l')
        elif isinstance(x, float) or isinstance(x, int):
            cmd += to_bytes(x, 'd')
    return cmd

class Handlers(object):
    creation_handler = b'aum_creation'
    operation_handler = b'aum_operation'
    fetch_handler = b'aum_fetch'
    delete_handler = b'aum_delete'
    exit_handler = b'aum_exit'

