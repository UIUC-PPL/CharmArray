import struct
import atexit
from pyccs import Server
from pyproject import array

server = None
client_id = None
next_name = 0
epoch = 0

OPCODES = {'+': 1, '-': 2, '*': 3 ,'/': 4, '@': 5, 'copy': 6, 'axpy': 7,
           'axpy_multiplier': 8, '*s': 9, '/s': 10}

def get_epoch():
    global epoch
    curr_epoch = epoch
    epoch += 1
    return curr_epoch

def get_name():
    global next_name
    curr_name = next_name
    next_name += 1
    return (client_id << 56) + curr_name

def to_bytes(value, dtype='I'):
    return struct.pack(dtype, value)

def from_bytes(bvalue, dtype='I'):
    return struct.unpack(dtype, bvalue)[0]

def send_command_raw(handler, msg, reply_size):
    server.send_request(handler, 0, msg)
    return server.receive_response(reply_size)

def send_command(handler, msg, reply_size=1, reply_type='B'):
    return from_bytes(send_command_raw(handler, msg, reply_size), reply_type)

def send_command_async(handler, msg):
    server.send_request(handler, 0, msg)

def connect(server_ip, server_port):
    global server, client_id
    server = Server(server_ip, server_port)
    server.connect()
    client_id = send_command(Handlers.connection_handler, "")
    atexit.register(disconnect)

def disconnect():
    global client_id
    cmd = to_bytes(client_id, 'B')
    send_command_async(Handlers.disconnection_handler, cmd)

def get_creation_command(arr, name):
    """
    Generate array creation CCS command
    """
    global client_id
    msg_size = 26 + arr.ndim * 8 + (8 if arr.init_value else 0)
    cmd = to_bytes(client_id, 'B')
    cmd += to_bytes(get_epoch(), 'L')
    cmd += to_bytes(msg_size, 'I')
    cmd += to_bytes(name, 'L')
    cmd += to_bytes(arr.ndim, 'I')
    cmd += to_bytes(arr.init_value is not None, '?')
    for s in arr.shape:
        cmd += to_bytes(int(s), 'L')
    if arr.init_value is not None:
        cmd += to_bytes(arr.init_value, 'd')
    return cmd

def get_fetch_command(arr):
    """
    Generate CCS command to fetch entire array data
    """
    global client_id
    msg_size = 21
    cmd = to_bytes(client_id, 'B')
    cmd += to_bytes(get_epoch(), 'L')
    cmd += to_bytes(msg_size, 'I')
    cmd += to_bytes(arr.name, 'L')
    return cmd

def get_operation_command(operation, name, operands):
    """
    Generate CCS command for operation
    """
    global client_id
    if operation not in OPCODES:
        raise NotImplementedError("Operation %s not supported"
                                  % operation)
    msg_size = 25 + 8 * len(operands)
    opcode = OPCODES.get(operation)
    cmd = to_bytes(client_id, 'B')
    cmd += to_bytes(get_epoch(), 'L')
    cmd += to_bytes(msg_size, 'I')
    cmd += to_bytes(name, 'L')
    cmd += to_bytes(opcode, 'I')
    for x in operands:
        if isinstance(x, array.ndarray):
            cmd += to_bytes(x.name, 'L')
        elif isinstance(x, float) or isinstance(x, int):
            cmd += to_bytes(x, 'd')
    return cmd

class Handlers(object):
    connection_handler = b'aum_connect'
    disconnection_handler = b'aum_disconnect'
    creation_handler = b'aum_creation'
    operation_handler = b'aum_operation'
    fetch_handler = b'aum_fetch'
    delete_handler = b'aum_delete'
    exit_handler = b'aum_exit'

