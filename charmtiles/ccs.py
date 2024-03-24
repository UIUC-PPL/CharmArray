import struct
import atexit
from pyccs import Server

debug = False
server = None
client_id = 0
next_name = 0
epoch = 0

OPCODES = {'+': 1, '-': 2, '*': 3 ,'/': 4, '@': 5, 'copy': 6, 'axpy': 7,
           'axpy_multiplier': 8, 'setitem': 9}

INV_OPCODES = {v: k for k, v in OPCODES.items()}

def enable_debug():
    global debug
    debug = True

def disable_debug():
    global debug
    debug = False

def is_debug():
    global debug
    return debug

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
    if server is not None:
        server.send_request(handler, 0, msg)
        return server.receive_response(reply_size)

def send_command(handler, msg, reply_size=1, reply_type='B'):
    global server
    if server is not None:
        return from_bytes(send_command_raw(handler, msg, reply_size), reply_type)

def send_command_async(handler, msg):
    global server
    if server is not None:
        server.send_request(handler, 0, msg)

def get_epoch():
    global epoch
    curr_epoch, epoch = epoch, epoch + 1
    return curr_epoch

def connect(server_ip, server_port):
    global server, client_id, debug
    if not debug:
        server = Server(server_ip, server_port)
        server.connect()
        client_id = send_command(Handlers.connection_handler, "")
        atexit.register(disconnect)

def disconnect():
    from charmtiles.array import deletion_buffer, deletion_buffer_size
    global client_id, deletion_buffer, deletion_buffer_size
    if deletion_buffer_size > 0:
        cmd = to_bytes(len(deletion_buffer), 'I') + deletion_buffer
        cmd = to_bytes(get_epoch(), 'i') + to_bytes(cmd, 'I') + cmd
        send_command_async(Handlers.delete_handler, cmd)
        deletion_buffer = b''
        deletion_buffer_size = b''
    cmd = to_bytes(client_id, 'B')
    cmd = to_bytes(get_epoch(), 'i') + to_bytes(len(cmd), 'I') + cmd
    send_command_async(Handlers.disconnection_handler, cmd)

def get_creation_command(arr, name, shape, buf=None):
    """
    Generate array creation CCS command
    """
    cmd = to_bytes(name, 'L')
    cmd += to_bytes(arr.ndim, 'I')
    cmd += to_bytes(buf is not None, '?')
    cmd += to_bytes(arr.init_value is not None, '?')
    for s in shape:
        cmd += to_bytes(int(s), 'L')
    if buf is not None:
        cmd += buf
    elif arr.init_value is not None:
        cmd += to_bytes(arr.init_value, 'd')
    print(cmd)
    cmd = to_bytes(get_epoch(), 'i') + to_bytes(len(cmd), 'I') + cmd
    return cmd

def get_fetch_command(arr):
    """
    Generate CCS command to fetch entire array data
    """
    cmd = to_bytes(arr.name, 'L')
    cmd = to_bytes(get_epoch(), 'i') + to_bytes(len(cmd), 'I') + cmd
    return cmd

def sync():
    # FIXME remove the size from the cmd
    cmd = to_bytes(get_epoch(), 'i') + to_bytes(0, 'I')
    send_command_raw(Handlers.sync_handler, cmd, 1)

class Handlers(object):
    connection_handler = b'aum_connect'
    disconnection_handler = b'aum_disconnect'
    creation_handler = b'aum_creation'
    operation_handler = b'aum_operation'
    fetch_handler = b'aum_fetch'
    delete_handler = b'aum_delete'
    sync_handler = b'aum_sync'
    exit_handler = b'aum_exit'
    pd_creation_handler = b'pd_creation'
    pd_operation_handler = b'pd_operation'
    pd_fetch_handler = b'pd_fetch'
    pd_delete_handler = b'pd_delete'
    sync_handler = b'aum_sync'
    exit_handler = b'aum_exit'
