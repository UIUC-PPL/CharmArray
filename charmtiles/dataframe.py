import sys
import warnings
import numpy as np
import weakref
import sys
import pandas as pd
from charmtiles.ccs import to_bytes, from_bytes, send_command_raw, send_command, \
    send_command, get_creation_command, \
    get_epoch, get_name, get_fetch_command, Handlers, OPCODES, is_debug


deletion_buffer = b''
deletion_buffer_size = 0


class nddataframe:
    def __init__(self, data: pd.DataFrame):
        """
        This is the wrapper class for AUM array objects.
        The argument 'name' should be None except when wrapping
        an array that already exists on the AUM backend server
        """

        self.data = data
        self.name = get_name()
        self.col_map = dict()
        self.shape = data.shape
        buf = b""
        buf += to_bytes(self.name, "L")
        buf += to_bytes(data.shape[0], "L") 
        buf += to_bytes(data.shape[1], "L")
        for col in data.columns:
            self.col_map[col] = get_name()
            buf += to_bytes(self.col_map[col], "L")
            for row in data[col]:
                buf += to_bytes(row, "L")
        cmd = to_bytes(get_epoch(), 'i') + to_bytes(len(buf), 'I') + buf
        send_command(Handlers.creation_handler, cmd)

    def _flush_command_buffer(self):
        # send the command to server
        # finally set command buffer to array name
        global deletion_buffer, deletion_buffer_size
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
            print("Deletion size :", deletion_buffer_size)
            cmd = to_bytes(deletion_buffer_size, 'I') + deletion_buffer + cmd
            cmd = to_bytes(get_epoch(), 'i') + to_bytes(len(cmd), 'I') + cmd
            send_command_async(Handlers.operation_handler, cmd)
            deletion_buffer = b''
            deletion_buffer_size = 0
            for i in range(len(validated_arrays)):
                arr = validated_arrays[name]
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

