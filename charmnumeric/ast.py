import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
from ctypes import c_long
from networkx.drawing.nx_pydot import graphviz_layout
from charmnumeric.ccs import OPCODES, INV_OPCODES, to_bytes


max_depth = 10


def set_max_depth(d):
    global max_depth
    max_depth = d


def get_max_depth():
    global max_depth
    return max_depth


class ASTNode(object):
    def __init__(self, name, opcode, operands, args=[]):
        from charmtiles.array import ndarray
        # contains opcode, operands
        # operands are ndarrays
        self.name = name
        self.opcode = opcode
        self.operands = operands
        self.depth = 0
        self.args = args
        if self.opcode != 0:
            for op in self.operands:
                if isinstance(op, ndarray):
                    self.depth = max(self.depth, 1 + op.command_buffer.depth)

    ###############################################################################################################################################
    # Marker determines whether we are dealing with a tensor, a scalar or an arithmetic type                                                      #
    # Marker = 0 : arithmetic type                                                                                                                #
    # Marker = 1 : scalar     type                                                                                                                #
    # Marker = 2 : tensor     type                                                                                                                #
    # Encoding = | Marker | dim | shape | opcode | save_op | ID | NumArgs | Args | NumOperands | OperandEncodingSize | RecursiveOperandEncoding | #
    #            |   8    |  8  |  64   |   32   |   1     | 64 |   32    |  64  |     8       |         32          | ........................ | #
    # NB: If opcode is 0, the encoding is limited to ID                                                                                           #
    # Encoding = | Marker | shape |  val  |                                                                                                       #
    #            |   8    |  64   |  64   |                                                                                                       #
    # NB: Latter encoding for double constants                                                                                                    #
    ###############################################################################################################################################
    def get_command(self, validated_arrays, ndim, shape, save=True, is_scalar=False):
        from charmnumeric.array import ndarray

        # Ndims and Shape setup
        if is_scalar:
            cmd = to_bytes(1, 'B')
        else:
            cmd = to_bytes(2, 'B')
        cmd += to_bytes(ndim, 'B')
        for _shape in shape:
            cmd += to_bytes(_shape, 'L')

        if self.opcode == 0:
            cmd += to_bytes(0, 'I') + to_bytes(False, '?') + to_bytes(self.operands[0].name, 'L')
            return cmd

        cmd += to_bytes(self.opcode, 'I') + to_bytes(save, '?') + to_bytes(self.name, 'L')
        cmd += to_bytes(len(self.args), 'I')
        for arg in self.args:
            cmd += to_bytes(arg, 'd')

        cmd += to_bytes(len(self.operands), 'B')
        for op in self.operands:
            if isinstance(op, ndarray):
                if op.name in validated_arrays:
                    if op.is_scalar:
                        opcmd = to_bytes(1, 'B')
                    else:
                        opcmd = to_bytes(2, 'B')
                    opcmd += to_bytes(op.ndim, 'B')
                    for _shape in op.shape:
                        opcmd += to_bytes(_shape, 'L')
                    opcmd += to_bytes(0, 'I') + to_bytes(False, '?') + to_bytes(op.name, 'L')
                else:
                    save_op = True if c_long.from_address(id(op)).value - 2 > 0 else False
                    opcmd = op.command_buffer.get_command(validated_arrays, op.ndim, op.shape, save=save_op, is_scalar=op.is_scalar)
                    if not op.valid and save_op:
                        validated_arrays[op.name] = op
            elif isinstance(op, float) or isinstance(op, int):
                opcmd = to_bytes(0, 'B')
                for _shape in shape:
                    opcmd += to_bytes(_shape, 'L')
                opcmd += to_bytes(float(op), 'd')
            cmd += to_bytes(len(opcmd), 'I')
            cmd += opcmd
        return cmd

    def plot_graph(self, validated_arrays={}, G=None, node_map={},
                   color_map={}, next_id=0, parent=None, save=True):
        from charmnumeric.array import ndarray
        if G is None:
            G = nx.Graph()
        if self.opcode == 0:
            node_map[next_id] = 'a' + str(self.operands[0].name)
            G.add_node(next_id)
            if parent is not None:
                G.add_edge(parent, next_id)
            return next_id + 1
        opnode = next_id
        G.add_node(next_id)
        if parent is not None:
            G.add_edge(parent, next_id)
        node_map[next_id] = INV_OPCODES.get(self.opcode, '?')
        if save:
            color_map[next_id] = 'tab:red'
            node_map[next_id] += (': a%i' % self.name)
        next_id += 1
        for op in self.operands:
            # an operand can also be a double
            if isinstance(op, ndarray):
                if op.name in validated_arrays:
                    G.add_node(next_id)
                    G.add_edge(opnode, next_id)
                    node_map[next_id] = 'a' + str(op.name)
                    color_map[next_id] = 'tab:green'
                    next_id += 1
                else:
                    save_op = True if c_long.from_address(id(op)).value - 2 > 0 else False
                    if not op.valid and save_op:
                        #color_map[next_id] = 'tab:red'
                        validated_arrays[op.name] = op
                    next_id = op.command_buffer.plot_graph(
                        validated_arrays, G, node_map, color_map, next_id,
                        opnode, save_op)
            elif isinstance(op, float) or isinstance(op, int):
                G.add_node(next_id)
                G.add_edge(opnode, next_id)
                node_map[next_id] = op
                next_id += 1
        if parent is None:
            pos = graphviz_layout(G, prog='dot')
            color_map_list = [color_map.get(node, 'tab:blue') for node in G]
            nx.draw(G, pos, labels=node_map, node_color=color_map_list,
                    node_size=600, font_size=10)
            plt.show()
        return next_id

