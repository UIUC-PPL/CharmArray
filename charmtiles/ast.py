import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
from ctypes import c_long
from networkx.drawing.nx_pydot import graphviz_layout
from charmtiles.ccs import OPCODES, INV_OPCODES, to_bytes


max_depth = 10


def set_max_depth(d):
    global max_depth
    max_depth = d


def get_max_depth():
    global max_depth
    return max_depth


class ASTNode(object):
    def __init__(self, name, opcode, operands):
        from charmtiles.array import ndarray
        # contains opcode, operands
        # operands are ndarrays
        self.name = name
        self.opcode = opcode
        self.operands = operands
        self.depth = 0
        if self.opcode != 0:
            for op in self.operands:
                if isinstance(op, ndarray):
                    self.depth = max(self.depth, 1 + op.command_buffer.depth)

    def get_command(self, validated_arrays, save=True):
        from charmtiles.array import ndarray
        if self.opcode == 0:
            cmd = to_bytes(self.opcode, 'L')
            cmd += to_bytes(False, '?')
            cmd += to_bytes(self.operands[0].name, 'L')
            return cmd
        cmd = to_bytes(self.opcode, 'L') + to_bytes(self.name, 'L')
        cmd += to_bytes(save, '?') + to_bytes(len(self.operands), 'B')
        for op in self.operands:
            # an operand can also be a double
            if isinstance(op, ndarray):
                if op.name in validated_arrays:
                    opcmd = to_bytes(0, 'L')
                    opcmd += to_bytes(False, '?')
                    opcmd += to_bytes(op.name, 'L')
                    cmd += to_bytes(len(opcmd), 'I')
                    cmd += opcmd
                else:
                    save_op = True if c_long.from_address(id(op)).value - 2 > 0 else False
                    opcmd = op.command_buffer.get_command(validated_arrays,
                                                          save=save_op)
                    if not op.valid and save_op:
                        validated_arrays[op.name] = op
                    cmd += to_bytes(len(opcmd), 'I')
                    cmd += opcmd
            elif isinstance(op, float) or isinstance(op, int):
                opcmd = to_bytes(0, 'L')
                opcmd += to_bytes(True, '?')
                opcmd += to_bytes(op, 'd')
                #print(opcmd, len(opcmd), len(cmd))
                cmd += to_bytes(len(opcmd), 'I')
                cmd += opcmd
        return cmd

    def plot_graph(self, validated_arrays={}, G=None, node_map={},
                   color_map={}, next_id=0, parent=None, save=True):
        from charmtiles.array import ndarray
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

