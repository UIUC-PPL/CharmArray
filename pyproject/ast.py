import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
from ctypes import c_long
from networkx.drawing.nx_pydot import graphviz_layout
from pyproject.ccs import OPCODES, INV_OPCODES, to_bytes


class ASTNode(object):
    def __init__(self, name, opcode, operands):
        # contains opcode, operands
        # operands are ndarrays
        self.name = name
        self.opcode = opcode
        self.operands = operands

    def get_command(self, validated_arrays, save=True):
        from pyproject.array import ndarray
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
                cmd += to_bytes(len(opcmd), 'I')
                cmd += opcmd
        return cmd

    def plot_graph(self, G=None, node_map={}, color_map={}, next_id=0, parent=None):
        from pyproject.array import ndarray
        if G is None:
            G = nx.Graph()
        if self.opcode == 0:
            node_map[next_id] = 'a' + str(self.operands[0].name)
            G.add_node(next_id)
            if parent is not None:
                G.add_edge(parent, next_id)
            next_id += 1
            if not self.operands[0].valid:
                next_id = self.operands[0].command_buffer.plot_graph(
                    G, node_map, color_map, next_id, next_id - 1)
            return next_id
        opnode = next_id
        G.add_node(next_id)
        if parent is not None:
            G.add_edge(parent, next_id)
        node_map[next_id] = INV_OPCODES.get(self.opcode, '?')
        if parent is None:
            color_map[next_id] = 1
        next_id += 1
        for op in self.operands:
            # an operand can also be a double
            if isinstance(op, ndarray):
                save_op = True if c_long.from_address(id(op)).value - 2 > 0 else False
                if not op.valid and save_op:
                    color_map[next_id] = 1
                next_id = op.command_buffer.plot_graph(
                    G, node_map, color_map, next_id, opnode)
            elif isinstance(op, float) or isinstance(op, int):
                G.add_node(next_id)
                G.add_edge(opnode, next_id)
                node_map[next_id] = op
                next_id += 1
        if parent is None:
            pos = graphviz_layout(G, prog="dot")
            color_map_list = []
            for node in G:
                if node in color_map:
                    color_map_list.append('tab:red')
                else:
                    color_map_list.append('tab:blue')
            nx.draw(G, pos, labels=node_map, node_color=color_map_list)
            plt.show()
        return next_id

