from charmnumeric.charmnumeric import create_array
from charmtyles.core import plot_execution_state, execute
from charmtyles.interface import CCSInterface
import numpy as np
import sys

N = 64
x = create_array((N,), dtype=np.float32)
x[:] = 1
res = x @ x
x = x / res

interface = CCSInterface()
interface.connect('192.168.1.115', 1234, 4)
print(x.get(interface))
#execute(interface)
