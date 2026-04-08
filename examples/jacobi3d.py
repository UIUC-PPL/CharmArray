from charmnumeric.charmnumeric import create_array
from charmtyles.core import plot_execution_state, execute
from charmtyles.interface import CCSInterface
import numpy as np

u = create_array((128, 128, 128), dtype=np.float32)

u[0, :, :] = 1.0
u[-1, :, :] = 1.0
u[:, 0, :] = 1.0
u[:, -1, :] = 1.0
u[:, :, 0] = 1.0
u[:, :, -1] = 1.0

for it in range(100):
    u[1:-1, 1:-1, 1:-1] = 0.16666666666666666 * (u[:-2, 1:-1, 1:-1] + u[2:, 1:-1, 1:-1] + u[1:-1, :-2, 1:-1] + u[1:-1, 2:, 1:-1] + u[1:-1, 1:-1, :-2] + u[1:-1, 1:-1, 2:])

interface = CCSInterface()
interface.connect('192.168.1.115', 1234, 4)
execute(interface)

print(u.get(interface))
