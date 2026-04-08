from charmnumeric.charmnumeric import create_array
from charmtyles.core import plot_execution_state, execute
from charmtyles.interface import CCSInterface
import numpy as np

u = create_array((128,), dtype=np.float32)
#b = create_array((100, 100), dtype=np.float32)


# u[0, :] = 1.0
# u[-1, :] = 1.0
# u[:, 0] = 1.0
# u[:, -1] = 1.0

# for it in range(3):
#     u[1:-1, 1:-1] = 0.25 * (u[:-2, 1:-1] + u[2:, 1:-1] + u[1:-1, :-2] + u[1:-1, 2:])

u[0] = 1.0
u[-1] = 1.0

for it in range(3):
    t = 0.5 * (u[:-2] + u[2:])
    u[1:-1] = t
    #u, u2 = u2, u

#plot_execution_state()
interface = CCSInterface()
interface.connect('192.168.1.114', 1234, 4)
execute(interface)

print(u.get(interface))
