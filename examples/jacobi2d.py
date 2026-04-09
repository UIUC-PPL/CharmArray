import charmnumeric as cnp
from charmtyles.core import execute, set_auto_flush

interface = cnp.CharmNumericInterface()
interface.connect('192.168.1.115', 1234, 4)
set_auto_flush(interface, 1000)

u = cnp.zeros((2048, 2048), dtype=cnp.float32)

u[0, :] = 1.0
u[-1, :] = 1.0
u[:, 0] = 1.0
u[:, -1] = 1.0

for it in range(20):
    u[1:-1, 1:-1] = 0.25 * (u[:-2, 1:-1] + u[2:, 1:-1] + u[1:-1, :-2] + u[1:-1, 2:])

#plot_execution_state()

print(u.get(interface))
