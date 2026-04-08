import charmnumeric as cnp
from charmtyles.core import execute

u = cnp.zeros((128, 128, 128), dtype=cnp.float32)

u[0, :, :] = 1.0
u[-1, :, :] = 1.0
u[:, 0, :] = 1.0
u[:, -1, :] = 1.0
u[:, :, 0] = 1.0
u[:, :, -1] = 1.0

for it in range(100):
    u[1:-1, 1:-1, 1:-1] = 0.16666666666666666 * (u[:-2, 1:-1, 1:-1] + u[2:, 1:-1, 1:-1] + u[1:-1, :-2, 1:-1] + u[1:-1, 2:, 1:-1] + u[1:-1, 1:-1, :-2] + u[1:-1, 1:-1, 2:])

interface = cnp.CharmNumericInterface()
interface.connect('192.168.1.115', 1234, 4)
execute(interface)

print(u.get(interface))
