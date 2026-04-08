import charmnumeric as cnp
from charmtyles.core import execute

u = cnp.zeros((128,), dtype=cnp.float32)
#b = cnp.zeros((100, 100), dtype=cnp.float32)


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
interface = cnp.CharmNumericInterface()
interface.connect('192.168.1.114', 1234, 4)
execute(interface)

print(u.get(interface))
