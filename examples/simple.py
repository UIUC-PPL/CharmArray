from charmnumeric.charmnumeric import create_array
from charmtyles.core import plot_execution_state, execute
from charmtyles.interface import CCSInterface
import numpy as np

a = create_array((128,), dtype=np.float32)
b = create_array((128,), dtype=np.float32)
#c = create_array(10, dtype=np.float32)

x = 2 * (a + b + 3)
x[:50] = x[70:120] + 1
#y = 2 * x
#x[1:50] = 1#3 * c[1:50]
#x[49:100] = 2#b[49:100] + 5
#z = a + x

# a[0, :, :10] = c[0]
# a[0, :, 10:] = 2

# b[0] = a[0] * 3

#plot_execution_state()
interface = CCSInterface()
interface.connect('192.168.1.114', 1234, 4)
execute(interface)

print(x.get(interface))
