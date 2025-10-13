from charmnumeric.array import connect, ndarray
from charmnumeric.ast import set_max_depth
from charmnumeric.ccs import enable_debug
import charmnumeric.linalg as lg
import numpy as np

set_max_depth(10)

def f():
    v = ndarray(1, 50, np.float64, init_value=-20)
    b = ndarray(1, 50, np.float64, init_value=1)
    c = ndarray(1, 50, np.float64, init_value=30)
    d = ndarray(1, 50, np.float64, init_value=5)

    g1 = v.abs().add(b).weighted_average(c, 0.7, 0.3)
    g2 = b.log().exp()
    g3 = v.abs().scale(2).scale(2).add_constant(29) + b + 32
    
    
    print("k1:", g1.get())
    print("k2:", g2.get())
    print("k3:", g3.get())

if __name__ == '__main__':
    connect("127.0.0.1", 10000)
    f()
