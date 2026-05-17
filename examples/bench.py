from charmnumeric.array import connect, ndarray
from charmnumeric.ast import set_max_depth
from charmnumeric.ccs import enable_debug
import charmnumeric.linalg as lg
import numpy as np
import time

set_max_depth(10)

def f():
    b = ndarray(1, 10, np.float64, init_value=10)
    v = ndarray(1, 10, np.float64, init_value=20)
    c = ndarray(1, 10, np.float64, init_value=20)
    v1 = (b + v) - c * 3
    v2 = b.scale(3) + c.add_constant(10)
    v3 = (b + c) @ v
    v1.get()
    v2.get()
    v3.get()
    start = time.time()
    for i in range(100):
        v1 = (b + v) - c * 3
        v2 = b.scale(3) + c.add_constant(10)
        v3 = (b + c) @ v
        v1.get()
        v2.get()
        v3.get()
        
    end = time.time()
    
    print("VECTOR BENCH ", end-start)
        
        
    start = time.time()
    b = ndarray(2, [10,10], np.float64, init_value=10)
    v = ndarray(2, [10,10], np.float64, init_value=20)
    c = ndarray(2, [10,10], np.float64, init_value=20)
    for i in range(100):
        v1 = (b + v) - c * 3
        v2 = b.exp() + c.add_constant(10)
        v3 = (b + c) @ v
        v1.get()
        v2.get()
        v3.get()
        
    end = time.time()
    print("MARIX BENCH ", end-start)

if __name__ == '__main__':
    connect("172.17.0.1", 10000)
    s = f()