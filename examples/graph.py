from charmnumeric.array import connect, ndarray
from charmnumeric.ast import set_max_depth
from charmnumeric.ccs import enable_debug
import charmnumeric.linalg as lg
import numpy as np

#enable_debug()
set_max_depth(10)

def f():
    v = ndarray(2, [10, 10], np.float64, init_value=-20)
    b = ndarray(1, 10, np.float64, init_value=10)
    c = ndarray(1, 10, np.float64, init_value=-30)
    # k = v * 2 + b + 3 + c - 32
    # l = k >= 42
    v1 = (b + c) @ (b - c)
    # q.get()
    # v1 = q @ c
    # v2 = b @ c
    # q1 = q @ v
    # v3 = b @ c

    # v1.get()
    # v2.get()
    # v3.get()

    # res =  v3 * 8 + v1 - 4 + v2.abs()

    # res.get()

    # final_res = res + 42

    # q.get()
    # w = c @ b
    # w.get()
    # res = q.abs() + c
    # baka = final_res.get()
    print(v1.get())
    # r = b.where(42, 69)
    # g = b.where(v, c)
    # z = ~r
    # g = v.abs() + b + 32
    # k = g + c * 8
    # k = g + 1
    # k = g + 2 * c - 3 * v
    # q = k.get()
    # print(res.get())
    # w = c
    # for i in range(5):
    #     y = v + b + w
    #     z = v - y
    #     w = 2 * (c - z) + b
    # w.evaluate()


if __name__ == '__main__':
    connect("172.17.0.1", 10000)
    s = f()

