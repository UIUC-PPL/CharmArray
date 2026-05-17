from charmnumeric.array import connect, ndarray
from charmnumeric.ast import set_max_depth
from charmnumeric.ccs import enable_debug
import charmnumeric.linalg as lg
import numpy as np

# enable_debug()
set_max_depth(2)

def f():
    a = ndarray(1, 10, np.float64, init_value=4)
    b = ndarray(1, 10, np.float64, init_value=1)
    c = ndarray(1, 10, np.float64, init_value=3)
    d = ndarray(1, 10, np.float64, init_value=2)
    # vx = [a, b]
    for _ in range(1):
        e = a + b + c + d
        f = e + a
        print(f.get())
        # e = a + b * d - c + 42 - 34
        # f = e + c / a + 32 - b
        # g = f.scale(69) + 53 - a / 32
        # g = f + d
        # k = a + b + c + v
        # k.get()
        # prog
        # k + -> k temp object -> ref k
        # + operation
        # tree
        # op (generate command)
        # l.get()
    # print(k.get())
    # print(l.get())
    # v1 = v @ b
    # v1 = (b + c) @ (b - c)
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
    # a1 = b @ c
    # print(a1.get())
    # a2 = v @ c
    # print(a2.get())
    # res = (a1 / a2) * b + c
    # v = 2
    # # a3 = a1 + a2
    # print(v.get())
    # res = a3 * v
    # w = c @ b
    # w.get()
    # res = q.abs() + c
    # baka = final_res.get()
    # v1 = b.copy()
    # print(res.get())
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
    connect("127.0.0.1", 10000)
    s = f()

