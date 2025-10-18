from charmnumeric.array import connect, ndarray
import charmnumeric.linalg as lg
from charmnumeric.ccs import sync
from charmnumeric.ast import set_max_depth
import time
import numpy as np
import gc
#from ctypes import c_long
# import argparse for command-line parsing
import argparse

set_max_depth(1)

class A(object):
    def __init__(self):
        pass
    
def generate_random(N, min, max, D):
    diff = D(max) - D(min)
    rands = ndarray(1, (N,), D)
    rands = rands * diff
    rands = rands + D(min)
    return rands

def initialize(N, D):
    S = generate_random(N, 5, 30, D)
    X = generate_random(N, 1, 100, D)
    T = generate_random(N, 0.25, 10, D)
    R = 0.02
    V = 0.3
    return S, X, T, R, V
    
def cnd(d):
    A1 = 0.31938153
    A2 = -0.356563782
    A3 = 1.781477937
    A4 = -1.821255978
    A5 = 1.330274429
    RSQRT2PI = 0.39894228040143267793994605993438

    abs_d = ndarray.absolute(d)
    K = 1.0 / (1.0 + 0.2316419 * abs_d)
    cnd_val = RSQRT2PI * ndarray.exp(-0.5 * d * d) * (
        K * (A1 + K * (A2 + K * (A3 + K * (A4 + K * A5))))
    )
    return ndarray.where(d > 0, 1.0 - cnd_val, cnd_val)

def black_scholes(S, X, T, R, V):
    sqrt_t = ndarray.sqrt(T)
    d1 = (ndarray.log(S / X) + (R + 0.5 * V * V) * T) / (V * sqrt_t)
    d2 = d1 - V * sqrt_t
    cnd_d1 = cnd(d1)
    cnd_d2 = cnd(d2)
    exp_rt = ndarray.exp(-R * T)
    call_result = S * cnd_d1 - X * exp_rt * cnd_d2
    put_result = X * exp_rt * (1.0 - cnd_d2) - S * (1.0 - cnd_d1)
    return call_result, put_result

def run_black_scholes(N, D=np.float64):
    print("Running black scholes on %dK options..." % N)
    N *= 1000
    start_time = time.time()
    S, X, T, R, V = initialize(N, D)
    c, p = black_scholes(S, X, T, R, V)
    print(c.get())
    print(p.get())
    end_time = time.time()
    total = (end_time - start_time) * 1000  # Convert to milliseconds
    print("Elapsed Time: " + str(total) + " ms")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Black-Scholes benchmark")
    parser.add_argument("num", nargs="?", type=int, default=1,
                        help="Number of thousands of options (default: 1 => 1K)")
    args = parser.parse_args()

    connect("192.168.0.250", 10000)
    run_black_scholes(args.num)
    sync()