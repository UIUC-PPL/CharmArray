from .charmnumeric import create_array
import numpy as np

def randn(*args):
    return create_array(args, dtype=np.float32)
