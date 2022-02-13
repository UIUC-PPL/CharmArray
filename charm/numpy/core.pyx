# cython: language_level=3, embedsignature=True
# distutils: language=c++

import sys


def initialize():
    args = [arg.encode() for arg in sys.argv]
    cdef int num_args = len(args)
    cdef int i = 0
    try:
        argv = <char**> malloc(num_args * sizeof(char*))
        for i in range(num_args):
            argv[i] = <char*>args[i]
        StartCharmExt(num_args, argv)
    finally:
        free(argv)


cdef pymatrix wrap_matrix(matrix& mat):
    cdef pymatrix pymat = pymatrix()
    pymat.set_data(mat)
    return pymat


cdef class pymatrix:
    def __init__(self, shape=None):
        if shape:
            self.shape = shape
        else:
            self.shape = np.array([0, 0], dtype=np.int32)

    def __cinit__(self, shape=None):
        if shape:
            self._allocate(shape)

    def __add__(self, other):
        cdef matrix result = self._data + other.get_data()
        return wrap_matrix(result)

    cdef inline void _allocate(self, shape):
        self._data = matrix(shape[0], shape[1])

    cdef inline matrix& get_data(self) nogil:
        return self._data

    cdef inline void set_data(self, matrix& mat) nogil:
        self._data = mat

