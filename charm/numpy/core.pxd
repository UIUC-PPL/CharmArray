# cython: language_level=3, embedsignature=True
# distutils: language=c++

cimport numpy as np
import numpy as np

from libc.stdlib cimport malloc, free


cdef extern from "charm.h":
    void StartCharmExt(int argc, char **argv)


cdef extern from "<aum/aum.hpp>" namespace "aum" nogil:
    cdef cppclass matrix:
        matrix() except +
        matrix(int, int) except +

        matrix& operator=(const matrix&)
        matrix& operator=(matrix)

        matrix operator+(const matrix&, const matrix&)
        matrix operator+(matrix, const matrix&)
        matrix operator+(const matrix&, matrix)
        matrix operator+(matrix, matrix)


cdef class pymatrix:
    cdef public np.ndarray shape
    cdef matrix _data

    cdef inline matrix& get_data(self) nogil
    cdef inline void set_data(self, matrix& mat) nogil

    cdef inline void _allocate(self, shape)
