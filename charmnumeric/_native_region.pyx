# distutils: language = c++
# cython: language_level=3

from charmtyles._native_region cimport CPPRegion, NativeRegion

cdef extern from "array_region.hpp":
    void* make_array_region_handle_1 "make_array_region_handle<1>"(const int*, const int*, const int*, bint)
    void* make_array_region_handle_2 "make_array_region_handle<2>"(const int*, const int*, const int*, bint)
    void* make_array_region_handle_3 "make_array_region_handle<3>"(const int*, const int*, const int*, bint)
    void* make_array_region_handle_4 "make_array_region_handle<4>"(const int*, const int*, const int*, bint)
    void* make_array_region_handle_5 "make_array_region_handle<5>"(const int*, const int*, const int*, bint)
    void* make_array_region_handle_6 "make_array_region_handle<6>"(const int*, const int*, const int*, bint)
    void* make_array_region_handle_7 "make_array_region_handle<7>"(const int*, const int*, const int*, bint)
    void* make_array_region_handle_8 "make_array_region_handle<8>"(const int*, const int*, const int*, bint)

    void delete_array_region_handle_1 "delete_array_region_handle<1>"(void*)
    void delete_array_region_handle_2 "delete_array_region_handle<2>"(void*)
    void delete_array_region_handle_3 "delete_array_region_handle<3>"(void*)
    void delete_array_region_handle_4 "delete_array_region_handle<4>"(void*)
    void delete_array_region_handle_5 "delete_array_region_handle<5>"(void*)
    void delete_array_region_handle_6 "delete_array_region_handle<6>"(void*)
    void delete_array_region_handle_7 "delete_array_region_handle<7>"(void*)
    void delete_array_region_handle_8 "delete_array_region_handle<8>"(void*)

    bint overlaps_array_region_handle_1 "overlaps_array_region_handle<1>"(void*, void*)
    bint overlaps_array_region_handle_2 "overlaps_array_region_handle<2>"(void*, void*)
    bint overlaps_array_region_handle_3 "overlaps_array_region_handle<3>"(void*, void*)
    bint overlaps_array_region_handle_4 "overlaps_array_region_handle<4>"(void*, void*)
    bint overlaps_array_region_handle_5 "overlaps_array_region_handle<5>"(void*, void*)
    bint overlaps_array_region_handle_6 "overlaps_array_region_handle<6>"(void*, void*)
    bint overlaps_array_region_handle_7 "overlaps_array_region_handle<7>"(void*, void*)
    bint overlaps_array_region_handle_8 "overlaps_array_region_handle<8>"(void*, void*)

    bint covers_array_region_handle_1 "covers_array_region_handle<1>"(void*, void*)
    bint covers_array_region_handle_2 "covers_array_region_handle<2>"(void*, void*)
    bint covers_array_region_handle_3 "covers_array_region_handle<3>"(void*, void*)
    bint covers_array_region_handle_4 "covers_array_region_handle<4>"(void*, void*)
    bint covers_array_region_handle_5 "covers_array_region_handle<5>"(void*, void*)
    bint covers_array_region_handle_6 "covers_array_region_handle<6>"(void*, void*)
    bint covers_array_region_handle_7 "covers_array_region_handle<7>"(void*, void*)
    bint covers_array_region_handle_8 "covers_array_region_handle<8>"(void*, void*)

    bint intersect_array_region_handle_1 "intersect_array_region_handle<1>"(void*, void*, int*, int*, int*)
    bint intersect_array_region_handle_2 "intersect_array_region_handle<2>"(void*, void*, int*, int*, int*)
    bint intersect_array_region_handle_3 "intersect_array_region_handle<3>"(void*, void*, int*, int*, int*)
    bint intersect_array_region_handle_4 "intersect_array_region_handle<4>"(void*, void*, int*, int*, int*)
    bint intersect_array_region_handle_5 "intersect_array_region_handle<5>"(void*, void*, int*, int*, int*)
    bint intersect_array_region_handle_6 "intersect_array_region_handle<6>"(void*, void*, int*, int*, int*)
    bint intersect_array_region_handle_7 "intersect_array_region_handle<7>"(void*, void*, int*, int*, int*)
    bint intersect_array_region_handle_8 "intersect_array_region_handle<8>"(void*, void*, int*, int*, int*)


cdef int _MAX_NDIMS = 8


cdef inline void _fill_buffer(tuple values, int* out, int ndims):
    cdef int i
    for i in range(ndims):
        out[i] = <int>values[i]


cdef class NativeArrayRegion(NativeRegion):
    cdef void* _ptr
    cdef int _ndims

    def __cinit__(self, tuple start, tuple stop, tuple step, bint is_global=False):
        cdef int start_buf[8]
        cdef int stop_buf[8]
        cdef int step_buf[8]

        self._ptr = NULL
        self._ndims = len(start)
        if self._ndims < 1 or self._ndims > _MAX_NDIMS:
            raise ValueError(f"Native ArrayRegion wrapper supports 1-{_MAX_NDIMS} dims")
        if len(stop) != self._ndims or len(step) != self._ndims:
            raise ValueError("start, stop, and step must have the same rank")

        _fill_buffer(start, start_buf, self._ndims)
        _fill_buffer(stop, stop_buf, self._ndims)
        _fill_buffer(step, step_buf, self._ndims)

        if self._ndims == 1:
            self._ptr = make_array_region_handle_1(start_buf, stop_buf, step_buf, is_global)
        elif self._ndims == 2:
            self._ptr = make_array_region_handle_2(start_buf, stop_buf, step_buf, is_global)
        elif self._ndims == 3:
            self._ptr = make_array_region_handle_3(start_buf, stop_buf, step_buf, is_global)
        elif self._ndims == 4:
            self._ptr = make_array_region_handle_4(start_buf, stop_buf, step_buf, is_global)
        elif self._ndims == 5:
            self._ptr = make_array_region_handle_5(start_buf, stop_buf, step_buf, is_global)
        elif self._ndims == 6:
            self._ptr = make_array_region_handle_6(start_buf, stop_buf, step_buf, is_global)
        elif self._ndims == 7:
            self._ptr = make_array_region_handle_7(start_buf, stop_buf, step_buf, is_global)
        else:
            self._ptr = make_array_region_handle_8(start_buf, stop_buf, step_buf, is_global)
        self._region_ptr = <CPPRegion*>self._ptr

    def __dealloc__(self):
        if self._ptr == NULL:
            return
        if self._ndims == 1:
            delete_array_region_handle_1(self._ptr)
        elif self._ndims == 2:
            delete_array_region_handle_2(self._ptr)
        elif self._ndims == 3:
            delete_array_region_handle_3(self._ptr)
        elif self._ndims == 4:
            delete_array_region_handle_4(self._ptr)
        elif self._ndims == 5:
            delete_array_region_handle_5(self._ptr)
        elif self._ndims == 6:
            delete_array_region_handle_6(self._ptr)
        elif self._ndims == 7:
            delete_array_region_handle_7(self._ptr)
        else:
            delete_array_region_handle_8(self._ptr)
        self._region_ptr = NULL
        self._ptr = NULL

    cdef void _check_compatible(self, NativeArrayRegion other):
        if self._ndims != other._ndims:
            raise ValueError("ArrayRegion ranks must match")

    cpdef object intersect(self, NativeArrayRegion other):
        cdef int out_start[8]
        cdef int out_stop[8]
        cdef int out_step[8]
        cdef int i
        cdef bint ok
        cdef list start_values
        cdef list stop_values
        cdef list step_values

        self._check_compatible(other)

        if self._ndims == 1:
            ok = intersect_array_region_handle_1(self._ptr, other._ptr, out_start, out_stop, out_step)
        elif self._ndims == 2:
            ok = intersect_array_region_handle_2(self._ptr, other._ptr, out_start, out_stop, out_step)
        elif self._ndims == 3:
            ok = intersect_array_region_handle_3(self._ptr, other._ptr, out_start, out_stop, out_step)
        elif self._ndims == 4:
            ok = intersect_array_region_handle_4(self._ptr, other._ptr, out_start, out_stop, out_step)
        elif self._ndims == 5:
            ok = intersect_array_region_handle_5(self._ptr, other._ptr, out_start, out_stop, out_step)
        elif self._ndims == 6:
            ok = intersect_array_region_handle_6(self._ptr, other._ptr, out_start, out_stop, out_step)
        elif self._ndims == 7:
            ok = intersect_array_region_handle_7(self._ptr, other._ptr, out_start, out_stop, out_step)
        else:
            ok = intersect_array_region_handle_8(self._ptr, other._ptr, out_start, out_stop, out_step)

        if not ok:
            return None

        start_values = [0] * self._ndims
        stop_values = [0] * self._ndims
        step_values = [0] * self._ndims
        for i in range(self._ndims):
            start_values[i] = out_start[i]
            stop_values[i] = out_stop[i]
            step_values[i] = out_step[i]

        return (tuple(start_values), tuple(stop_values), tuple(step_values))
