#pragma once

#include "backend.hpp"
#include "dispatch.hpp"

#ifdef USE_KOKKOS
#include <KokkosBlas1_axpby.hpp>
#else
#include <Eigen/Dense>
#endif

/// Eigen-based gemv: res = mat[:, :actual_cols] * vec.
/// Matrix is row-major with `local_cols` stride.
#ifndef USE_KOKKOS
template <typename T>
inline void eigen_gemv(T* mat_data, int local_rows, int local_cols,
                       T* vec_data, int actual_cols, T* res_data) {
    Eigen::Map<Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>>
        mat(mat_data, local_rows, local_cols);
    Eigen::Map<Eigen::Matrix<T, Eigen::Dynamic, 1>> vec(vec_data, actual_cols);
    Eigen::Map<Eigen::Matrix<T, Eigen::Dynamic, 1>> res(res_data, local_rows);
    res.noalias() = mat.leftCols(actual_cols) * vec;
}

/// Eigen-based sub-matrix gemv: res = mat[row_offset:row_offset+sub_rows,
///                                      col_offset:col_offset+sub_cols] * vec.
/// full_local_cols is the row stride of the full local matrix (row-major).
template <typename T>
inline void eigen_gemv_sub(T* mat_data, int full_local_cols,
                           int row_offset, int col_offset,
                           int sub_rows, int sub_cols,
                           T* vec_data, T* res_data) {
    typedef Eigen::Stride<Eigen::Dynamic, 1> RowStride;
    Eigen::Map<Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>,
               0, RowStride>
        sub_mat(mat_data + row_offset * full_local_cols + col_offset,
                sub_rows, sub_cols, RowStride(full_local_cols, 1));
    Eigen::Map<Eigen::Matrix<T, Eigen::Dynamic, 1>> vec(vec_data, sub_cols);
    Eigen::Map<Eigen::Matrix<T, Eigen::Dynamic, 1>> res(res_data, sub_rows);
    res.noalias() = sub_mat * vec;
}

/// Eigen-based gemv on a 2D sub-matrix extracted from a 3D tile.
/// The 3D tile has local shape (local_d0, local_d1, local_d2) in row-major order.
/// dropped_dim identifies which dimension is the singleton (0, 1, or 2).
/// dd_local_offset is the local index in the dropped dimension.
/// row_offset/col_offset are local offsets in the remaining 2 dims.
template <typename T>
inline void eigen_gemv_sub_3d(T* mat_data,
                               int local_d0, int local_d1, int local_d2,
                               int dropped_dim, int dd_local_offset,
                               int row_offset, int col_offset,
                               int sub_rows, int sub_cols,
                               T* vec_data, T* res_data) {
    T* base;
    int row_stride, col_stride;

    if (dropped_dim == 0) {
        // Sub-matrix at (dd, row, col): contiguous 2D block
        base = mat_data + dd_local_offset * local_d1 * local_d2
             + row_offset * local_d2 + col_offset;
        row_stride = local_d2;
        col_stride = 1;
    } else if (dropped_dim == 1) {
        // Sub-matrix at (row, dd, col)
        base = mat_data + row_offset * local_d1 * local_d2
             + dd_local_offset * local_d2 + col_offset;
        row_stride = local_d1 * local_d2;
        col_stride = 1;
    } else {
        // Sub-matrix at (row, col, dd)
        base = mat_data + row_offset * local_d1 * local_d2
             + col_offset * local_d2 + dd_local_offset;
        row_stride = local_d1 * local_d2;
        col_stride = local_d2;
    }

    typedef Eigen::Stride<Eigen::Dynamic, Eigen::Dynamic> GenStride;
    Eigen::Map<Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>,
               0, GenStride>
        sub_mat(base, sub_rows, sub_cols, GenStride(row_stride, col_stride));
    Eigen::Map<Eigen::Matrix<T, Eigen::Dynamic, 1>> vec(vec_data, sub_cols);
    Eigen::Map<Eigen::Matrix<T, Eigen::Dynamic, 1>> res(res_data, sub_rows);
    res.noalias() = sub_mat * vec;
}

/// Eigen-based dot product: returns a.dot(b) for vectors of length n.
template <typename T>
inline T eigen_dot(T* a_data, T* b_data, int n) {
    Eigen::Map<Eigen::Matrix<T, Eigen::Dynamic, 1>> a(a_data, n);
    Eigen::Map<Eigen::Matrix<T, Eigen::Dynamic, 1>> b(b_data, n);
    return a.dot(b);
}

/// Eigen-based gemm: C += A * B (accumulate).
/// A is (a_rows × a_cols), B is (a_cols × b_cols), C is (a_rows × b_cols).
/// All matrices are row-major, contiguous.
template <typename T>
inline void eigen_gemm(T* a_data, int a_rows, int a_cols,
                       T* b_data, int b_cols,
                       T* c_data) {
    using Mat = Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>;
    Eigen::Map<Mat> A(a_data, a_rows, a_cols);
    Eigen::Map<Mat> B(b_data, a_cols, b_cols);
    Eigen::Map<Mat> C(c_data, a_rows, b_cols);
    C.noalias() += A * B;
}

/// Eigen-based sub-matrix gemm: C += A_sub * B_sub (accumulate).
/// Extracts sub-blocks from row-major A and B by offset, accumulates into C.
/// A has row stride a_full_cols, B has row stride b_full_cols.
/// C is contiguous (sub_rows × sub_cols).
template <typename T>
inline void eigen_gemm_sub(T* a_data, int a_full_cols,
                           int a_row_offset, int a_col_offset,
                           int sub_rows, int k_size,
                           T* b_data, int b_full_cols,
                           int b_row_offset, int b_col_offset,
                           int sub_cols,
                           T* c_data) {
    typedef Eigen::Stride<Eigen::Dynamic, 1> RowStride;
    Eigen::Map<Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>,
               0, RowStride>
        A_sub(a_data + a_row_offset * a_full_cols + a_col_offset,
              sub_rows, k_size, RowStride(a_full_cols, 1));
    Eigen::Map<Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>,
               0, RowStride>
        B_sub(b_data + b_row_offset * b_full_cols + b_col_offset,
              k_size, sub_cols, RowStride(b_full_cols, 1));
    Eigen::Map<Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>>
        C(c_data, sub_rows, sub_cols);
    C.noalias() += A_sub * B_sub;
}
#endif

/// Helper: compute the tile size for a given number of dimensions.
inline int ct_tile(int ndims) {
    switch (ndims) {
    case 1:
        return CT_TILE_1D;
    case 2:
        return CT_TILE_2D;
    case 3:
        return CT_TILE_3D;
    default:
        CkAbort("Unsupported ndims=%d for ct_tile", ndims);
        return 0;
    }
}

/// Get the tile size for array `name` from array_meta.
/// Falls back to ct_tile(ndims) if the array has no metadata entry or tile == 0.
/// Use this in all executor communication paths instead of ct_tile() directly,
/// so that per-array tile sizes assigned by compute_decompositions are respected.
inline int array_tile(const std::unordered_map<int, ArrayDAGGroup::ArrayMetadata>& meta,
                      int name, int ndims) {
    auto it = meta.find(name);
    if (it != meta.end() && it->second.tile > 0)
        return it->second.tile;
    return ct_tile(ndims);
}

/// Walk the AST to extract input and output regions for communication.
/// Returns true if region operations were found.
template <int N>
bool extract_regions_nd(DAGNode* node, std::unordered_map<int, CTArrayBase<N>*>& arrays,
                        ArrayRegion<N>& r_out, std::vector<ArrayRegion<N>>& input_regions,
                        std::vector<int>& input_source_names, std::array<int, N>& global_shape_out);

/// Determine the DType for a DAG node from its AST or the arrays map.
template <int N>
DType determine_dtype(DAGNode* node, std::unordered_map<int, CTArrayBase<N>*>& arrays);

/// Send matmul result from a 3D partition back to a 1D partition.
template <typename T>
inline void cross_matmul_send_result_3d_to_1d(PartitionImpl<3>* partition, DAGNode* node,
                                              int result_name, const ChareIndex<1>& target_ci,
                                              T* result_data, int result_rows, int row_start,
                                              CProxy_Partition1D& proxy_1d) {
    int64_t byte_size = result_rows * sizeof(T);

    // Encode as 1D region: [row_start, row_start + result_rows)
    int region_data[1 * 3];
    region_data[0] = row_start;
    region_data[1] = row_start + result_rows;
    region_data[2] = 1;

#ifndef NDEBUG
    partition->comm_bytes_sent += byte_size;
#endif

#ifndef USE_KOKKOS
    T* send_buf = new T[result_rows];
    memcpy(send_buf, result_data, byte_size);
    proxy_at<1>(proxy_1d, target_ci)
        .receive_data(node->id, /*input_index=*/0, result_name, 1, region_data, byte_size,
                      reinterpret_cast<char*>(send_buf));
    delete[] send_buf;
#else
    T* send_buf = static_cast<T*>(Kokkos::kokkos_malloc<DeviceSpace>(byte_size));
    Kokkos::parallel_for(
        CT_COMM_POLICY(partition, result_rows),
        KOKKOS_LAMBDA(int i) { send_buf[i] = result_data[i]; });
    device_pack_send<3, 1>(partition, proxy_1d, target_ci, node->id, 0, result_name, region_data,
                           byte_size, send_buf);
#endif
}

/// Send matmul result from a 2D partition (column 0) back to a 1D partition.
template <typename T>
inline void cross_matmul_send_result_2d_to_1d(PartitionImpl<2>* partition, DAGNode* node,
                                              int result_name, const ChareIndex<1>& target_ci,
                                              T* result_data, int result_rows, int row_start,
                                              CProxy_Partition1D& proxy_1d) {
    int64_t byte_size = result_rows * sizeof(T);

    // Encode as 1D region: [row_start, row_start + result_rows)
    int region_data[1 * 3];
    region_data[0] = row_start;
    region_data[1] = row_start + result_rows;
    region_data[2] = 1;

#ifndef NDEBUG
    partition->comm_bytes_sent += byte_size;
#endif

#ifndef USE_KOKKOS
    T* send_buf = new T[result_rows];
    memcpy(send_buf, result_data, byte_size);
    proxy_at<1>(proxy_1d, target_ci)
        .receive_data(node->id, /*input_index=*/0, result_name, 1, region_data, byte_size,
                      reinterpret_cast<char*>(send_buf));
    delete[] send_buf;
#else
    T* send_buf = static_cast<T*>(Kokkos::kokkos_malloc<DeviceSpace>(byte_size));
    Kokkos::parallel_for(
        CT_COMM_POLICY(partition, result_rows),
        KOKKOS_LAMBDA(int i) { send_buf[i] = result_data[i]; });
    device_pack_send<2, 1>(partition, proxy_1d, target_ci, node->id, 0, result_name, region_data,
                           byte_size, send_buf);
#endif
}


/// ---------------------------------------------------------------------------
/// Custom Charm++ reduction for dot product (carries metadata + value)
/// ---------------------------------------------------------------------------

/// Data contributed by each chare for a REDUCE (dot product) operation.
/// Layout: [node_id, result_name, dtype, pad, value[8]] = 24 bytes.
struct ReduceContrib {
    int node_id;
    int result_name;
    int dtype_int;
    int pad;
    char value[8]; // large enough for float, double, int32_t, int64_t
};
static_assert(sizeof(ReduceContrib) == 24, "ReduceContrib must be 24 bytes");

/// Custom reducer: sums the value field based on dtype, preserving metadata.
CkReductionMsg* reduce_dot_sum(int nMsg, CkReductionMsg** msgs);

/// Global reducer type handle — registered once during init.
extern CkReduction::reducerType reduce_dot_sum_type;

/// Call once to register the custom reducer.
void register_reduce_dot_sum();

/// ---------------------------------------------------------------------------
/// Cross-partition SET_REGION send helpers
/// ---------------------------------------------------------------------------

/// Compute the dimension mapping between source (N_src) and target (N_tgt).
/// For higher→lower: finds non-singleton source dims.
/// For lower→higher: finds non-singleton target region dims.
/// dim_map[tgt_dim] = src_dim  (maps each target dimension to a source dimension)
/// Singleton target dims get mapped to the corresponding singleton source dim.
template <int N_src, int N_tgt>
inline void compute_dim_map(const int* src_global_shape, Region* tgt_region_base, int* dim_map) {
    if constexpr (N_src > N_tgt) {
        // Higher→Lower: non-singleton source dims map to target dims in order
        int tgt_d = 0;
        for (int sd = 0; sd < N_src && tgt_d < N_tgt; ++sd) {
            if (src_global_shape[sd] > 1)
                dim_map[tgt_d++] = sd;
        }
        // If all source dims are size 1, just map in order
        if (tgt_d == 0)
            for (int d = 0; d < N_tgt; ++d)
                dim_map[d] = d;
    } else {
        // Lower→Higher: non-singleton target region dims receive source dims in order
        auto* tgt_region = static_cast<ArrayRegion<N_tgt>*>(tgt_region_base);
        int src_d = 0;
        for (int td = 0; td < N_tgt && src_d < N_src; ++td) {
            if (tgt_region->size(td) > 1)
                dim_map[td] = src_d++;
            else
                dim_map[td] = -1; // singleton — use target region's fixed value
        }
    }
}

/// Send local source data from PartitionImpl<N_src> to target chares on Partition<N_tgt>.
/// The data is packed and sent with region encoded in N_tgt-dimensional target coordinates.
template <int N_src, int N_tgt, typename T>
inline void cross_set_region_send(PartitionImpl<N_src>* partition, DAGNode* node, int source_name,
                                  const int* dim_map, Region* tgt_region_base,
                                  typename PartitionTraits<N_tgt>::ProxyType& target_proxy,
                                  const ArrayDecomp<N_tgt>& tgt_decomp) {
    auto src_it = partition->arrays.find(source_name);
    if (src_it == partition->arrays.end() || src_it->second->local_size() == 0)
        return;

    auto* src = static_cast<Array<N_src, T>*>(src_it->second);
    auto* tgt_region = static_cast<ArrayRegion<N_tgt>*>(tgt_region_base);

    auto src_nd = partition->nd_index();
    auto& src_decomp = src->decomp;
    auto src_chare_global = src_decomp.chare_region_global(src_nd);

    // Compute source local region in global coordinates
    std::array<int, N_src> src_cs;
    std::array<int, N_src> src_local_start, src_local_stop;
    for (int d = 0; d < N_src; ++d) {
        src_cs[d] = src_chare_global.start[d];
        src_local_start[d] = src_chare_global.start[d];
        src_local_stop[d] = src_chare_global.start[d] + src->region.size(d);
    }

    // Map to target coordinates: build the target region this chare's data covers
    std::array<int, N_tgt> tgt_start, tgt_stop, tgt_step;
    for (int td = 0; td < N_tgt; ++td) {
        tgt_step[td] = 1;
        if (dim_map[td] >= 0) {
            int sd = dim_map[td];
            tgt_start[td] = src_local_start[sd];
            tgt_stop[td] = src_local_stop[sd];
        } else {
            // Singleton target dim: use the target region's fixed value
            tgt_start[td] = tgt_region->start[td];
            tgt_stop[td] = tgt_region->stop[td];
        }
    }
    ArrayRegion<N_tgt> mapped_region(tgt_start, tgt_stop, tgt_step);

    // Decompose into target chares using the target array's decomposition
    auto chare_map = decompose(mapped_region, tgt_decomp);

    // Compute source strides
    std::array<int, N_src> src_strides;
    src_strides[N_src - 1] = 1;
    for (int d = N_src - 2; d >= 0; --d)
        src_strides[d] = src_strides[d + 1] * src->region.size(d + 1);

    for (auto& [ci, cr] : chare_map) {
        // Compute the size of this fragment
        int64_t total_size = cr.size();
        int64_t byte_size = total_size * sizeof(T);

        // Encode region in N_tgt coordinates
        int region_data[N_tgt * 3];
        for (int d = 0; d < N_tgt; ++d) {
            region_data[d * 3 + 0] = cr.start[d];
            region_data[d * 3 + 1] = cr.stop[d];
            region_data[d * 3 + 2] = 1;
        }

#ifndef NDEBUG
        partition->comm_bytes_sent += byte_size;
#endif

#ifndef USE_KOKKOS
        src->copyToHost();
        T* send_buf = new T[total_size];
        T* src_data = static_cast<T*>(src->data_ptr());

        // Pack data: iterate over the target fragment, map back to source coords
        std::array<int, N_tgt> idx;
        for (int d = 0; d < N_tgt; ++d)
            idx[d] = cr.start[d];
        for (int64_t flat = 0; flat < total_size; ++flat) {
            int src_flat = 0;
            for (int sd = 0; sd < N_src; ++sd) {
                // Find which target dim maps to this source dim
                int coord = 0;
                for (int td = 0; td < N_tgt; ++td) {
                    if (dim_map[td] == sd) {
                        coord = idx[td] - src_cs[sd];
                        break;
                    }
                }
                src_flat += coord * src_strides[sd];
            }
            send_buf[flat] = src_data[src_flat];

            // Advance odometer
            for (int d = N_tgt - 1; d >= 0; --d) {
                if (++idx[d] < cr.stop[d])
                    break;
                idx[d] = cr.start[d];
            }
        }

        proxy_at<N_tgt>(target_proxy, ci)
            .receive_data(node->id, /*input_index=*/0, source_name, N_tgt, region_data, byte_size,
                          reinterpret_cast<char*>(send_buf));
        delete[] send_buf;
#else
        T* send_buf = static_cast<T*>(Kokkos::kokkos_malloc<DeviceSpace>(byte_size));
        T* src_device = static_cast<T*>(src->device_data_ptr());

        // Capture for lambda
        int dm[N_tgt], sc[N_src], ss[N_src], cr_start[N_tgt], cr_sizes[N_tgt];
        for (int d = 0; d < N_tgt; ++d) {
            dm[d] = dim_map[d];
            cr_start[d] = cr.start[d];
            cr_sizes[d] = cr.size(d);
        }
        for (int d = 0; d < N_src; ++d) {
            sc[d] = src_cs[d];
            ss[d] = src_strides[d];
        }

        Kokkos::parallel_for(
            CT_COMM_POLICY(partition, total_size), KOKKOS_LAMBDA(int flat_idx) {
                int remaining = flat_idx;
                int tgt_coords[N_tgt];
                for (int d = N_tgt - 1; d >= 0; --d) {
                    tgt_coords[d] = cr_start[d] + remaining % cr_sizes[d];
                    remaining /= cr_sizes[d];
                }
                int src_flat = 0;
                for (int sd = 0; sd < N_src; ++sd) {
                    int coord = 0;
                    for (int td = 0; td < N_tgt; ++td) {
                        if (dm[td] == sd) {
                            coord = tgt_coords[td] - sc[sd];
                            break;
                        }
                    }
                    src_flat += coord * ss[sd];
                }
                send_buf[flat_idx] = src_device[src_flat];
            });
        device_pack_send<N_src, N_tgt>(partition, target_proxy, ci, node->id, 0, source_name,
                                       region_data, byte_size, send_buf);
#endif
    }
}

/// Dispatch cross_set_region_send by DType.
template <int N_src, int N_tgt>
inline void dispatch_cross_set_region_send(DType dt, PartitionImpl<N_src>* partition, DAGNode* node,
                                           int source_name, const int* dim_map,
                                           Region* tgt_region_base,
                                           typename PartitionTraits<N_tgt>::ProxyType& target_proxy,
                                           const ArrayDecomp<N_tgt>& tgt_decomp) {
    switch (dt) {
    case DType::FLOAT32:
        cross_set_region_send<N_src, N_tgt, float>(partition, node, source_name, dim_map,
                                                    tgt_region_base, target_proxy, tgt_decomp);
        break;
    case DType::FLOAT64:
        cross_set_region_send<N_src, N_tgt, double>(partition, node, source_name, dim_map,
                                                     tgt_region_base, target_proxy, tgt_decomp);
        break;
    case DType::INT32:
        cross_set_region_send<N_src, N_tgt, int32_t>(partition, node, source_name, dim_map,
                                                      tgt_region_base, target_proxy, tgt_decomp);
        break;
    case DType::INT64:
        cross_set_region_send<N_src, N_tgt, int64_t>(partition, node, source_name, dim_map,
                                                      tgt_region_base, target_proxy, tgt_decomp);
        break;
    }
}
