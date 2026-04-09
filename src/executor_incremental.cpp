#include "backend_internal.hpp"

#include <algorithm>
#include <type_traits>

#ifdef USE_KOKKOS
#include <KokkosBlas3_gemm.hpp>
#endif

template <int N>
void ArrayDAGExecutorND<N>::on_matmatmul_receive(int node_id, int input_index) {
    if constexpr (N != 2)
        return;

    auto it = pending.find(node_id);
    if (it == pending.end())
        return;
    PendingComm<N>& comm = it->second;

    // The just-arrived panel is the last element in remote_buffers[input_index]
    auto& arrived_bufs = comm.remote_buffers[input_index];
    if (arrived_bufs.empty())
        return;
    auto& new_panel = arrived_bufs.back();

    // Scan all panels on the opposite side for k-overlap
    int other_side = 1 - input_index;
    auto other_it = comm.remote_buffers.find(other_side);
    if (other_it == comm.remote_buffers.end() || other_it->second.empty())
        return;

    DAGNode* node = comm.node;
    ASTNode* mm_root = nullptr;
    for (ASTNode* root : node->ast->roots) {
        if (static_cast<Opcode>(root->opcode) == Opcode::MATMATMUL) {
            mm_root = root;
            break;
        }
    }
    if (!mm_root)
        return;

    int result_name = mm_root->result_name;
    auto arr_it = partition->arrays.find(result_name);
    if (arr_it == partition->arrays.end())
        return;

    DType dt = arr_it->second->dtype;
    auto nd_idx = partition->nd_index();
    int c_row_lo = arr_it->second->decomp.chare_start_global(0, nd_idx[0]);
    int c_col_lo = arr_it->second->decomp.chare_start_global(1, nd_idx[1]);
    int sub_rows = arr_it->second->region.size(0);
    int sub_cols = arr_it->second->region.size(1);

    // Dispatch by dtype for typed computation
    auto compute_pair = [&](auto* dummy) {
        using T = std::remove_pointer_t<decltype(dummy)>;

        for (auto& other_panel : other_it->second) {
            // Determine which is A (input_index=0) and which is B (input_index=1)
            auto& a_buf = (input_index == 0) ? new_panel : other_panel;
            auto& b_buf = (input_index == 0) ? other_panel : new_panel;

            int a_k_start = a_buf.region.start[0];
            int a_k_end = a_buf.region.stop[0];
            int a_c_row_start = a_buf.region.start[1];
            int a_c_row_end = a_buf.region.stop[1];
            int a_rows = a_c_row_end - a_c_row_start;
            int a_k_size = a_k_end - a_k_start;

            int b_k_start = b_buf.region.start[0];
            int b_k_end = b_buf.region.stop[0];
            int b_c_col_start = b_buf.region.start[1];
            int b_c_col_end = b_buf.region.stop[1];
            int b_cols = b_c_col_end - b_c_col_start;

            // Check k-range overlap
            int k_lo = std::max(a_k_start, b_k_start);
            int k_hi = std::min(a_k_end, b_k_end);
            if (k_lo >= k_hi)
                continue;

            int c_local_row = a_c_row_start - c_row_lo;
            int c_local_col = b_c_col_start - c_col_lo;

            T* a_data = reinterpret_cast<T*>(a_buf.data);
            T* b_data = reinterpret_cast<T*>(b_buf.data);

            int k_size = k_hi - k_lo;
            int a_col_off = k_lo - a_k_start;
            int b_row_off = k_lo - b_k_start;

#ifdef USE_KOKKOS
            // Device-side GEMM: rb.data is a device pointer under USE_KOKKOS
            T* c_device = static_cast<T*>(arr_it->second->device_data_ptr());

            Kokkos::View<T**, Kokkos::LayoutRight, DeviceSpace,
                         Kokkos::MemoryTraits<Kokkos::Unmanaged>>
                d_A_full(a_data, a_rows, a_k_size);
            auto d_A_sub = Kokkos::subview(d_A_full, Kokkos::ALL,
                                           Kokkos::make_pair(a_col_off, a_col_off + k_size));

            int b_k_size = b_k_end - b_k_start;
            Kokkos::View<T**, Kokkos::LayoutRight, DeviceSpace,
                         Kokkos::MemoryTraits<Kokkos::Unmanaged>>
                d_B_full(b_data, b_k_size, b_cols);
            auto d_B_sub = Kokkos::subview(d_B_full,
                                           Kokkos::make_pair(b_row_off, b_row_off + k_size),
                                           Kokkos::ALL);

            Kokkos::View<T**, Kokkos::LayoutRight, DeviceSpace,
                         Kokkos::MemoryTraits<Kokkos::Unmanaged>>
                d_C_full(c_device, sub_rows, sub_cols);
            auto d_C_sub = Kokkos::subview(d_C_full,
                                           Kokkos::make_pair(c_local_row, c_local_row + a_rows),
                                           Kokkos::make_pair(c_local_col, c_local_col + b_cols));

            KokkosBlas::gemm("N", "N", T(1), d_A_sub, d_B_sub, T(1), d_C_sub);
#else
            T* c_data = static_cast<T*>(arr_it->second->data_ptr());
            T* c_sub = c_data + c_local_row * sub_cols + c_local_col;

            using RMat = Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>;
            using CStride = Eigen::Stride<Eigen::Dynamic, 1>;

            Eigen::Map<RMat, 0, CStride>
                A_map(a_data + a_col_off, a_rows, k_size, CStride(a_k_size, 1));
            Eigen::Map<RMat, 0, CStride>
                B_map(b_data + b_row_off * b_cols, k_size, b_cols, CStride(b_cols, 1));
            Eigen::Map<RMat, 0, CStride>
                C_map(c_sub, a_rows, b_cols, CStride(sub_cols, 1));
            C_map.noalias() += A_map * B_map;
#endif
        }
    };

    switch (dt) {
    case DType::FLOAT32: {
        float* d = nullptr;
        compute_pair(d);
        break;
    }
    case DType::FLOAT64: {
        double* d = nullptr;
        compute_pair(d);
        break;
    }
    case DType::INT32: {
        int32_t* d = nullptr;
        compute_pair(d);
        break;
    }
    case DType::INT64: {
        int64_t* d = nullptr;
        compute_pair(d);
        break;
    }
    }
}

template <int N>
void ArrayDAGExecutorND<N>::on_matmul_partial(int node_id, RemoteBuffer<N>& partial_buf) {
    // No longer used — partials are accumulated on 1D partition directly
}

template <int N>
void ArrayDAGExecutorND<N>::matmul_check_finalize(int node_id) {
    // No longer used — partials are accumulated on 1D partition directly
}

template void ArrayDAGExecutorND<1>::on_matmatmul_receive(int, int);
template void ArrayDAGExecutorND<2>::on_matmatmul_receive(int, int);
template void ArrayDAGExecutorND<3>::on_matmatmul_receive(int, int);

template void ArrayDAGExecutorND<1>::on_matmul_partial(int, RemoteBuffer<1>&);
template void ArrayDAGExecutorND<2>::on_matmul_partial(int, RemoteBuffer<2>&);
template void ArrayDAGExecutorND<3>::on_matmul_partial(int, RemoteBuffer<3>&);

template void ArrayDAGExecutorND<1>::matmul_check_finalize(int);
template void ArrayDAGExecutorND<2>::matmul_check_finalize(int);
template void ArrayDAGExecutorND<3>::matmul_check_finalize(int);
