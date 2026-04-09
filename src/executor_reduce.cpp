#include "backend_internal.hpp"

#ifdef USE_KOKKOS
#include <KokkosBlas1_dot.hpp>
#endif

template <int N>
void ArrayDAGExecutorND<N>::execute_reduce_node(DAGNode* node) {
    if constexpr (N != 1) {
        CkAbort("REDUCE only supported for 1D partitions (N=%d)", N);
        return;
    }

    if constexpr (N == 1) {
        ASTNode* reduce_root = nullptr;
        for (ASTNode* root : node->ast->roots) {
            if (static_cast<Opcode>(root->opcode) == Opcode::REDUCE) {
                reduce_root = root;
                break;
            }
        }
        int lhs_name = reduce_root->operands[0]->result_name;
        int rhs_name = reduce_root->operands[1]->result_name;
        int result_name = reduce_root->result_name;

        DType dt = determine_dtype<1>(node, partition->arrays);

        DBG_PRINT("[1D Chare %d] execute_reduce_node: lhs=%d rhs=%d result=%d\n",
                  partition->index[0], lhs_name, rhs_name, result_name);

        // Compute local dot product (0 if this chare has no data for either operand)
        auto lhs_it = partition->arrays.find(lhs_name);
        auto rhs_it = partition->arrays.find(rhs_name);

        // Build ReduceContrib with metadata + local partial value
        ReduceContrib contrib = {};
        contrib.node_id = node->id;
        contrib.result_name = result_name;
        contrib.dtype_int = static_cast<int>(dt);

        if (lhs_it != partition->arrays.end() && rhs_it != partition->arrays.end()) {
            int local_size =
                std::min(lhs_it->second->local_size(), rhs_it->second->local_size());

#ifdef USE_KOKKOS
            auto kokkos_dot = [&](auto dummy) {
                using VT = decltype(dummy);
                Kokkos::View<VT*, DeviceSpace, Kokkos::MemoryTraits<Kokkos::Unmanaged>>
                    d_lhs(static_cast<VT*>(lhs_it->second->device_data_ptr()), local_size);
                Kokkos::View<VT*, DeviceSpace, Kokkos::MemoryTraits<Kokkos::Unmanaged>>
                    d_rhs(static_cast<VT*>(rhs_it->second->device_data_ptr()), local_size);
                VT val = KokkosBlas::dot(d_lhs, d_rhs);
                memcpy(contrib.value, &val, sizeof(VT));
            };
            switch (dt) {
            case DType::FLOAT32: kokkos_dot(float{}); break;
            case DType::FLOAT64: kokkos_dot(double{}); break;
            case DType::INT32: kokkos_dot(int32_t{}); break;
            case DType::INT64: kokkos_dot(int64_t{}); break;
            }
#else
            lhs_it->second->copyToHost();
            rhs_it->second->copyToHost();

            switch (dt) {
            case DType::FLOAT32: {
                float val = eigen_dot(static_cast<float*>(lhs_it->second->data_ptr()),
                                      static_cast<float*>(rhs_it->second->data_ptr()),
                                      local_size);
                memcpy(contrib.value, &val, sizeof(float));
                break;
            }
            case DType::FLOAT64: {
                double val = eigen_dot(static_cast<double*>(lhs_it->second->data_ptr()),
                                       static_cast<double*>(rhs_it->second->data_ptr()),
                                       local_size);
                memcpy(contrib.value, &val, sizeof(double));
                break;
            }
            case DType::INT32: {
                int32_t val = eigen_dot(static_cast<int32_t*>(lhs_it->second->data_ptr()),
                                        static_cast<int32_t*>(rhs_it->second->data_ptr()),
                                        local_size);
                memcpy(contrib.value, &val, sizeof(int32_t));
                break;
            }
            case DType::INT64: {
                int64_t val = eigen_dot(static_cast<int64_t*>(lhs_it->second->data_ptr()),
                                        static_cast<int64_t*>(rhs_it->second->data_ptr()),
                                        local_size);
                memcpy(contrib.value, &val, sizeof(int64_t));
                break;
            }
            }
#endif

            DBG_PRINT("[1D Chare %d]   local dot computed (local_size=%d)\n",
                      partition->index[0], local_size);
        }
        // else: contrib.value is all zeros — contributes 0 to the reduction

        // All chares contribute to the Charm++ reduction using the custom
        // reducer that carries metadata (node_id, result_name, dtype).
        ChareIndex<1> target_ci;
        target_ci.idx[0] = 0;

        auto* dag_group = static_cast<ArrayDAGGroup*>(group);
        CkCallback cb(CkIndex_Partition1D::reduce_result(nullptr),
                      proxy_at<1>(dag_group->partition_proxy_1, target_ci));
        partition->contribute(sizeof(ReduceContrib), &contrib, reduce_dot_sum_type, cb);

        // Non-zero chares are done — their partial is in flight via the reduction.
        auto nd_idx = partition->nd_index();
        if (nd_idx[0] != 0) {
            node_finished(node->id);
        }
        // Chare 0 waits for reduce_result callback before calling node_finished.
    }
}

template void ArrayDAGExecutorND<1>::execute_reduce_node(DAGNode*);
template void ArrayDAGExecutorND<2>::execute_reduce_node(DAGNode*);
template void ArrayDAGExecutorND<3>::execute_reduce_node(DAGNode*);
