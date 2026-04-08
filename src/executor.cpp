#include "backend_internal.hpp"
#include "dispatch.hpp"

#include <unordered_set>

#ifdef USE_KOKKOS
#include <KokkosBlas1_dot.hpp>
#include <KokkosBlas2_gemv.hpp>
#include <KokkosBlas3_gemm.hpp>
#endif

// ---- Custom reducer for dot product ----

CkReduction::reducerType reduce_dot_sum_type;

CkReductionMsg* reduce_dot_sum(int nMsg, CkReductionMsg** msgs) {
    // All messages have the same layout: ReduceContrib (24 bytes).
    // Sum the value field based on dtype, preserve metadata from first msg.
    ReduceContrib result;
    memcpy(&result, msgs[0]->getData(), sizeof(ReduceContrib));

    DType dt = static_cast<DType>(result.dtype_int);
    for (int i = 1; i < nMsg; ++i) {
        ReduceContrib other;
        memcpy(&other, msgs[i]->getData(), sizeof(ReduceContrib));
        switch (dt) {
        case DType::FLOAT32: {
            float a, b;
            memcpy(&a, result.value, sizeof(float));
            memcpy(&b, other.value, sizeof(float));
            a += b;
            memcpy(result.value, &a, sizeof(float));
            break;
        }
        case DType::FLOAT64: {
            double a, b;
            memcpy(&a, result.value, sizeof(double));
            memcpy(&b, other.value, sizeof(double));
            a += b;
            memcpy(result.value, &a, sizeof(double));
            break;
        }
        case DType::INT32: {
            int32_t a, b;
            memcpy(&a, result.value, sizeof(int32_t));
            memcpy(&b, other.value, sizeof(int32_t));
            a += b;
            memcpy(result.value, &a, sizeof(int32_t));
            break;
        }
        case DType::INT64: {
            int64_t a, b;
            memcpy(&a, result.value, sizeof(int64_t));
            memcpy(&b, other.value, sizeof(int64_t));
            a += b;
            memcpy(result.value, &a, sizeof(int64_t));
            break;
        }
        }
    }
    return CkReductionMsg::buildNew(sizeof(ReduceContrib), &result);
}

void register_reduce_dot_sum() {
    reduce_dot_sum_type = CkReduction::addReducer(reduce_dot_sum);
}

// ---- Unified ArrayDAGExecutorND ----

template <int N>
DType determine_dtype(DAGNode* node, std::unordered_map<int, CTArrayBase<N>*>& arrays) {
    // Check AST roots for dtype info
    for (ASTNode* root : node->ast->roots)
        if (root->dtype != DType::FLOAT32)
            return root->dtype;
    // Fall back to first referenced array's dtype
    for (ASTNode* root : node->ast->roots)
        for (ASTNode* operand : root->operands)
            if (!operand->is_scalar && !operand->is_broadcast) {
                auto it = arrays.find(operand->result_name);
                if (it != arrays.end())
                    return it->second->dtype;
            }
    return DType::FLOAT32;
}

/// Helper: pack and send data for a single remote input.
/// Type-specific because it accesses Array<N,T>::data.
template <int N, typename T>
static void send_remote_input(PartitionImpl<N>* partition, DAGNode* node, const RemoteSend<N>& send,
                              int inp_name, const std::array<int, N>& nd_idx) {
    auto arr_it = partition->arrays.find(inp_name);
    if (arr_it == partition->arrays.end())
        return;
    Array<N, T>* arr = static_cast<Array<N, T>*>(arr_it->second);

    // Use the input array's own decomp for chare region and local offset
    ArrayRegion<N> r_chare_inp = arr->decomp.chare_region_global(nd_idx);
    auto [overlap, has_overlap] = intersect(send.region, r_chare_inp);
    if (!has_overlap)
        return;

    std::array<int, N> cs;
    for (int d = 0; d < N; ++d)
        cs[d] = arr->decomp.chare_start_global(d, nd_idx[d]);
    int64_t total_size = overlap.size();

    {
        bool out_of_bounds = false;
        for (int d = 0; d < N; ++d) {
            int local_start_d = overlap.start[d] - cs[d];
            int phys_extent = local_start_d + (overlap.size(d) - 1) * overlap.step[d] + 1;
            if (local_start_d < 0 || phys_extent > arr->region.size(d)) {
                out_of_bounds = true;
                break;
            }
        }
        if (out_of_bounds)
            return;
    }

    std::array<int, N> arr_strides;
    arr_strides[N - 1] = 1;
    for (int d = N - 2; d >= 0; --d)
        arr_strides[d] = arr_strides[d + 1] * arr->region.size(d + 1);

    int region_data[N * 3];
    for (int d = 0; d < N; ++d) {
        region_data[d * 3 + 0] = overlap.start[d];
        region_data[d * 3 + 1] = overlap.stop[d];
        region_data[d * 3 + 2] = overlap.step[d];
    }
    int64_t byte_size = total_size * sizeof(T);

#ifndef NDEBUG
    partition->comm_bytes_sent += byte_size;
#endif

#ifdef USE_KOKKOS
    // Pack on device and send via direct GPU messaging
    T* send_buf = static_cast<T*>(Kokkos::kokkos_malloc<DeviceSpace>(total_size * sizeof(T)));
    T* src_device = static_cast<T*>(arr->device_data_ptr());

    // Capture strides/offsets in plain arrays for KOKKOS_LAMBDA
    int overlap_sizes[N], local_starts[N], src_strides_arr[N], overlap_steps[N];
    for (int d = 0; d < N; d++) {
        overlap_sizes[d] = overlap.size(d);
        local_starts[d] = overlap.start[d] - cs[d];
        src_strides_arr[d] = arr_strides[d];
        overlap_steps[d] = overlap.step[d];
    }

    Kokkos::parallel_for(
        CT_COMM_POLICY(partition, total_size), KOKKOS_LAMBDA(int flat_idx) {
            int remaining = flat_idx;
            int src_flat = 0;
            for (int d = N - 1; d >= 0; --d) {
                int coord_d = remaining % overlap_sizes[d];
                remaining /= overlap_sizes[d];
                src_flat += (local_starts[d] + coord_d * overlap_steps[d]) * src_strides_arr[d];
            }
            send_buf[flat_idx] = src_device[src_flat];
        });

    device_pack_send<N>(partition, partition->thisProxy, send.target,
                        node->id, send.input_index, inp_name, region_data,
                        byte_size, send_buf);
#else
    // Host path: pack on CPU and send via regular Charm++ messaging
    arr->copyToHost();
    T* send_buf = new T[total_size];
    T* host_data = static_cast<T*>(arr->data_ptr());

    if (overlap.step[N - 1] == 1) {
        // Fast path: innermost dimension is contiguous, use memcpy
        int64_t inner_size = overlap.size(N - 1);
        int64_t buf_offset = 0;
        std::array<int, N> idx = {};
        while (true) {
            int64_t src_flat = 0;
            for (int d = 0; d < N; ++d)
                src_flat += (int64_t)(overlap.start[d] + idx[d] * overlap.step[d] - cs[d]) *
                            arr_strides[d];
            memcpy(send_buf + buf_offset, host_data + src_flat, inner_size * sizeof(T));
            buf_offset += inner_size;

            int d = N - 2;
            while (d >= 0) {
                if (++idx[d] < overlap.size(d))
                    break;
                idx[d] = 0;
                --d;
            }
            if (d < 0)
                break;
        }
    } else {
        // Element-wise packing for non-unit innermost step
        int64_t buf_offset = 0;
        std::array<int, N> idx = {};
        while (true) {
            int64_t src_flat = 0;
            for (int d = 0; d < N; ++d)
                src_flat += (int64_t)(overlap.start[d] + idx[d] * overlap.step[d] - cs[d]) *
                            arr_strides[d];
            send_buf[buf_offset++] = host_data[src_flat];

            int d = N - 1;
            while (d >= 0) {
                if (++idx[d] < overlap.size(d))
                    break;
                idx[d] = 0;
                --d;
            }
            if (d < 0)
                break;
        }
    }

    proxy_at<N>(partition->thisProxy, send.target)
        .receive_data(node->id, send.input_index, inp_name, N, region_data, byte_size,
                      reinterpret_cast<char*>(send_buf));
    delete[] send_buf;
#endif
}

/// Dispatch the typed send for a single remote input based on runtime DType.
template <int N>
static void dispatch_send(DType dt, PartitionImpl<N>* partition, DAGNode* node,
                          const RemoteSend<N>& send, int inp_name,
                          const std::array<int, N>& nd_idx) {
    switch (dt) {
    case DType::FLOAT32:
        send_remote_input<N, float>(partition, node, send, inp_name, nd_idx);
        break;
    case DType::FLOAT64:
        send_remote_input<N, double>(partition, node, send, inp_name, nd_idx);
        break;
    case DType::INT32:
        send_remote_input<N, int32_t>(partition, node, send, inp_name, nd_idx);
        break;
    case DType::INT64:
        send_remote_input<N, int64_t>(partition, node, send, inp_name, nd_idx);
        break;
    }
}

/// Dispatch execute_node_nd based on runtime DType.
template <int N>
static void dispatch_execute(DType dt, ArrayDAGGroup* group, DAGNode* node, PartitionImpl<N>* partition,
                             PendingComm<N>* comm) {
    switch (dt) {
    case DType::FLOAT32:
        group->execute_node_nd<N, float>(node, partition, comm);
        break;
    case DType::FLOAT64:
        group->execute_node_nd<N, double>(node, partition, comm);
        break;
    case DType::INT32:
        group->execute_node_nd<N, int32_t>(node, partition, comm);
        break;
    case DType::INT64:
        group->execute_node_nd<N, int64_t>(node, partition, comm);
        break;
    }
}

template <int N>
void ArrayDAGExecutorND<N>::delete_array(int name) {
    auto* dag_group = static_cast<ArrayDAGGroup*>(group);
    const int had_live_meta = dag_group->live_array_meta.count(name) ? 1 : 0;
    DBG_PRINT("[PE %d] Partition<%d> delete_array epoch=%d name=%d live_meta=%d\n",
              CkMyPe(), N, epoch, name, had_live_meta);
    partition->retire_array(name);
    dag_group->live_array_meta.erase(name);
}

template <int N>
void ArrayDAGExecutorND<N>::execute_dag_node(DAGNode* node) {
    // Check if this node is relevant to our partition dimensionality
    bool relevant = false;
    bool is_matmul_node = false;
    bool is_matmatmul_node = false;
    bool is_reduce_node = false;
    bool is_cross_set_region = false;
    bool is_diag_node = false;
    bool is_tile_node = false;
    // Broadcast operands: size-1 arrays from another partition used as scalars
    struct BroadcastInfo {
        int source_name;
        int source_ndims;
        int target_ndims;
    };
    std::vector<BroadcastInfo> broadcast_ops;
    auto* dag_group_meta = static_cast<ArrayDAGGroup*>(group);
    auto nd_idx = partition->nd_index();

    auto root_output_name = [](ASTNode* root) {
        if (root == nullptr)
            return -1;
        if (static_cast<Opcode>(root->opcode) == Opcode::SET_REGION &&
            !root->operands.empty() && root->operands[0] != nullptr)
            return root->operands[0]->result_name;
        return root->result_name;
    };

    auto chare_owns_result = [&](ASTNode* root) -> bool {
        if (root == nullptr || root->ndims != N)
            return false;

        int output_name = root_output_name(root);
        auto meta_it = dag_group_meta->array_meta.find(output_name);
        if (meta_it == dag_group_meta->array_meta.end())
            return false;

        auto decomp = meta_it->second.template decomp<N>();
        auto chare_region = decomp.chare_region_global(nd_idx);
        if (chare_region.size() <= 0)
            return false;

        if (static_cast<Opcode>(root->opcode) == Opcode::SET_REGION && root->region != nullptr &&
            !root->region->is_global) {
            auto* out_region = static_cast<ArrayRegion<N>*>(root->region);
            auto out_region_global = decomp.to_global(*out_region);
            auto [overlap, has_overlap] = intersect(out_region_global, chare_region);
            return has_overlap && overlap.size() > 0;
        }

        return true;
    };

    for (ASTNode* root : node->ast->roots) {
        auto opc = static_cast<Opcode>(root->opcode);
        if (opc == Opcode::CREATE) {
            if (root->ndims == N)
                relevant = true;
            continue;
        }
        if (opc == Opcode::REDUCE) {
            is_reduce_node = true;
            if (N == 1)
                relevant = true;
            continue;
        }
        if (opc == Opcode::MATMUL) {
            is_matmul_node = true;
            // MATMUL is relevant for 1D (vector send/result receive),
            // 2D (matrix compute), and 3D (dimension-dropped matvec)
            if (N == 1 || N == 2 || N == 3)
                relevant = true;
            continue;
        }
        if (opc == Opcode::MATMATMUL) {
            is_matmatmul_node = true;
            // MATMATMUL is relevant for 2D chares that hold A, B, or will hold C,
            // and 3D chares with dim-dropped operands
            if constexpr (N == 2) {
                // Check if this chare holds A or B
                for (ASTNode* operand : root->operands)
                    if (!operand->is_scalar && !operand->is_broadcast &&
                        partition->arrays.count(operand->result_name))
                        relevant = true;
                // Check if this chare will hold part of C
                auto* dag_group_tmp = static_cast<ArrayDAGGroup*>(group);
                auto c_meta = dag_group_tmp->array_meta.find(root->result_name);
                if (c_meta != dag_group_tmp->array_meta.end()) {
                    auto nd = partition->nd_index();
                    auto c_decomp = c_meta->second.template decomp<2>();
                    auto c_chare = c_decomp.chare_region_global(nd);
                    if (c_chare.stop[0] > c_chare.start[0] &&
                        c_chare.stop[1] > c_chare.start[1])
                        relevant = true;
                }
            } else if constexpr (N == 3) {
                relevant = true;
            }
            continue;
        }
        if (opc == Opcode::DIAG) {
            is_diag_node = true;
            // DIAG is relevant for 1D and 2D partitions
            if (N == 1 || N == 2)
                relevant = true;
            continue;
        }
        if (opc == Opcode::TILE) {
            is_tile_node = true;
            int input_ndims = root->operands[0]->ndims;
            int result_ndims = root->ndims;
            if (N == input_ndims || N == result_ndims)
                relevant = true;
            continue;
        }
        if (opc == Opcode::SET_REGION && root->operands.size() >= 2 &&
            !root->operands[1]->is_scalar && !root->operands[1]->is_broadcast) {
            int source_ndims = root->operands[1]->ndims;
            int target_ndims = root->ndims;
            if (source_ndims != target_ndims) {
                is_cross_set_region = true;
                // Relevant if we are the source or target partition
                if (N == source_ndims || N == target_ndims)
                    relevant = true;
                continue;
            }
        }
        // Detect broadcast operands (cross-partition or same-partition)
        for (ASTNode* operand : root->operands) {
            if (operand->is_broadcast) {
                auto* dag_group = static_cast<ArrayDAGGroup*>(group);
                auto meta_it = dag_group->array_meta.find(operand->result_name);
                // DBG_PRINT("[Chare %d]   broadcast operand name=%d meta_found=%d\n",
                //           partition->index[0], operand->result_name,
                //           (int)(meta_it != dag_group->array_meta.end()));
                if (meta_it != dag_group->array_meta.end()) {
                    int src_nd = meta_it->second.ndims;
                    int tgt_nd = root->ndims;
                    // DBG_PRINT("[Chare %d]   broadcast src_nd=%d tgt_nd=%d\n",
                    //           partition->index[0], src_nd, tgt_nd);
                    broadcast_ops.push_back({operand->result_name, src_nd, tgt_nd});
                    // Relevant for source and target partitions
                    if (N == src_nd || N == tgt_nd)
                        relevant = true;
                }
            }
        }
        // For other ops, check if any operand arrays exist on this partition
        for (ASTNode* operand : root->operands)
            if (!operand->is_scalar && !operand->is_broadcast &&
                partition->arrays.count(operand->result_name))
                relevant = true;
        if (partition->arrays.count(root->result_name))
            relevant = true;
        if (chare_owns_result(root))
            relevant = true;
    }
    if (!relevant) {
        DBG_PRINT("[PE %d] Partition<%d> chare %d: IRRELEVANT node %d\n",
                 CkMyPe(), N, partition->index[0], node->id);
        node_finished(node->id);
        return;
    }

    // Check for REDUCE nodes — 1D dot product with cross-chare reduction
    if (is_reduce_node) {
        execute_reduce_node(node);
        return;
    }

    // Check for MATMUL nodes — these use a custom communication pattern
    if (is_matmul_node) {
        execute_matmul_node(node);
        return;
    }

    // Check for MATMATMUL nodes — SUMMA matrix-matrix multiply
    if (is_matmatmul_node) {
        execute_matmatmul_node(node);
        return;
    }

    // Check for cross-partition SET_REGION
    if (is_cross_set_region) {
        execute_cross_set_region_node(node);
        return;
    }

    // Check for DIAG nodes — cross-partition diagonal construction/extraction
    if (is_diag_node) {
        execute_diag_node(node);
        return;
    }

    // Check for TILE nodes — numpy.tile repetition with possible ndims change
    if (is_tile_node) {
        execute_tile_node(node);
        return;
    }

    // Handle cross-partition broadcast: send scalar values from source to target
    if (!broadcast_ops.empty()) {
        auto* dag_group = static_cast<ArrayDAGGroup*>(group);
        bool is_source_only = true;  // true if this partition only sends, doesn't compute
        bool has_local_broadcast = false; // true if broadcast source is on this partition+chare

        // Compute broadcast input index for each broadcast op (shared logic).
        // Must match the receiver convention: n_memref_leaves + bcast_index.
        // First count all memref leaves, then find the broadcast index.
        auto compute_bcast_input_idx = [&](int source_name) -> int {
            int n_memrefs = 0;
            for (ASTNode* root : node->ast->roots) {
                auto root_opc = static_cast<Opcode>(root->opcode);
                for (ASTNode* operand : root->operands) {
                    if (operand->is_scalar || operand->is_broadcast)
                        continue;
                    if (root_opc == Opcode::SET_REGION && operand == root->operands[0])
                        continue;
                    n_memrefs++;
                }
            }

            // Second pass: find the broadcast index for source_name
            std::unordered_set<int> seen;
            int bcast_idx = 0;
            for (ASTNode* root : node->ast->roots) {
                for (ASTNode* operand : root->operands) {
                    if (operand->is_scalar || seen.count(operand->result_name))
                        continue;
                    seen.insert(operand->result_name);
                    if (operand->is_broadcast) {
                        if (operand->result_name == source_name)
                            return n_memrefs + bcast_idx;
                        bcast_idx++;
                    }
                }
            }
            return n_memrefs;
        };

        // Helper: send scalar to a target chare via receive_data
        // Helper: send a broadcast scalar to a target chare identified by ND index
        auto send_broadcast = [&](int tgt_nd, const std::array<int, 3>& tgt_idx,
                                  int bcast_input_idx, int source_name, char* val_ptr,
                                  int elem_sz) {
            std::vector<int> region_data(tgt_nd * 3, 0);
            for (int d = 0; d < tgt_nd; ++d) {
                region_data[d * 3 + 1] = 1;
                region_data[d * 3 + 2] = 1;
            }
            char* send_buf = new char[elem_sz];
            memcpy(send_buf, val_ptr, elem_sz);
#ifndef NDEBUG
            partition->comm_bytes_sent += elem_sz;
#endif
            switch (tgt_nd) {
            case 1: {
                ChareIndex<1> ci;
                ci.idx[0] = tgt_idx[0];
                proxy_at<1>(dag_group->partition_proxy_1, ci)
                    .receive_data(node->id, bcast_input_idx, source_name, tgt_nd,
                                  region_data.data(), elem_sz, send_buf);
                break;
            }
            case 2: {
                ChareIndex<2> ci;
                ci.idx[0] = tgt_idx[0];
                ci.idx[1] = tgt_idx[1];
                proxy_at<2>(dag_group->partition_proxy_2, ci)
                    .receive_data(node->id, bcast_input_idx, source_name, tgt_nd,
                                  region_data.data(), elem_sz, send_buf);
                break;
            }
            case 3: {
                ChareIndex<3> ci;
                ci.idx[0] = tgt_idx[0];
                ci.idx[1] = tgt_idx[1];
                ci.idx[2] = tgt_idx[2];
                proxy_at<3>(dag_group->partition_proxy_3, ci)
                    .receive_data(node->id, bcast_input_idx, source_name, tgt_nd,
                                  region_data.data(), elem_sz, send_buf);
                break;
            }
            }
            delete[] send_buf;
        };

        for (auto& bcast : broadcast_ops) {
            int bcast_input_idx = compute_bcast_input_idx(bcast.source_name);

            if (N == bcast.source_ndims) {
                // We are on the source partition — send if we have the array
                auto arr_it = partition->arrays.find(bcast.source_name);
                DBG_PRINT("[Chare %d]   bcast send check: name=%d found=%d local_size=%d\n",
                          partition->index[0], bcast.source_name,
                          (int)(arr_it != partition->arrays.end()),
                          (arr_it != partition->arrays.end()) ? arr_it->second->local_size() : -1);
                if (arr_it != partition->arrays.end() && arr_it->second->local_size() > 0) {
                    int elem_sz = arr_it->second->elem_size();
#ifdef USE_KOKKOS
                    char host_scalar[8];
                    Kokkos::deep_copy(
                        Kokkos::View<char*, HostSpace, Kokkos::MemoryTraits<Kokkos::Unmanaged>>(
                            host_scalar, elem_sz),
                        Kokkos::View<char*, DeviceSpace, Kokkos::MemoryTraits<Kokkos::Unmanaged>>(
                            static_cast<char*>(arr_it->second->device_data_ptr()), elem_sz));
                    char* val_ptr = host_scalar;
#else
                    arr_it->second->copyToHost();
                    char* val_ptr = static_cast<char*>(arr_it->second->data_ptr());
#endif

                    if (bcast.source_ndims == bcast.target_ndims) {
                        // Same-partition broadcast: send to all OTHER chares
                        has_local_broadcast = true;
                        auto pg_it = dag_group->partition_grid.find(N);
                        if (pg_it != dag_group->partition_grid.end()) {
                            int total_chares = 1;
                            for (int d = 0; d < N; ++d)
                                total_chares *= pg_it->second.grid[d];
                            for (int t = 0; t < total_chares; ++t) {
                                std::array<int, 3> tgt_idx = {};
                                int rem = t;
                                for (int d = N - 1; d >= 0; --d) {
                                    tgt_idx[d] = rem % pg_it->second.grid[d];
                                    rem /= pg_it->second.grid[d];
                                }
                                // Skip self
                                bool is_self = true;
                                for (int d = 0; d < N; ++d)
                                    if (tgt_idx[d] != nd_idx[d])
                                        is_self = false;
                                if (is_self)
                                    continue;
                                send_broadcast(N, tgt_idx, bcast_input_idx, bcast.source_name,
                                               val_ptr, elem_sz);
                            }
                        }
                    } else {
                        // Cross-partition broadcast: send to all chares on target partition
                        int tgt_nd = bcast.target_ndims;
                        auto pg_it = dag_group->partition_grid.find(tgt_nd);
                        if (pg_it != dag_group->partition_grid.end()) {
                            int total_chares = 1;
                            for (int d = 0; d < tgt_nd; ++d)
                                total_chares *= pg_it->second.grid[d];
                            for (int t = 0; t < total_chares; ++t) {
                                std::array<int, 3> tgt_idx = {};
                                int rem = t;
                                for (int d = tgt_nd - 1; d >= 0; --d) {
                                    tgt_idx[d] = rem % pg_it->second.grid[d];
                                    rem /= pg_it->second.grid[d];
                                }
                                send_broadcast(tgt_nd, tgt_idx, bcast_input_idx,
                                               bcast.source_name, val_ptr, elem_sz);
                            }
                        }
                    }
                }
            }

            if (N == bcast.target_ndims)
                is_source_only = false;
        }

        // If this partition is only the broadcast source (cross-partition), we're done
        if (is_source_only) {
            node_finished(node->id);
            return;
        }

        // Count expected broadcast messages for this chare
        // For same-partition: chares that DON'T have the source array expect 1 message per broadcast
        // For cross-partition: all target chares expect 1 message per broadcast
        int n_expected_broadcasts = 0;
        for (auto& bcast : broadcast_ops) {
            if (N != bcast.target_ndims)
                continue;
            if (bcast.source_ndims == bcast.target_ndims) {
                // Same-partition: only expect a message if we DON'T have the array locally
                bool have_it = partition->arrays.count(bcast.source_name) > 0;
                DBG_PRINT("[Chare %d]   bcast count: name=%d src_nd=%d tgt_nd=%d has_local_broadcast=%d have_it=%d\n",
                          partition->index[0], bcast.source_name, bcast.source_ndims, bcast.target_ndims,
                          (int)has_local_broadcast, (int)have_it);
                if (!has_local_broadcast)
                    n_expected_broadcasts++;
            } else {
                // Cross-partition: we always expect a message
                n_expected_broadcasts++;
            }
        }
        DBG_PRINT("[Chare %d]   n_expected_broadcasts=%d is_source_only=%d\n",
                  partition->index[0], n_expected_broadcasts, (int)is_source_only);

        if (n_expected_broadcasts > 0) {
            auto pre_it = pending.find(node->id);
            int pre_arrived = 0;
            std::unordered_map<int, std::vector<RemoteBuffer<N>>> pre_buffers;
            if (pre_it != pending.end()) {
                pre_arrived = -(pre_it->second.expected_msgs);
                pre_buffers = std::move(pre_it->second.remote_buffers);
            }

            int remaining = n_expected_broadcasts - pre_arrived;
            pending[node->id] = {node, remaining, {}, std::move(pre_buffers)};

            if (remaining <= 0) {
                on_comm_done(node->id);
            } else {
                DBG_PRINT("[Chare %d]   -> waiting for %d broadcast values\n",
                          partition->index[0], remaining);
            }
            return;
        }
        // If no messages expected (source chare in same-partition broadcast),
        // fall through to normal execution — broadcast value is already local.
    }

    ArrayRegion<N> r_out;
    std::vector<ArrayRegion<N>> input_regions;
    std::vector<int> input_source_names;
    std::array<int, N> global_shape;

    DType dt = determine_dtype<N>(node, partition->arrays);

    // DBG_PRINT("[Chare %d] execute_dag_node<%d>: node_id=%d\n", partition->index[0], N, node->id);

    if (!extract_regions_nd<N>(node, partition->arrays, r_out, input_regions, input_source_names,
                               global_shape) ||
        input_regions.empty()) {
        // DBG_PRINT("[Chare %d]   no regions -> direct execute\n", partition->index[0]);
        dispatch_execute<N>(dt, static_cast<ArrayDAGGroup*>(group), node, partition, nullptr);
#ifdef USE_NVIDIA
        {
            auto* p = new ComputeDoneParam<N>{partition, node->id, false};
            CkCallback hcb(compute_done_cb<N>, p);
            hapiAddCallback(partition->compute_stream_raw, &hcb);
        }
#else
        CT_KOKKOS_FENCE();
        // DBG_PRINT("[PE %d][Chare %d] direct-exec node %d -> node_finished (done=%d/%d)\n",
        //          CkMyPe(), partition->index[0], node->id,
        //          dag->num_nodes_done + 1, dag->num_nodes);
        node_finished(node->id);
#endif
        return;
    }

    // Build per-input decomps and output decomp from array metadata
    auto* dag_group = static_cast<ArrayDAGGroup*>(group);
    std::vector<ArrayDecomp<N>> input_decomps;
    for (int i = 0; i < (int)input_source_names.size(); ++i) {
        auto arr_it = partition->arrays.find(input_source_names[i]);
        if (arr_it != partition->arrays.end()) {
            input_decomps.push_back(arr_it->second->decomp);
        } else {
            auto meta_it = dag_group->array_meta.find(input_source_names[i]);
            if (meta_it != dag_group->array_meta.end())
                input_decomps.push_back(meta_it->second.template decomp<N>());
            else
                input_decomps.push_back(ArrayDecomp<N>::default_decomp(global_shape,
                    array_tile(dag_group->array_meta, input_source_names[i], N)));

        }
    }

    // Output decomp: use the first non-temp root output, not the first AST root.
    // Fused ASTs often keep temporary internal roots ahead of the real result,
    // and those temps can legitimately have a different decomposition offset.
    ArrayDecomp<N> output_decomp;
    {
        int result_name = root_output_name(node->ast->roots[0]);
        for (ASTNode* root : node->ast->roots) {
            auto opc = static_cast<Opcode>(root->opcode);
            if (opc != Opcode::NOOP && opc != Opcode::CREATE && !root->is_temp) {
                result_name = root_output_name(root);
                break;
            }
        }
        auto arr_it = partition->arrays.find(result_name);
        if (arr_it != partition->arrays.end()) {
            output_decomp = arr_it->second->decomp;
        } else {
            auto meta_it = dag_group->array_meta.find(result_name);
            if (meta_it != dag_group->array_meta.end())
                output_decomp = meta_it->second.template decomp<N>();
            else
                output_decomp = ArrayDecomp<N>::default_decomp(global_shape,
                    array_tile(dag_group->array_meta, result_name, N));

        }
    }

    // Translate regions from local space to global space
    ArrayRegion<N> r_out_global = output_decomp.to_global(r_out);
    std::vector<ArrayRegion<N>> input_regions_global;
    for (int i = 0; i < (int)input_regions.size(); ++i)
        input_regions_global.push_back(input_decomps[i].to_global(input_regions[i]));

    ArrayRegion<N> r_chare_out = output_decomp.chare_region_global(nd_idx);

    // Send remote inputs (type-dispatched)
    auto sends = send_remote_inputs<N>(r_out_global, input_regions_global, nd_idx, output_decomp,
                                      input_decomps);
    // DBG_PRINT("[Chare %d]   %d sends to remote chares\n", partition->index[0], (int)sends.size());
    for (auto& send : sends) {
        int inp_name = input_source_names[send.input_index];
        dispatch_send<N>(dt, partition, node, send, inp_name, nd_idx);
    }

    // Determine expected messages
    auto li = local_inputs<N>(r_out_global, input_regions_global, nd_idx, output_decomp, input_decomps);
    // DBG_PRINT("[Chare %d]   expected_msgs=%d, %d my_inputs\n", partition->index[0],
    //           li.expected_msgs, (int)li.my_inputs.size());

    if (li.expected_msgs == 0) {
        if (li.my_inputs.empty()) {
            auto [r_myout, has_out] = intersect(r_out_global, r_chare_out);
            if (has_out && r_myout.size() > 0) {
                PendingComm<N> comm_local = {node, 0, {}, {}};
                dispatch_execute<N>(dt, static_cast<ArrayDAGGroup*>(group), node, partition,
                                    &comm_local);
            }
#ifdef USE_NVIDIA
            {
                auto* p = new ComputeDoneParam<N>{partition, node->id, false};
                CkCallback hcb(compute_done_cb<N>, p);
                hapiAddCallback(partition->compute_stream_raw, &hcb);
            }
#else
            CT_KOKKOS_FENCE();
            // DBG_PRINT("[PE %d][Chare %d] no-comm-no-inputs node %d -> node_finished (done=%d/%d)\n",
            //          CkMyPe(), partition->index[0], node->id,
            //          dag->num_nodes_done + 1, dag->num_nodes);
            node_finished(node->id);
#endif
        } else {
            PendingComm<N> comm_local = {node, 0, std::move(li.my_inputs), {}};
            dispatch_execute<N>(dt, static_cast<ArrayDAGGroup*>(group), node, partition,
                                &comm_local);
#ifdef USE_NVIDIA
            {
                auto* p = new ComputeDoneParam<N>{partition, node->id, false};
                CkCallback hcb(compute_done_cb<N>, p);
                hapiAddCallback(partition->compute_stream_raw, &hcb);
            }
#else
            CT_KOKKOS_FENCE();
            // DBG_PRINT("[PE %d][Chare %d] no-comm-with-inputs node %d -> node_finished (done=%d/%d)\n",
            //          CkMyPe(), partition->index[0], node->id,
            //          dag->num_nodes_done + 1, dag->num_nodes);
            node_finished(node->id);
#endif
        }
    } else {
        auto pre_it = pending.find(node->id);
        int pre_arrived = 0;
        std::unordered_map<int, std::vector<RemoteBuffer<N>>> pre_buffers;
        if (pre_it != pending.end()) {
            pre_arrived = -(pre_it->second.expected_msgs);
            pre_buffers = std::move(pre_it->second.remote_buffers);
        }

        int remaining = li.expected_msgs - pre_arrived;
        pending[node->id] = {node, remaining, std::move(li.my_inputs), std::move(pre_buffers)};

        if (remaining <= 0) {
            on_comm_done(node->id);
        } else {
            DBG_PRINT("[Chare %d]   -> waiting for %d remote messages\n", partition->index[0],
                      remaining);
        }
    }
}

/// Send a slice of vector data from a 1D partition to a 2D partition for cross-partition matmul.
/// local_offset: offset into this chare's local vector buffer
/// send_len: number of elements to send
/// col_start/col_end: the matrix column range this vector chunk maps to (global coords)
template <typename T>
static void cross_matmul_send_vector_slice_1d_to_2d(
    PartitionImpl<1>* partition, DAGNode* node, int vec_name,
    const ChareIndex<2>& target_ci, int local_offset, int send_len, int col_start, int col_end,
    CProxy_Partition2D& proxy_2d) {
    auto vec_it = partition->arrays.find(vec_name);
    if (vec_it == partition->arrays.end())
        return;
    Array<1, T>* vec = static_cast<Array<1, T>*>(vec_it->second);
    int64_t byte_size = send_len * sizeof(T);

    int region_data[2 * 3];
    region_data[0] = col_start;
    region_data[1] = col_end;
    region_data[2] = 1;
    region_data[3] = 0;
    region_data[4] = 1;
    region_data[5] = 1;

#ifndef NDEBUG
    partition->comm_bytes_sent += byte_size;
#endif

#ifdef USE_KOKKOS
    T* send_buf = static_cast<T*>(Kokkos::kokkos_malloc<DeviceSpace>(byte_size));
    T* src_device = static_cast<T*>(vec->device_data_ptr()) + local_offset;
    Kokkos::parallel_for(
        CT_COMM_POLICY(partition, send_len),
        KOKKOS_LAMBDA(int i) { send_buf[i] = src_device[i]; });
    device_pack_send<1, 2>(partition, proxy_2d, target_ci, node->id, 0, vec_name, region_data,
                           byte_size, send_buf);
#else
    vec->copyToHost();
    T* send_buf = new T[send_len];
    memcpy(send_buf, static_cast<T*>(vec->data_ptr()) + local_offset, byte_size);
    proxy_at<2>(proxy_2d, target_ci)
        .receive_data(node->id, /*input_index=*/0, vec_name, 2, region_data, byte_size,
                      reinterpret_cast<char*>(send_buf));
    delete[] send_buf;
#endif
}

/// Send a slice of vector data from a 1D partition to a 3D partition for cross-partition matmul.
template <typename T>
static void cross_matmul_send_vector_slice_1d_to_3d(
    PartitionImpl<1>* partition, DAGNode* node, int vec_name,
    const ChareIndex<3>& target_ci, int local_offset, int send_len, int col_start, int col_end,
    CProxy_Partition3D& proxy_3d) {
    auto vec_it = partition->arrays.find(vec_name);
    if (vec_it == partition->arrays.end())
        return;
    Array<1, T>* vec = static_cast<Array<1, T>*>(vec_it->second);
    int64_t byte_size = send_len * sizeof(T);

    int region_data[3 * 3];
    region_data[0] = col_start;
    region_data[1] = col_end;
    region_data[2] = 1;
    region_data[3] = 0;
    region_data[4] = 1;
    region_data[5] = 1;
    region_data[6] = 0;
    region_data[7] = 1;
    region_data[8] = 1;

#ifndef NDEBUG
    partition->comm_bytes_sent += byte_size;
#endif

#ifdef USE_KOKKOS
    T* send_buf = static_cast<T*>(Kokkos::kokkos_malloc<DeviceSpace>(byte_size));
    T* src_device = static_cast<T*>(vec->device_data_ptr()) + local_offset;
    Kokkos::parallel_for(
        CT_COMM_POLICY(partition, send_len),
        KOKKOS_LAMBDA(int i) { send_buf[i] = src_device[i]; });
    device_pack_send<1, 3>(partition, proxy_3d, target_ci, node->id, 0, vec_name, region_data,
                           byte_size, send_buf);
#else
    vec->copyToHost();
    T* send_buf = new T[send_len];
    memcpy(send_buf, static_cast<T*>(vec->data_ptr()) + local_offset, byte_size);
    proxy_at<3>(proxy_3d, target_ci)
        .receive_data(node->id, /*input_index=*/0, vec_name, 3, region_data, byte_size,
                      reinterpret_cast<char*>(send_buf));
    delete[] send_buf;
#endif
}

/// Dispatch cross-partition sliced vector send to 3D by DType.
static void dispatch_cross_matmul_send_vector_slice_3d(
    DType dt, PartitionImpl<1>* partition, DAGNode* node, int vec_name,
    const ChareIndex<3>& target_ci, int local_offset, int send_len, int col_start, int col_end,
    CProxy_Partition3D& proxy_3d) {
    switch (dt) {
    case DType::FLOAT32:
        cross_matmul_send_vector_slice_1d_to_3d<float>(partition, node, vec_name, target_ci,
                                                        local_offset, send_len, col_start, col_end,
                                                        proxy_3d);
        break;
    case DType::FLOAT64:
        cross_matmul_send_vector_slice_1d_to_3d<double>(partition, node, vec_name, target_ci,
                                                         local_offset, send_len, col_start,
                                                         col_end, proxy_3d);
        break;
    case DType::INT32:
        cross_matmul_send_vector_slice_1d_to_3d<int32_t>(partition, node, vec_name, target_ci,
                                                          local_offset, send_len, col_start,
                                                          col_end, proxy_3d);
        break;
    case DType::INT64:
        cross_matmul_send_vector_slice_1d_to_3d<int64_t>(partition, node, vec_name, target_ci,
                                                          local_offset, send_len, col_start,
                                                          col_end, proxy_3d);
        break;
    }
}

/// Dispatch cross-partition sliced vector send by DType.
static void dispatch_cross_matmul_send_vector_slice(
    DType dt, PartitionImpl<1>* partition, DAGNode* node, int vec_name,
    const ChareIndex<2>& target_ci, int local_offset, int send_len, int col_start, int col_end,
    CProxy_Partition2D& proxy_2d) {
    switch (dt) {
    case DType::FLOAT32:
        cross_matmul_send_vector_slice_1d_to_2d<float>(partition, node, vec_name, target_ci,
                                                        local_offset, send_len, col_start, col_end,
                                                        proxy_2d);
        break;
    case DType::FLOAT64:
        cross_matmul_send_vector_slice_1d_to_2d<double>(partition, node, vec_name, target_ci,
                                                         local_offset, send_len, col_start,
                                                         col_end, proxy_2d);
        break;
    case DType::INT32:
        cross_matmul_send_vector_slice_1d_to_2d<int32_t>(partition, node, vec_name, target_ci,
                                                          local_offset, send_len, col_start,
                                                          col_end, proxy_2d);
        break;
    case DType::INT64:
        cross_matmul_send_vector_slice_1d_to_2d<int64_t>(partition, node, vec_name, target_ci,
                                                          local_offset, send_len, col_start,
                                                          col_end, proxy_2d);
        break;
    }
}

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

template <int N>
void ArrayDAGExecutorND<N>::execute_matmul_node(DAGNode* node) {
    // Parse AST to find operand names and detect cross-partition case
    ASTNode* matmul_root = nullptr;
    for (ASTNode* root : node->ast->roots) {
        if (static_cast<Opcode>(root->opcode) == Opcode::MATMUL) {
            matmul_root = root;
            break;
        }
    }
    int mat_name = matmul_root->operands[0]->result_name;
    int vec_name = matmul_root->operands[1]->result_name;
    int result_name = matmul_root->result_name;

    auto* dag_group = static_cast<ArrayDAGGroup*>(group);

    if constexpr (N == 1) {
        // --- 1D partition role in cross-partition matmul ---
        int tile_1d = array_tile(dag_group->array_meta, vec_name, 1);

        auto nd_idx = partition->nd_index();
        int k = nd_idx[0];

        DType dt = determine_dtype<1>(node, partition->arrays);

        // Look up the matrix global shape from array_meta
        auto meta_it = dag_group->array_meta.find(mat_name);
        if (meta_it == dag_group->array_meta.end()) {
            CkAbort("Cross-partition MATMUL: could not find matrix metadata for name=%d", mat_name);
            return;
        }
        int mat_ndims = meta_it->second.ndims;

        auto vec_it = partition->arrays.find(vec_name);
        bool has_vec = (vec_it != partition->arrays.end() && vec_it->second->local_size() > 0);
        int local_size = has_vec ? vec_it->second->local_size() : 0;

        // Compute vec global size from array_meta
        int vec_s = 0, vec_e = 0;
        auto vec_meta_it = dag_group->array_meta.find(vec_name);
        if (vec_meta_it != dag_group->array_meta.end())
            vec_e = vec_meta_it->second.global_shape[0];

        // Get decomps for global-space translation
        ArrayDecomp<1> vec_decomp;
        if (vec_meta_it != dag_group->array_meta.end())
            vec_decomp = vec_meta_it->second.template decomp<1>();
        auto result_meta_1d = dag_group->array_meta.find(result_name);
        ArrayDecomp<1> result_decomp;
        if (result_meta_1d != dag_group->array_meta.end())
            result_decomp = result_meta_1d->second.template decomp<1>();

        if (mat_ndims == 3) {
            // --- 3D matrix path (dimension-dropped matvec) ---
            int tile_3d = array_tile(dag_group->array_meta, mat_name, 3);


            // operand_regions[0] is a 3D region, operand_regions[1] is 1D vec region
            auto* mat_region_3d = static_cast<ArrayRegion<3>*>(matmul_root->operand_regions[0]);
            auto* vec_region = static_cast<ArrayRegion<1>*>(matmul_root->operand_regions[1]);
            vec_s = vec_region->start[0]; vec_e = vec_region->stop[0];

            // Identify dropped dim (size-1 range), row dim, col dim
            int dd = -1;
            for (int d = 0; d < 3; d++) {
                if (mat_region_3d->stop[d] - mat_region_3d->start[d] == 1) {
                    dd = d; break;
                }
            }
            int rd = (dd == 0) ? 1 : 0;
            int cd = (dd <= 1) ? 2 : 1;

            int dd_s = mat_region_3d->start[dd], dd_e = mat_region_3d->stop[dd];
            int rd_s = mat_region_3d->start[rd], rd_e = mat_region_3d->stop[rd];
            int cd_s = mat_region_3d->start[cd], cd_e = mat_region_3d->stop[cd];

            // Translate AST regions to global space
            auto mat_decomp_3d = meta_it->second.template decomp<3>();
            dd_s += mat_decomp_3d.offset[dd]; dd_e += mat_decomp_3d.offset[dd];
            rd_s += mat_decomp_3d.offset[rd]; rd_e += mat_decomp_3d.offset[rd];
            cd_s += mat_decomp_3d.offset[cd]; cd_e += mat_decomp_3d.offset[cd];
            vec_s += vec_decomp.offset[0]; vec_e += vec_decomp.offset[0];

            int slice_rows = rd_e - rd_s;

            // Determine which 3D chares overlap the region
            int first_dd_chare = dd_s / tile_3d;
            int last_dd_chare = (dd_e > 0) ? (dd_e - 1) / tile_3d : 0;
            int first_row_chare = rd_s / tile_3d;
            int last_row_chare = (rd_e > 0) ? (rd_e - 1) / tile_3d : 0;
            int first_col_chare = cd_s / tile_3d;
            int last_col_chare = (cd_e > 0) ? (cd_e - 1) / tile_3d : 0;

            DBG_PRINT("[1D Chare %d] cross_matmul_3d: node=%d dd=%d rd=%d cd=%d "
                      "dd=[%d:%d] rd=[%d:%d] cd=[%d:%d] vec=[%d:%d]\n",
                      partition->index[0], node->id, dd, rd, cd,
                      dd_s, dd_e, rd_s, rd_e, cd_s, cd_e, vec_s, vec_e);

            // Phase 1: Send vector data to relevant 3D chares
            if (has_vec) {
                int my_vec_start = vec_decomp.chare_start_global(0, k);
                int my_vec_end = my_vec_start + local_size;
                int rel_start = std::max(my_vec_start, vec_s);
                int rel_end = std::min(my_vec_end, vec_e);
                if (rel_start < rel_end) {
                    int mat_col_lo = cd_s + (rel_start - vec_s);
                    int mat_col_hi = cd_s + (rel_end - vec_s);

                    int fc = mat_col_lo / tile_3d;
                    int lc = (mat_col_hi - 1) / tile_3d;

                    for (int tc = fc; tc <= lc; tc++) {
                        int chare_col_start = std::max(mat_col_lo, tc * tile_3d);
                        int chare_col_end = std::min(mat_col_hi, (tc + 1) * tile_3d);
                        int send_len = chare_col_end - chare_col_start;
                        int local_offset = (chare_col_start - cd_s + vec_s) - my_vec_start;

                        for (int tr = first_row_chare; tr <= last_row_chare; tr++) {
                            for (int tb = first_dd_chare; tb <= last_dd_chare; tb++) {
                                ChareIndex<3> target_ci;
                                target_ci.idx[dd] = tb;
                                target_ci.idx[rd] = tr;
                                target_ci.idx[cd] = tc;
                                dispatch_cross_matmul_send_vector_slice_3d(
                                    dt, partition, node, vec_name, target_ci,
                                    local_offset, send_len, chare_col_start, chare_col_end,
                                    dag_group->partition_proxy_3);
                            }
                        }
                    }
                }
            }

            // Phase 2: Wait for partial results from 3D chares
            auto r_local_3d = result_decomp.chare_region_local(nd_idx);
            int my_result_start = r_local_3d.start[0];
            int my_result_end = std::min(r_local_3d.stop[0], slice_rows);
            int total_expected = 0;
            if (my_result_start < slice_rows) {
                for (int tb = first_dd_chare; tb <= last_dd_chare; tb++) {
                    for (int tc = first_col_chare; tc <= last_col_chare; tc++) {
                        for (int tr = first_row_chare; tr <= last_row_chare; tr++) {
                            int global_row_lo = std::max(tr * tile_3d, rd_s);
                            int global_row_hi = std::min((tr + 1) * tile_3d, rd_e);
                            int result_lo = global_row_lo - rd_s;
                            int result_hi = global_row_hi - rd_s;
                            if (result_lo < my_result_end && result_hi > my_result_start)
                                total_expected++;
                        }
                    }
                }
            }

            auto pre_it = pending.find(node->id);
            int pre_arrived = 0;
            std::unordered_map<int, std::vector<RemoteBuffer<1>>> pre_buffers;
            if (pre_it != pending.end()) {
                pre_arrived = -(pre_it->second.expected_msgs);
                pre_buffers = std::move(pre_it->second.remote_buffers);
            }
            int remaining = total_expected - pre_arrived;
            pending[node->id] = {node, remaining, {}, std::move(pre_buffers)};
            DBG_PRINT("[1D Chare %d] cross_matmul_3d: total_expected=%d remaining=%d\n",
                      partition->index[0], total_expected, remaining);
            if (remaining <= 0)
                on_comm_done(node->id);

        } else {
            // --- 2D matrix path (existing) ---
            int tile_2d = array_tile(dag_group->array_meta, mat_name, 2);

            int mat_global_rows = meta_it->second.global_shape[0];
            int mat_global_cols = meta_it->second.global_shape[1];

            int mat_rs = 0, mat_re = mat_global_rows;
            int mat_cs = 0, mat_ce = mat_global_cols;
            if (matmul_root->operand_regions.size() >= 2) {
                auto* mat_region = static_cast<ArrayRegion<2>*>(matmul_root->operand_regions[0]);
                auto* vec_region = static_cast<ArrayRegion<1>*>(matmul_root->operand_regions[1]);
                mat_rs = mat_region->start[0]; mat_re = mat_region->stop[0];
                mat_cs = mat_region->start[1]; mat_ce = mat_region->stop[1];
                vec_s = vec_region->start[0];  vec_e = vec_region->stop[0];
            }

            // Translate AST regions to global space
            auto mat_decomp_2d = meta_it->second.template decomp<2>();
            mat_rs += mat_decomp_2d.offset[0]; mat_re += mat_decomp_2d.offset[0];
            mat_cs += mat_decomp_2d.offset[1]; mat_ce += mat_decomp_2d.offset[1];
            vec_s += vec_decomp.offset[0]; vec_e += vec_decomp.offset[0];

            int slice_rows = mat_re - mat_rs;

            int first_row_chare = mat_rs / tile_2d;
            int last_row_chare = (mat_re > 0) ? (mat_re - 1) / tile_2d : 0;
            int first_col_chare = mat_cs / tile_2d;
            int last_col_chare = (mat_ce > 0) ? (mat_ce - 1) / tile_2d : 0;

            DBG_PRINT("[1D Chare %d] cross_matmul: node=%d vec=%d mat=%d result=%d "
                      "slice=[%d:%d,%d:%d] vec=[%d:%d] has_vec=%d\n",
                      partition->index[0], node->id, vec_name, mat_name, result_name,
                      mat_rs, mat_re, mat_cs, mat_ce, vec_s, vec_e, (int)has_vec);

            if (has_vec) {
                int my_vec_start = vec_decomp.chare_start_global(0, k);
                int my_vec_end = my_vec_start + local_size;
                int rel_start = std::max(my_vec_start, vec_s);
                int rel_end = std::min(my_vec_end, vec_e);
                if (rel_start < rel_end) {
                    int mat_col_lo = mat_cs + (rel_start - vec_s);
                    int mat_col_hi = mat_cs + (rel_end - vec_s);

                    int fc = mat_col_lo / tile_2d;
                    int lc = (mat_col_hi - 1) / tile_2d;
                    for (int tc = fc; tc <= lc; tc++) {
                        int chare_col_start = std::max(mat_col_lo, tc * tile_2d);
                        int chare_col_end = std::min(mat_col_hi, (tc + 1) * tile_2d);
                        int send_len = chare_col_end - chare_col_start;
                        int local_offset = (chare_col_start - mat_cs + vec_s) - my_vec_start;

                        for (int tr = first_row_chare; tr <= last_row_chare; tr++) {
                            ChareIndex<2> target_ci;
                            target_ci.idx[0] = tr;
                            target_ci.idx[1] = tc;
                            dispatch_cross_matmul_send_vector_slice(
                                dt, partition, node, vec_name, target_ci,
                                local_offset, send_len, chare_col_start, chare_col_end,
                                dag_group->partition_proxy_2);
                        }
                    }
                }
            }

            auto r_local_2d = result_decomp.chare_region_local(nd_idx);
            int my_result_start = r_local_2d.start[0];
            int my_result_end = std::min(r_local_2d.stop[0], slice_rows);
            int total_expected = 0;
            if (my_result_start < slice_rows) {
                for (int tc = first_col_chare; tc <= last_col_chare; tc++) {
                    for (int tr = first_row_chare; tr <= last_row_chare; tr++) {
                        int global_row_lo = std::max(tr * tile_2d, mat_rs);
                        int global_row_hi = std::min((tr + 1) * tile_2d, mat_re);
                        int result_lo = global_row_lo - mat_rs;
                        int result_hi = global_row_hi - mat_rs;
                        if (result_lo < my_result_end && result_hi > my_result_start)
                            total_expected++;
                    }
                }
            }

            auto pre_it = pending.find(node->id);
            int pre_arrived = 0;
            std::unordered_map<int, std::vector<RemoteBuffer<1>>> pre_buffers;
            if (pre_it != pending.end()) {
                pre_arrived = -(pre_it->second.expected_msgs);
                pre_buffers = std::move(pre_it->second.remote_buffers);
            }
            int remaining = total_expected - pre_arrived;
            pending[node->id] = {node, remaining, {}, std::move(pre_buffers)};
            DBG_PRINT("[1D Chare %d] cross_matmul: node=%d total_expected=%d pre_arrived=%d remaining=%d\n",
                      partition->index[0], node->id, total_expected, pre_arrived, remaining);
            if (remaining <= 0)
                on_comm_done(node->id);
        }

    } else if constexpr (N == 2) {
        int tile_1d = array_tile(dag_group->array_meta, vec_name, 1);

        auto nd_idx = partition->nd_index();
        int r = nd_idx[0];
        int c = nd_idx[1];

        DType dt = determine_dtype<N>(node, partition->arrays);

        auto mat_it = partition->arrays.find(mat_name);
        if (mat_it == partition->arrays.end()) {
            node_finished(node->id);
            return;
        }
        int tile_2d = mat_it->second->decomp.tile;
        int mat_global_rows = mat_it->second->global_shape[0];
        int mat_global_cols = mat_it->second->global_shape[1];

        // Extract slice regions if present
        int mat_rs = 0, mat_re = mat_global_rows;
        int mat_cs = 0, mat_ce = mat_global_cols;
        int vec_s = 0, vec_e = mat_ce - mat_cs; // default: full range
        if (matmul_root->operand_regions.size() >= 2) {
            auto* mat_region = static_cast<ArrayRegion<2>*>(matmul_root->operand_regions[0]);
            auto* vec_region = static_cast<ArrayRegion<1>*>(matmul_root->operand_regions[1]);
            mat_rs = mat_region->start[0]; mat_re = mat_region->stop[0];
            mat_cs = mat_region->start[1]; mat_ce = mat_region->stop[1];
            vec_s = vec_region->start[0];  vec_e = vec_region->stop[0];
        }

        // Translate AST regions to global space
        auto mat_decomp = mat_it->second->decomp;
        mat_rs += mat_decomp.offset[0]; mat_re += mat_decomp.offset[0];
        mat_cs += mat_decomp.offset[1]; mat_ce += mat_decomp.offset[1];
        auto vec_decomp_1d = dag_group->array_meta[vec_name].template decomp<1>();
        vec_s += vec_decomp_1d.offset[0]; vec_e += vec_decomp_1d.offset[0];

        // Check if this 2D chare's tile overlaps the matrix slice
        auto mat_chare = mat_decomp.chare_region_global(nd_idx);
        int row_lo = std::max(mat_chare.start[0], mat_rs);
        int row_hi = std::min(mat_chare.stop[0], mat_re);
        int col_lo = std::max(mat_chare.start[1], mat_cs);
        int col_hi = std::min(mat_chare.stop[1], mat_ce);
        if (row_lo >= row_hi || col_lo >= col_hi) {
            // This chare is outside the slice range
            node_finished(node->id);
            return;
        }

        // Count how many vector messages to expect.
        // Vector elements for columns [col_lo, col_hi) come from V indices
        // [vec_s + (col_lo - mat_cs), vec_s + (col_hi - mat_cs)).
        int vec_idx_start = vec_s + (col_lo - mat_cs);
        int vec_idx_end = vec_s + (col_hi - mat_cs);
        int first_vec_chare = vec_idx_start / tile_1d;
        int last_vec_chare = (vec_idx_end - 1) / tile_1d;
        int vec_msgs = last_vec_chare - first_vec_chare + 1;

        auto pre_it = pending.find(node->id);
        int pre_arrived = 0;
        std::unordered_map<int, std::vector<RemoteBuffer<N>>> pre_buffers;
        if (pre_it != pending.end()) {
            pre_arrived = -(pre_it->second.expected_msgs);
            pre_buffers = std::move(pre_it->second.remote_buffers);
        }
        int remaining = vec_msgs - pre_arrived;
        pending[node->id] = {node, remaining, {}, std::move(pre_buffers)};
        if (pre_arrived > 0 || remaining <= 0) {
            DBG_PRINT("[2D Chare %d] matmul node=%d: pre_arrived=%d remaining=%d vec_msgs=%d\n",
                      partition->index[0], node->id, pre_arrived, remaining, vec_msgs);
        }

        if (remaining <= 0)
            on_comm_done(node->id);

    } else if constexpr (N == 3) {
        // --- 3D partition role in cross-partition matmul (dimension-dropped) ---
        int tile_1d = array_tile(dag_group->array_meta, vec_name, 1);

        auto nd_idx = partition->nd_index();

        DType dt = determine_dtype<N>(node, partition->arrays);

        auto mat_it = partition->arrays.find(mat_name);
        if (mat_it == partition->arrays.end()) {
            node_finished(node->id);
            return;
        }
        int tile_3d = mat_it->second->decomp.tile;

        // operand_regions[0] is a 3D region
        auto* mat_region_3d = static_cast<ArrayRegion<3>*>(matmul_root->operand_regions[0]);
        auto* vec_region = static_cast<ArrayRegion<1>*>(matmul_root->operand_regions[1]);

        // Identify dropped dim (size-1 range), row dim, col dim
        int dd = -1;
        for (int d = 0; d < 3; d++) {
            if (mat_region_3d->stop[d] - mat_region_3d->start[d] == 1) {
                dd = d; break;
            }
        }
        int rd = (dd == 0) ? 1 : 0;
        int cd = (dd <= 1) ? 2 : 1;

        int dd_s = mat_region_3d->start[dd], dd_e = mat_region_3d->stop[dd];
        int rd_s = mat_region_3d->start[rd], rd_e = mat_region_3d->stop[rd];
        int cd_s = mat_region_3d->start[cd], cd_e = mat_region_3d->stop[cd];
        int vec_s = vec_region->start[0], vec_e = vec_region->stop[0];

        // Translate AST regions to global space
        auto mat_decomp = mat_it->second->decomp;
        dd_s += mat_decomp.offset[dd]; dd_e += mat_decomp.offset[dd];
        rd_s += mat_decomp.offset[rd]; rd_e += mat_decomp.offset[rd];
        cd_s += mat_decomp.offset[cd]; cd_e += mat_decomp.offset[cd];
        auto vec_decomp_1d = dag_group->array_meta[vec_name].template decomp<1>();
        vec_s += vec_decomp_1d.offset[0]; vec_e += vec_decomp_1d.offset[0];

        // Check if this 3D chare overlaps the region in ALL dims
        auto mat_chare = mat_decomp.chare_region_global(nd_idx);
        int dd_lo = std::max(mat_chare.start[dd], dd_s);
        int dd_hi = std::min(mat_chare.stop[dd], dd_e);
        int row_lo = std::max(mat_chare.start[rd], rd_s);
        int row_hi = std::min(mat_chare.stop[rd], rd_e);
        int col_lo = std::max(mat_chare.start[cd], cd_s);
        int col_hi = std::min(mat_chare.stop[cd], cd_e);
        if (dd_lo >= dd_hi || row_lo >= row_hi || col_lo >= col_hi) {
            node_finished(node->id);
            return;
        }

        // Count how many vector messages to expect
        int vec_idx_start = vec_s + (col_lo - cd_s);
        int vec_idx_end = vec_s + (col_hi - cd_s);
        int first_vec_chare = vec_idx_start / tile_1d;
        int last_vec_chare = (vec_idx_end - 1) / tile_1d;
        int vec_msgs = last_vec_chare - first_vec_chare + 1;

        auto pre_it = pending.find(node->id);
        int pre_arrived = 0;
        std::unordered_map<int, std::vector<RemoteBuffer<N>>> pre_buffers;
        if (pre_it != pending.end()) {
            pre_arrived = -(pre_it->second.expected_msgs);
            pre_buffers = std::move(pre_it->second.remote_buffers);
        }
        int remaining = vec_msgs - pre_arrived;
        pending[node->id] = {node, remaining, {}, std::move(pre_buffers)};
        DBG_PRINT("[3D Chare %d] matmul node=%d: dd=%d rd=%d cd=%d "
                  "dd_lo=%d dd_hi=%d row_lo=%d row_hi=%d col_lo=%d col_hi=%d "
                  "vec_msgs=%d remaining=%d\n",
                  partition->index[0], node->id, dd, rd, cd,
                  dd_lo, dd_hi, row_lo, row_hi, col_lo, col_hi,
                  vec_msgs, remaining);

        if (remaining <= 0)
            on_comm_done(node->id);

    } else {
        CkAbort("MATMUL only supported for 1D, 2D, and 3D partitions (N=%d)", N);
    }
}

// ---------------------------------------------------------------------------
// SUMMA Matrix-Matrix Multiply (MATMATMUL)
// ---------------------------------------------------------------------------

/// Send a 2D sub-block from one 2D chare to another for SUMMA matmatmul.
/// input_index: 0 for A panels, 1 for B panels
/// Region encoding:
///   dim 0: [k_start, k_end) — the k-range this panel covers
///   dim 1: [c_pos, c_pos + size) — C-space row range (A panels) or col range (B panels)
/// c_pos: the C-space row position (for A panels) or col position (for B panels)
template <typename T>
static void cross_matmatmul_send_panel_2d(
    PartitionImpl<2>* partition, DAGNode* node, int arr_name, int input_index,
    const ChareIndex<2>& target_ci, T* local_data, int local_cols,
    int row_offset, int col_offset, int sub_rows, int sub_cols,
    int k_start, int k_end, int c_pos,
    CProxy_Partition2D& proxy_2d) {
    int64_t send_count = (int64_t)sub_rows * sub_cols;
    int64_t byte_size = send_count * sizeof(T);

#ifndef NDEBUG
    partition->comm_bytes_sent += byte_size;
#endif

    // For A panels (input_index=0): dim 1 = [c_pos, c_pos + sub_rows) = C-space row range
    // For B panels (input_index=1): dim 1 = [c_pos, c_pos + sub_cols) = C-space col range
    int region_data[2 * 3];
    region_data[0] = k_start;
    region_data[1] = k_end;
    region_data[2] = 1;
    region_data[3] = c_pos;
    region_data[4] = c_pos + ((input_index == 0) ? sub_rows : sub_cols);
    region_data[5] = 1;

#ifndef USE_KOKKOS
    // Pack sub-block into contiguous send buffer
    T* send_buf = new T[send_count];
    for (int r = 0; r < sub_rows; r++)
        memcpy(send_buf + r * sub_cols,
               local_data + (row_offset + r) * local_cols + col_offset,
               sub_cols * sizeof(T));
    proxy_at<2>(proxy_2d, target_ci)
        .receive_data(node->id, input_index, arr_name, 2, region_data, byte_size,
                      reinterpret_cast<char*>(send_buf));
    delete[] send_buf;
#else
    T* send_buf = static_cast<T*>(Kokkos::kokkos_malloc<DeviceSpace>(byte_size));
    T* src = local_data + row_offset * local_cols + col_offset;
    int lc = local_cols;
    int sc = sub_cols;
    Kokkos::parallel_for(
        CT_COMM_POLICY(partition, sub_rows),
        KOKKOS_LAMBDA(int r) {
            for (int c = 0; c < sc; c++)
                send_buf[r * sc + c] = src[r * lc + c];
        });
    device_pack_send<2>(partition, proxy_2d, target_ci, node->id, input_index, arr_name,
                        region_data, byte_size, send_buf);
#endif
}

/// Dispatch cross_matmatmul_send_panel_2d by dtype.
static void dispatch_cross_matmatmul_send_panel_2d(
    DType dt, PartitionImpl<2>* partition, DAGNode* node, int arr_name, int input_index,
    const ChareIndex<2>& target_ci, int local_cols,
    int row_offset, int col_offset, int sub_rows, int sub_cols,
    int k_start, int k_end, int c_pos,
    CProxy_Partition2D& proxy_2d) {
    // Under USE_KOKKOS the packing lambda operates on device, so pass device_data_ptr.
    // Under CPU-only, pass host data_ptr.
    auto arr_ptr = [&](auto* dummy) -> void* {
        (void)dummy;
#ifdef USE_KOKKOS
        return partition->arrays[arr_name]->device_data_ptr();
#else
        return partition->arrays[arr_name]->data_ptr();
#endif
    };
    switch (dt) {
    case DType::FLOAT32:
        cross_matmatmul_send_panel_2d<float>(
            partition, node, arr_name, input_index, target_ci,
            static_cast<float*>(arr_ptr((float*)nullptr)),
            local_cols, row_offset, col_offset, sub_rows, sub_cols,
            k_start, k_end, c_pos, proxy_2d);
        break;
    case DType::FLOAT64:
        cross_matmatmul_send_panel_2d<double>(
            partition, node, arr_name, input_index, target_ci,
            static_cast<double*>(arr_ptr((double*)nullptr)),
            local_cols, row_offset, col_offset, sub_rows, sub_cols,
            k_start, k_end, c_pos, proxy_2d);
        break;
    case DType::INT32:
        cross_matmatmul_send_panel_2d<int32_t>(
            partition, node, arr_name, input_index, target_ci,
            static_cast<int32_t*>(arr_ptr((int32_t*)nullptr)),
            local_cols, row_offset, col_offset, sub_rows, sub_cols,
            k_start, k_end, c_pos, proxy_2d);
        break;
    case DType::INT64:
        cross_matmatmul_send_panel_2d<int64_t>(
            partition, node, arr_name, input_index, target_ci,
            static_cast<int64_t*>(arr_ptr((int64_t*)nullptr)),
            local_cols, row_offset, col_offset, sub_rows, sub_cols,
            k_start, k_end, c_pos, proxy_2d);
        break;
    }
}

template <int N>
void ArrayDAGExecutorND<N>::execute_matmatmul_node(DAGNode* node) {
    ASTNode* mm_root = nullptr;
    for (ASTNode* root : node->ast->roots) {
        if (static_cast<Opcode>(root->opcode) == Opcode::MATMATMUL) {
            mm_root = root;
            break;
        }
    }
    int a_name = mm_root->operands[0]->result_name;
    int b_name = mm_root->operands[1]->result_name;
    int result_name = mm_root->result_name;

    auto* dag_group = static_cast<ArrayDAGGroup*>(group);

    if constexpr (N == 2) {
        int tile_2d = array_tile(dag_group->array_meta, a_name, 2);

        auto nd_idx = partition->nd_index();
        int r = nd_idx[0];
        int c = nd_idx[1];

        DType dt = determine_dtype<N>(node, partition->arrays);

        // Look up operand metadata
        auto a_meta_it = dag_group->array_meta.find(a_name);
        auto b_meta_it = dag_group->array_meta.find(b_name);
        auto c_meta_it = dag_group->array_meta.find(result_name);

        // Extract A and B slice regions (or full range)
        int a_rs = 0, a_re, a_cs = 0, a_ce;
        int b_rs = 0, b_re, b_cs = 0, b_ce;
        if (a_meta_it != dag_group->array_meta.end()) {
            a_re = a_meta_it->second.global_shape[0];
            a_ce = a_meta_it->second.global_shape[1];
        } else {
            CkAbort("MATMATMUL: could not find A metadata for name=%d", a_name);
            return;
        }
        if (b_meta_it != dag_group->array_meta.end()) {
            b_re = b_meta_it->second.global_shape[0];
            b_ce = b_meta_it->second.global_shape[1];
        } else {
            CkAbort("MATMATMUL: could not find B metadata for name=%d", b_name);
            return;
        }

        if (mm_root->operand_regions.size() >= 2) {
            // Extract effective 2D ranges from operand regions.
            // Operands may be 3D (dimension-dropped), so check ndims.
            int a_ndims = a_meta_it->second.ndims;
            int b_ndims = b_meta_it->second.ndims;

            if (a_ndims == 3) {
                auto* ar = static_cast<ArrayRegion<3>*>(mm_root->operand_regions[0]);
                int dd = -1;
                for (int d = 0; d < 3; d++)
                    if (ar->stop[d] - ar->start[d] == 1) { dd = d; break; }
                int rd = (dd == 0) ? 1 : 0;
                int cd = (dd <= 1) ? 2 : 1;
                a_rs = ar->start[rd]; a_re = ar->stop[rd];
                a_cs = ar->start[cd]; a_ce = ar->stop[cd];
            } else {
                auto* ar = static_cast<ArrayRegion<2>*>(mm_root->operand_regions[0]);
                a_rs = ar->start[0]; a_re = ar->stop[0];
                a_cs = ar->start[1]; a_ce = ar->stop[1];
            }

            if (b_ndims == 3) {
                auto* br = static_cast<ArrayRegion<3>*>(mm_root->operand_regions[1]);
                int dd = -1;
                for (int d = 0; d < 3; d++)
                    if (br->stop[d] - br->start[d] == 1) { dd = d; break; }
                int rd = (dd == 0) ? 1 : 0;
                int cd = (dd <= 1) ? 2 : 1;
                b_rs = br->start[rd]; b_re = br->stop[rd];
                b_cs = br->start[cd]; b_ce = br->stop[cd];
            } else {
                auto* br = static_cast<ArrayRegion<2>*>(mm_root->operand_regions[1]);
                b_rs = br->start[0]; b_re = br->stop[0];
                b_cs = br->start[1]; b_ce = br->stop[1];
            }
        }

        // Translate A/B regions to global space
        if (mm_root->operand_regions.size() >= 2) {
            int a_ndims = a_meta_it->second.ndims;
            if (a_ndims == 3) {
                auto ad = a_meta_it->second.template decomp<3>();
                auto* ar3 = static_cast<ArrayRegion<3>*>(mm_root->operand_regions[0]);
                int dd = -1;
                for (int d = 0; d < 3; d++)
                    if (ar3->stop[d] - ar3->start[d] == 1) { dd = d; break; }
                int rd = (dd == 0) ? 1 : 0, cd = (dd <= 1) ? 2 : 1;
                a_rs += ad.offset[rd]; a_re += ad.offset[rd];
                a_cs += ad.offset[cd]; a_ce += ad.offset[cd];
            } else {
                auto ad = a_meta_it->second.template decomp<2>();
                a_rs += ad.offset[0]; a_re += ad.offset[0];
                a_cs += ad.offset[1]; a_ce += ad.offset[1];
            }
            int b_ndims = b_meta_it->second.ndims;
            if (b_ndims == 3) {
                auto bd = b_meta_it->second.template decomp<3>();
                auto* br3 = static_cast<ArrayRegion<3>*>(mm_root->operand_regions[1]);
                int dd = -1;
                for (int d = 0; d < 3; d++)
                    if (br3->stop[d] - br3->start[d] == 1) { dd = d; break; }
                int rd = (dd == 0) ? 1 : 0, cd = (dd <= 1) ? 2 : 1;
                b_rs += bd.offset[rd]; b_re += bd.offset[rd];
                b_cs += bd.offset[cd]; b_ce += bd.offset[cd];
            } else {
                auto bd = b_meta_it->second.template decomp<2>();
                b_rs += bd.offset[0]; b_re += bd.offset[0];
                b_cs += bd.offset[1]; b_ce += bd.offset[1];
            }
        } else {
            // No operand regions — both A and B are 2D
            auto ad = a_meta_it->second.template decomp<2>();
            a_rs += ad.offset[0]; a_re += ad.offset[0];
            a_cs += ad.offset[1]; a_ce += ad.offset[1];
            auto bd = b_meta_it->second.template decomp<2>();
            b_rs += bd.offset[0]; b_re += bd.offset[0];
            b_cs += bd.offset[1]; b_ce += bd.offset[1];
        }

        int M = a_re - a_rs;        // result rows
        int K = a_ce - a_cs;        // shared dimension
        int N_cols = b_ce - b_cs;   // result cols

        // C result range in global coords
        ArrayDecomp<2> c_decomp;
        if (c_meta_it != dag_group->array_meta.end())
            c_decomp = c_meta_it->second.template decomp<2>();
        int c_rs = c_decomp.offset[0], c_re = c_decomp.offset[0] + M;
        int c_cs = c_decomp.offset[1], c_ce = c_decomp.offset[1] + N_cols;

        // Chare ranges for A, B, and C
        int first_a_row_chare = a_rs / tile_2d;
        int last_a_row_chare = (a_re > 0) ? (a_re - 1) / tile_2d : 0;
        int first_a_col_chare = a_cs / tile_2d;
        int last_a_col_chare = (a_ce > 0) ? (a_ce - 1) / tile_2d : 0;
        int first_b_row_chare = b_rs / tile_2d;
        int last_b_row_chare = (b_re > 0) ? (b_re - 1) / tile_2d : 0;
        int first_b_col_chare = b_cs / tile_2d;
        int last_b_col_chare = (b_ce > 0) ? (b_ce - 1) / tile_2d : 0;
        int first_c_row_chare = c_rs / tile_2d;
        int last_c_row_chare = (c_re > 0) ? (c_re - 1) / tile_2d : 0;
        int first_c_col_chare = c_cs / tile_2d;
        int last_c_col_chare = (c_ce > 0) ? (c_ce - 1) / tile_2d : 0;

        // --- Phase 1: Send A panels ---
        // Chare (r, c) that has A data sends to chares (r, j) for all j in C's col range
        auto a_it = partition->arrays.find(a_name);
        if (a_it != partition->arrays.end() && a_it->second->local_size() > 0) {
            int local_rows = a_it->second->region.size(0);
            int local_cols = a_it->second->region.size(1);

            // Overlap of this chare's tile with A's slice region
            auto a_decomp = a_it->second->decomp;
            auto a_chare = a_decomp.chare_region_global(nd_idx);
            int a_row_lo = std::max(a_chare.start[0], a_rs);
            int a_row_hi = std::min(a_chare.stop[0], a_re);
            int a_col_lo = std::max(a_chare.start[1], a_cs);
            int a_col_hi = std::min(a_chare.stop[1], a_ce);

            if (a_row_lo < a_row_hi && a_col_lo < a_col_hi) {
                int sub_rows = a_row_hi - a_row_lo;
                int sub_cols = a_col_hi - a_col_lo;
                int row_offset = a_row_lo - a_chare.start[0];
                int col_offset = a_col_lo - a_chare.start[1];

                // k-range in K-space: [a_col_lo - a_cs, a_col_hi - a_cs)
                int k_start = a_col_lo - a_cs;
                int k_end = a_col_hi - a_cs;

                // Which result row chare does this A block map to?
                // A rows [a_row_lo, a_row_hi) map to C rows in global C space
                int c_row_start = c_rs + (a_row_lo - a_rs);
                int c_row_end = c_rs + (a_row_hi - a_rs);
                int first_dest_row_chare = c_row_start / tile_2d;
                int last_dest_row_chare = (c_row_end - 1) / tile_2d;

#ifndef USE_KOKKOS
                a_it->second->copyToHost();
#endif
                for (int tj = first_c_col_chare; tj <= last_c_col_chare; tj++) {
                    for (int ti = first_dest_row_chare; ti <= last_dest_row_chare; ti++) {
                        ChareIndex<2> target_ci;
                        target_ci.idx[0] = ti;
                        target_ci.idx[1] = tj;

                        // Compute the sub-block of A rows that map to this dest row chare
                        int dest_row_start = std::max(c_row_start, ti * tile_2d);
                        int dest_row_end = std::min(c_row_end, (ti + 1) * tile_2d);
                        int send_rows = dest_row_end - dest_row_start;
                        int send_row_offset = row_offset + (dest_row_start - c_row_start);

                        dispatch_cross_matmatmul_send_panel_2d(
                            dt, partition, node, a_name, /*input_index=*/0,
                            target_ci, local_cols,
                            send_row_offset, col_offset, send_rows, sub_cols,
                            k_start, k_end, /*c_pos=*/dest_row_start,
                            dag_group->partition_proxy_2);
                    }
                }
            }
        }

        // --- Phase 2: Send B panels ---
        // Chare (r, c) that has B data sends to chares (i, c) for all i in C's row range
        auto b_it = partition->arrays.find(b_name);
        if (b_it != partition->arrays.end() && b_it->second->local_size() > 0) {
            int local_rows = b_it->second->region.size(0);
            int local_cols = b_it->second->region.size(1);

            auto b_decomp = b_it->second->decomp;
            auto b_chare = b_decomp.chare_region_global(nd_idx);
            int b_row_lo = std::max(b_chare.start[0], b_rs);
            int b_row_hi = std::min(b_chare.stop[0], b_re);
            int b_col_lo = std::max(b_chare.start[1], b_cs);
            int b_col_hi = std::min(b_chare.stop[1], b_ce);

            if (b_row_lo < b_row_hi && b_col_lo < b_col_hi) {
                int sub_rows = b_row_hi - b_row_lo;
                int sub_cols = b_col_hi - b_col_lo;
                int row_offset = b_row_lo - b_chare.start[0];
                int col_offset = b_col_lo - b_chare.start[1];

                // k-range in K-space: [b_row_lo - b_rs, b_row_hi - b_rs)
                int k_start = b_row_lo - b_rs;
                int k_end = b_row_hi - b_rs;

                // B cols [b_col_lo, b_col_hi) map to C cols in global C space
                int c_col_start = c_cs + (b_col_lo - b_cs);
                int c_col_end = c_cs + (b_col_hi - b_cs);
                int first_dest_col_chare = c_col_start / tile_2d;
                int last_dest_col_chare = (c_col_end - 1) / tile_2d;

#ifndef USE_KOKKOS
                b_it->second->copyToHost();
#endif
                for (int ti = first_c_row_chare; ti <= last_c_row_chare; ti++) {
                    for (int tj = first_dest_col_chare; tj <= last_dest_col_chare; tj++) {
                        ChareIndex<2> target_ci;
                        target_ci.idx[0] = ti;
                        target_ci.idx[1] = tj;

                        // Compute the sub-block of B cols that map to this dest col chare
                        int dest_col_start = std::max(c_col_start, tj * tile_2d);
                        int dest_col_end = std::min(c_col_end, (tj + 1) * tile_2d);
                        int send_cols = dest_col_end - dest_col_start;
                        int send_col_offset = col_offset + (dest_col_start - c_col_start);

                        dispatch_cross_matmatmul_send_panel_2d(
                            dt, partition, node, b_name, /*input_index=*/1,
                            target_ci, local_cols,
                            row_offset, send_col_offset, sub_rows, send_cols,
                            k_start, k_end, /*c_pos=*/dest_col_start,
                            dag_group->partition_proxy_2);
                    }
                }
            }
        }

        // --- Phase 3: Allocate C, count expected messages, enable incremental ---
        auto c_chare = c_decomp.chare_region_global(nd_idx);
        int my_c_row_lo = std::max(c_chare.start[0], c_rs);
        int my_c_row_hi = std::min(c_chare.stop[0], c_re);
        int my_c_col_lo = std::max(c_chare.start[1], c_cs);
        int my_c_col_hi = std::min(c_chare.stop[1], c_ce);

        int total_expected = 0;
        if (my_c_row_lo < my_c_row_hi && my_c_col_lo < my_c_col_hi) {
            int sub_rows = my_c_row_hi - my_c_row_lo;
            int sub_cols = my_c_col_hi - my_c_col_lo;

            // Allocate C upfront so incremental receives can accumulate into it
            if (partition->arrays.find(result_name) == partition->arrays.end()) {
                std::array<int, 2> out_start = {0, 0};
                std::array<int, 2> out_stop = {sub_rows, sub_cols};
                std::array<int, 2> out_step = {1, 1};
                std::array<int, 2> out_gs = {M, N_cols};
                ArrayRegion<2> out_region(out_start, out_stop, out_step);
                ArrayDecomp<2> out_decomp = dag_group->array_meta[result_name].template decomp<2>();
                partition->arrays[result_name] =
                    partition->allocate_or_reuse(out_region, out_gs, result_name, dt, out_decomp);
            }

            // Count A panels: how many A column chares cover the K dimension
            int a_k_chares = 0;
            for (int tk = first_a_col_chare; tk <= last_a_col_chare; tk++) {
                int a_row_for_c_lo = a_rs + my_c_row_lo;
                int a_row_for_c_hi = a_rs + my_c_row_hi;
                int fa = a_row_for_c_lo / tile_2d;
                int la = (a_row_for_c_hi - 1) / tile_2d;
                a_k_chares += (la - fa + 1);
            }

            // Count B panels: how many B row chares cover the K dimension
            int b_k_chares = 0;
            for (int tk = first_b_row_chare; tk <= last_b_row_chare; tk++) {
                int b_col_for_c_lo = b_cs + my_c_col_lo;
                int b_col_for_c_hi = b_cs + my_c_col_hi;
                int fb = b_col_for_c_lo / tile_2d;
                int lb = (b_col_for_c_hi - 1) / tile_2d;
                b_k_chares += (lb - fb + 1);
            }

            total_expected = a_k_chares + b_k_chares;
        }

        auto pre_it = pending.find(node->id);
        int pre_arrived = 0;
        std::unordered_map<int, std::vector<RemoteBuffer<2>>> pre_buffers;
        if (pre_it != pending.end()) {
            pre_arrived = -(pre_it->second.expected_msgs);
            pre_buffers = std::move(pre_it->second.remote_buffers);
        }
        int remaining = total_expected - pre_arrived;
        pending[node->id] = {node, remaining, {}, std::move(pre_buffers), /*incremental=*/true};

        DBG_PRINT("[2D Chare %d] matmatmul node=%d: A=[%d:%d,%d:%d] B=[%d:%d,%d:%d] "
                  "C_tile=[%d:%d,%d:%d] total_expected=%d remaining=%d\n",
                  partition->index[0], node->id,
                  a_rs, a_re, a_cs, a_ce, b_rs, b_re, b_cs, b_ce,
                  my_c_row_lo, my_c_row_hi, my_c_col_lo, my_c_col_hi,
                  total_expected, remaining);

        // Process any pre-arrived panels that came before incremental mode was set
        if (pre_arrived > 0) {
            PendingComm<2>& comm = pending[node->id];
            // For each pre-arrived A panel, compute against all pre-arrived B panels
            auto a_it2 = comm.remote_buffers.find(0);
            auto b_it2 = comm.remote_buffers.find(1);
            if (a_it2 != comm.remote_buffers.end() && b_it2 != comm.remote_buffers.end()) {
                for (size_t ai = 0; ai < a_it2->second.size(); ai++) {
                    // Temporarily make this the "last" A panel and compute
                    // against all B panels using on_matmatmul_receive logic
                    // We can just call it directly since the panel is already in place
                }
                // Simpler: just iterate all pairs directly
                auto& a_bufs = a_it2->second;
                auto& b_bufs = b_it2->second;
                auto arr_it = partition->arrays.find(result_name);
                if (arr_it != partition->arrays.end()) {
                    auto nd_idx2 = partition->nd_index();
                    int c_row_lo2 = c_decomp.chare_start_global(0, nd_idx2[0]);
                    int c_col_lo2 = c_decomp.chare_start_global(1, nd_idx2[1]);
                    int sub_rows2 = arr_it->second->region.size(0);
                    int sub_cols2 = arr_it->second->region.size(1);

                    auto compute_pre = [&](auto* dummy) {
                        using T = std::remove_pointer_t<decltype(dummy)>;
                        T* c_data = static_cast<T*>(arr_it->second->data_ptr());
                        using RMat = Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>;
                        using CStride = Eigen::Stride<Eigen::Dynamic, 1>;
                        for (auto& a_buf : a_bufs) {
                            for (auto& b_buf : b_bufs) {
                                int a_k_s = a_buf.region.start[0], a_k_e = a_buf.region.stop[0];
                                int b_k_s = b_buf.region.start[0], b_k_e = b_buf.region.stop[0];
                                int k_lo = std::max(a_k_s, b_k_s);
                                int k_hi = std::min(a_k_e, b_k_e);
                                if (k_lo >= k_hi) continue;
                                int a_rows = a_buf.region.stop[1] - a_buf.region.start[1];
                                int a_k_size = a_k_e - a_k_s;
                                int b_cols = b_buf.region.stop[1] - b_buf.region.start[1];
                                int c_lr = a_buf.region.start[1] - c_row_lo2;
                                int c_lc = b_buf.region.start[1] - c_col_lo2;
                                T* a_data = reinterpret_cast<T*>(a_buf.data);
                                T* b_data = reinterpret_cast<T*>(b_buf.data);
                                T* c_sub = c_data + c_lr * sub_cols2 + c_lc;
                                int k_size = k_hi - k_lo;
                                int a_co = k_lo - a_k_s;
                                int b_ro = k_lo - b_k_s;
                                Eigen::Map<RMat, 0, CStride>
                                    A_map(a_data + a_co, a_rows, k_size, CStride(a_k_size, 1));
                                Eigen::Map<RMat, 0, CStride>
                                    B_map(b_data + b_ro * b_cols, k_size, b_cols, CStride(b_cols, 1));
                                Eigen::Map<RMat, 0, CStride>
                                    C_map(c_sub, a_rows, b_cols, CStride(sub_cols2, 1));
                                C_map.noalias() += A_map * B_map;
                            }
                        }
                    };
                    switch (dt) {
                    case DType::FLOAT32: { float* d = nullptr; compute_pre(d); break; }
                    case DType::FLOAT64: { double* d = nullptr; compute_pre(d); break; }
                    case DType::INT32: { int32_t* d = nullptr; compute_pre(d); break; }
                    case DType::INT64: { int64_t* d = nullptr; compute_pre(d); break; }
                    }
                }
            }
        }

        if (remaining <= 0)
            on_comm_done(node->id);

    } else if constexpr (N == 3) {
        // 3D partition: one operand is a dimension-dropped 3D array.
        // Extract the 2D sub-block and send as A or B panels to 2D C chares.
        // Use the first 3D operand's tile from array_meta
        int tile_3d = array_tile(dag_group->array_meta, a_name, 3);


        auto nd_idx = partition->nd_index();

        DType dt = determine_dtype<N>(node, partition->arrays);
        int tile_2d = array_tile(dag_group->array_meta, result_name, 2);


        // Determine which operand (0=A, 1=B) is the 3D one
        for (int op_idx = 0; op_idx < 2; op_idx++) {
            int op_name = mm_root->operands[op_idx]->result_name;
            auto op_it = partition->arrays.find(op_name);
            if (op_it == partition->arrays.end() || op_it->second->local_size() <= 0)
                continue;

            // Get the 3D region for this operand
            if (mm_root->operand_regions.size() < 2) continue;
            auto* r3d = static_cast<ArrayRegion<3>*>(mm_root->operand_regions[op_idx]);

            // Identify dropped dim (singleton), row dim, col dim
            int dd = -1;
            for (int d = 0; d < 3; d++) {
                if (r3d->stop[d] - r3d->start[d] == 1) { dd = d; break; }
            }
            if (dd < 0) continue; // not a dim-dropped operand
            int rd = (dd == 0) ? 1 : 0;
            int cd = (dd <= 1) ? 2 : 1;

            int dd_s = r3d->start[dd], dd_e = r3d->stop[dd];
            int rd_s = r3d->start[rd], rd_e = r3d->stop[rd];
            int cd_s = r3d->start[cd], cd_e = r3d->stop[cd];

            // Translate AST regions to global space
            auto op_decomp = op_it->second->decomp;
            dd_s += op_decomp.offset[dd]; dd_e += op_decomp.offset[dd];
            rd_s += op_decomp.offset[rd]; rd_e += op_decomp.offset[rd];
            cd_s += op_decomp.offset[cd]; cd_e += op_decomp.offset[cd];

            int eff_rows = rd_e - rd_s; // effective 2D rows
            int eff_cols = cd_e - cd_s; // effective 2D cols

            // Local tile sizes
            int local_dims[3];
            for (int d = 0; d < 3; d++)
                local_dims[d] = op_it->second->region.size(d);

            // Compute overlap of this chare's tile with the 3D region
            auto op_chare = op_decomp.chare_region_global(nd_idx);
            int dd_lo = std::max(op_chare.start[dd], dd_s);
            int dd_hi = std::min(op_chare.stop[dd], dd_e);
            int row_lo = std::max(op_chare.start[rd], rd_s);
            int row_hi = std::min(op_chare.stop[rd], rd_e);
            int col_lo = std::max(op_chare.start[cd], cd_s);
            int col_hi = std::min(op_chare.stop[cd], cd_e);

            if (dd_lo >= dd_hi || row_lo >= row_hi || col_lo >= col_hi)
                continue;

            int sub_rows = row_hi - row_lo;
            int sub_cols = col_hi - col_lo;
            int dd_local_offset = dd_lo - op_chare.start[dd];
            int row_offset = row_lo - op_chare.start[rd];
            int col_offset = col_lo - op_chare.start[cd];

#ifndef USE_KOKKOS
            op_it->second->copyToHost();
#endif

            // Get the other operand's metadata to compute C dimensions
            int other_idx = 1 - op_idx;
            int other_name = mm_root->operands[other_idx]->result_name;
            auto other_meta_it = dag_group->array_meta.find(other_name);
            auto c3_meta_it = dag_group->array_meta.find(result_name);
            ArrayDecomp<2> c3_decomp;
            if (c3_meta_it != dag_group->array_meta.end())
                c3_decomp = c3_meta_it->second.template decomp<2>();
            int c_M = c3_decomp.offset[0] + c3_decomp.global_shape[0];
            int c_N_cols_g = c3_decomp.offset[1] + c3_decomp.global_shape[1];

            // Determine C chare ranges (in global space)
            int first_c_row_chare = c3_decomp.offset[0] / tile_2d;
            int last_c_row_chare = (c_M > 0) ? (c_M - 1) / tile_2d : 0;
            int first_c_col_chare = c3_decomp.offset[1] / tile_2d;
            int last_c_col_chare = (c_N_cols_g > 0) ? (c_N_cols_g - 1) / tile_2d : 0;

            if (op_idx == 0) {
                // This 3D chare holds A. Send A panels to C chares.
                // Effective A rows → C rows, effective A cols → K dimension
                int k_start = col_lo - cd_s;
                int k_end = col_hi - cd_s;
                int c_row_start = c3_decomp.offset[0] + (row_lo - rd_s);
                int c_row_end = c3_decomp.offset[0] + (row_hi - rd_s);
                int first_dest_row_chare = c_row_start / tile_2d;
                int last_dest_row_chare = (c_row_end - 1) / tile_2d;

                // Extract 2D sub-block from 3D tile and send
                auto send_3d_panel = [&](auto* dummy) {
                    using T = std::remove_pointer_t<decltype(dummy)>;
                    // 3D row-major strides: [d0_stride, d1_stride, 1]
                    int s0 = local_dims[1] * local_dims[2];
                    int s1 = local_dims[2];
                    int s2 = 1;
                    int strides[3] = {s0, s1, s2};

#ifdef USE_KOKKOS
                    T* d_data = static_cast<T*>(op_it->second->device_data_ptr());
                    int cap_dd = dd_local_offset, cap_rd = row_offset, cap_cd = col_offset;
                    int cap_dd_dim = dd, cap_rd_dim = rd, cap_cd_dim = cd;
                    int cap_sub_cols = sub_cols;

                    for (int tj = first_c_col_chare; tj <= last_c_col_chare; tj++) {
                        for (int ti = first_dest_row_chare; ti <= last_dest_row_chare; ti++) {
                            int dest_row_start_c = std::max(c_row_start, ti * tile_2d);
                            int dest_row_end_c = std::min(c_row_end, (ti + 1) * tile_2d);
                            int send_rows = dest_row_end_c - dest_row_start_c;
                            int send_row_off = dest_row_start_c - c_row_start;

                            ChareIndex<2> target_ci;
                            target_ci.idx[0] = ti;
                            target_ci.idx[1] = tj;

                            // Extract directly from 3D tile to panel on device
                            int64_t panel_count = (int64_t)send_rows * sub_cols;
                            int64_t panel_bytes = panel_count * sizeof(T);
                            T* panel_buf = static_cast<T*>(
                                Kokkos::kokkos_malloc<DeviceSpace>(panel_bytes));
                            Kokkos::parallel_for(
                                CT_COMM_POLICY(partition, panel_count),
                                KOKKOS_LAMBDA(int i) {
                                    int r = i / cap_sub_cols;
                                    int c = i % cap_sub_cols;
                                    int idx3d[3];
                                    idx3d[cap_dd_dim] = cap_dd;
                                    idx3d[cap_rd_dim] = cap_rd + send_row_off + r;
                                    idx3d[cap_cd_dim] = cap_cd + c;
                                    panel_buf[i] = d_data[idx3d[0] * strides[0] +
                                                          idx3d[1] * strides[1] +
                                                          idx3d[2] * strides[2]];
                                });

                            int region_data[2 * 3] = {k_start, k_end, 1,
                                                      dest_row_start_c, dest_row_end_c, 1};

#ifndef NDEBUG
                            partition->comm_bytes_sent += panel_bytes;
#endif
                            device_pack_send<3, 2>(partition, dag_group->partition_proxy_2,
                                                   target_ci, node->id, 0, op_name,
                                                   region_data, panel_bytes, panel_buf);
                        }
                    }
#else
                    T* data = static_cast<T*>(op_it->second->data_ptr());
                    int64_t send_count = (int64_t)sub_rows * sub_cols;
                    T* send_buf = new T[send_count];

                    for (int r = 0; r < sub_rows; r++) {
                        for (int c = 0; c < sub_cols; c++) {
                            int idx3d[3];
                            idx3d[dd] = dd_local_offset;
                            idx3d[rd] = row_offset + r;
                            idx3d[cd] = col_offset + c;
                            send_buf[r * sub_cols + c] = data[idx3d[0]*strides[0] + idx3d[1]*strides[1] + idx3d[2]*strides[2]];
                        }
                    }

                    int64_t byte_size = send_count * sizeof(T);
                    for (int tj = first_c_col_chare; tj <= last_c_col_chare; tj++) {
                        for (int ti = first_dest_row_chare; ti <= last_dest_row_chare; ti++) {
                            int dest_row_start_c = std::max(c_row_start, ti * tile_2d);
                            int dest_row_end_c = std::min(c_row_end, (ti + 1) * tile_2d);
                            int send_rows = dest_row_end_c - dest_row_start_c;
                            int send_row_off = dest_row_start_c - c_row_start;

                            ChareIndex<2> target_ci;
                            target_ci.idx[0] = ti;
                            target_ci.idx[1] = tj;

                            int region_data[2 * 3];
                            region_data[0] = k_start;
                            region_data[1] = k_end;
                            region_data[2] = 1;
                            region_data[3] = dest_row_start_c;
                            region_data[4] = dest_row_end_c;
                            region_data[5] = 1;

                            int64_t panel_count = (int64_t)send_rows * sub_cols;
                            int64_t panel_bytes = panel_count * sizeof(T);
                            T* panel_buf = new T[panel_count];
                            for (int r = 0; r < send_rows; r++)
                                memcpy(panel_buf + r * sub_cols,
                                       send_buf + (send_row_off + r) * sub_cols,
                                       sub_cols * sizeof(T));


#ifndef NDEBUG
                            partition->comm_bytes_sent += panel_bytes;
#endif
                            proxy_at<2>(dag_group->partition_proxy_2, target_ci)
                                .receive_data(node->id, /*input_index=*/0, op_name, 2,
                                              region_data, panel_bytes,
                                              reinterpret_cast<char*>(panel_buf));
                            delete[] panel_buf;
                        }
                    }
                    delete[] send_buf;
#endif
                };
                switch (dt) {
                case DType::FLOAT32: { float* d = nullptr; send_3d_panel(d); break; }
                case DType::FLOAT64: { double* d = nullptr; send_3d_panel(d); break; }
                case DType::INT32: { int32_t* d = nullptr; send_3d_panel(d); break; }
                case DType::INT64: { int64_t* d = nullptr; send_3d_panel(d); break; }
                }
            } else {
                // This 3D chare holds B. Send B panels to C chares.
                // Effective B rows → K dimension, effective B cols → C cols
                int k_start = row_lo - rd_s;
                int k_end = row_hi - rd_s;
                int c_col_start = c3_decomp.offset[1] + (col_lo - cd_s);
                int c_col_end = c3_decomp.offset[1] + (col_hi - cd_s);
                int first_dest_col_chare = c_col_start / tile_2d;
                int last_dest_col_chare = (c_col_end - 1) / tile_2d;

                auto send_3d_panel = [&](auto* dummy) {
                    using T = std::remove_pointer_t<decltype(dummy)>;
                    int s0 = local_dims[1] * local_dims[2];
                    int s1 = local_dims[2];
                    int s2 = 1;
                    int strides[3] = {s0, s1, s2};

#ifdef USE_KOKKOS
                    T* d_data = static_cast<T*>(op_it->second->device_data_ptr());
                    int cap_dd = dd_local_offset, cap_rd = row_offset, cap_cd = col_offset;
                    int cap_dd_dim = dd, cap_rd_dim = rd, cap_cd_dim = cd;
                    int cap_sub_rows = sub_rows;

                    for (int ti = first_c_row_chare; ti <= last_c_row_chare; ti++) {
                        for (int tj = first_dest_col_chare; tj <= last_dest_col_chare; tj++) {
                            int dest_col_start_c = std::max(c_col_start, tj * tile_2d);
                            int dest_col_end_c = std::min(c_col_end, (tj + 1) * tile_2d);
                            int send_cols = dest_col_end_c - dest_col_start_c;
                            int send_col_off = dest_col_start_c - c_col_start;

                            ChareIndex<2> target_ci;
                            target_ci.idx[0] = ti;
                            target_ci.idx[1] = tj;

                            // Extract directly from 3D tile to panel on device
                            int64_t panel_count = (int64_t)sub_rows * send_cols;
                            int64_t panel_bytes = panel_count * sizeof(T);
                            T* panel_buf = static_cast<T*>(
                                Kokkos::kokkos_malloc<DeviceSpace>(panel_bytes));
                            int cap_send_cols = send_cols;
                            Kokkos::parallel_for(
                                CT_COMM_POLICY(partition, panel_count),
                                KOKKOS_LAMBDA(int i) {
                                    int r = i / cap_send_cols;
                                    int c = i % cap_send_cols;
                                    int idx3d[3];
                                    idx3d[cap_dd_dim] = cap_dd;
                                    idx3d[cap_rd_dim] = cap_rd + r;
                                    idx3d[cap_cd_dim] = cap_cd + send_col_off + c;
                                    panel_buf[i] = d_data[idx3d[0] * strides[0] +
                                                          idx3d[1] * strides[1] +
                                                          idx3d[2] * strides[2]];
                                });

                            int region_data[2 * 3] = {k_start, k_end, 1,
                                                      dest_col_start_c, dest_col_end_c, 1};
#ifndef NDEBUG
                            partition->comm_bytes_sent += panel_bytes;
#endif
                            device_pack_send<3, 2>(partition, dag_group->partition_proxy_2,
                                                   target_ci, node->id, 1, op_name,
                                                   region_data, panel_bytes, panel_buf);
                        }
                    }
#else
                    T* data = static_cast<T*>(op_it->second->data_ptr());
                    int64_t send_count = (int64_t)sub_rows * sub_cols;
                    T* send_buf = new T[send_count];

                    for (int r = 0; r < sub_rows; r++) {
                        for (int c = 0; c < sub_cols; c++) {
                            int idx3d[3];
                            idx3d[dd] = dd_local_offset;
                            idx3d[rd] = row_offset + r;
                            idx3d[cd] = col_offset + c;
                            send_buf[r * sub_cols + c] = data[idx3d[0]*strides[0] + idx3d[1]*strides[1] + idx3d[2]*strides[2]];
                        }
                    }

                    int64_t byte_size = send_count * sizeof(T);
                    for (int ti = first_c_row_chare; ti <= last_c_row_chare; ti++) {
                        for (int tj = first_dest_col_chare; tj <= last_dest_col_chare; tj++) {
                            int dest_col_start_c = std::max(c_col_start, tj * tile_2d);
                            int dest_col_end_c = std::min(c_col_end, (tj + 1) * tile_2d);
                            int send_cols = dest_col_end_c - dest_col_start_c;
                            int send_col_off = dest_col_start_c - c_col_start;

                            ChareIndex<2> target_ci;
                            target_ci.idx[0] = ti;
                            target_ci.idx[1] = tj;

                            int region_data[2 * 3];
                            region_data[0] = k_start;
                            region_data[1] = k_end;
                            region_data[2] = 1;
                            region_data[3] = dest_col_start_c;
                            region_data[4] = dest_col_end_c;
                            region_data[5] = 1;

                            int64_t panel_count = (int64_t)sub_rows * send_cols;
                            int64_t panel_bytes = panel_count * sizeof(T);
                            T* panel_buf = new T[panel_count];
                            for (int r = 0; r < sub_rows; r++)
                                for (int c = 0; c < send_cols; c++)
                                    panel_buf[r * send_cols + c] = send_buf[r * sub_cols + (send_col_off + c)];


#ifndef NDEBUG
                            partition->comm_bytes_sent += panel_bytes;
#endif
                            proxy_at<2>(dag_group->partition_proxy_2, target_ci)
                                .receive_data(node->id, /*input_index=*/1, op_name, 2,
                                              region_data, panel_bytes,
                                              reinterpret_cast<char*>(panel_buf));
                            delete[] panel_buf;
                        }
                    }
                    delete[] send_buf;
#endif
                };
                switch (dt) {
                case DType::FLOAT32: { float* d = nullptr; send_3d_panel(d); break; }
                case DType::FLOAT64: { double* d = nullptr; send_3d_panel(d); break; }
                case DType::INT32: { int32_t* d = nullptr; send_3d_panel(d); break; }
                case DType::INT64: { int64_t* d = nullptr; send_3d_panel(d); break; }
                }
            }
        }

        // 3D chares only send — they don't hold C, so just finish
        node_finished(node->id);

    } else {
        CkAbort("MATMATMUL only supported for 2D and 3D partitions (N=%d)", N);
    }
}

// ---------------------------------------------------------------------------
// Cross-partition SET_REGION
// ---------------------------------------------------------------------------

/// Helper: get the target partition proxy for a given ndims.
template <int N_tgt>
static typename PartitionTraits<N_tgt>::ProxyType& get_partition_proxy(ArrayDAGGroup* dag_group);

template <>
CProxy_Partition1D& get_partition_proxy<1>(ArrayDAGGroup* dag_group) {
    return dag_group->partition_proxy_1;
}
template <>
CProxy_Partition2D& get_partition_proxy<2>(ArrayDAGGroup* dag_group) {
    return dag_group->partition_proxy_2;
}
template <>
CProxy_Partition3D& get_partition_proxy<3>(ArrayDAGGroup* dag_group) {
    return dag_group->partition_proxy_3;
}

/// Source-side dispatch: send local data from Partition<N> to the target partition.
/// Determines target ndims at runtime and dispatches to the correct template.
template <int N>
static void dispatch_cross_send(DType dt, PartitionImpl<N>* partition, DAGNode* node, int source_name,
                                int target_name, ASTNode* sr_root, ArrayDAGGroup* dag_group) {
    int target_ndims = sr_root->ndims;
    Region* tgt_region = sr_root->region;

    // Compute dimension mapping from source metadata
    auto src_it = partition->arrays.find(source_name);
    if (src_it == partition->arrays.end() || src_it->second->local_size() == 0)
        return;

    int src_global_shape[3] = {};
    for (int d = 0; d < N; ++d)
        src_global_shape[d] = src_it->second->global_shape[d];

    // Look up target array decomposition from array_meta
    auto tgt_meta_it = dag_group->array_meta.find(target_name);

    // Dispatch to the correct N_tgt template
    if (target_ndims == 1) {
        int dim_map[1];
        compute_dim_map<N, 1>(src_global_shape, tgt_region, dim_map);
        auto tgt_decomp = tgt_meta_it->second.template decomp<1>();
        dispatch_cross_set_region_send<N, 1>(dt, partition, node, source_name, dim_map, tgt_region,
                                              get_partition_proxy<1>(dag_group), tgt_decomp);
    } else if (target_ndims == 2) {
        int dim_map[2];
        compute_dim_map<N, 2>(src_global_shape, tgt_region, dim_map);
        auto tgt_decomp = tgt_meta_it->second.template decomp<2>();
        dispatch_cross_set_region_send<N, 2>(dt, partition, node, source_name, dim_map, tgt_region,
                                              get_partition_proxy<2>(dag_group), tgt_decomp);
    } else if (target_ndims == 3) {
        int dim_map[3];
        compute_dim_map<N, 3>(src_global_shape, tgt_region, dim_map);
        auto tgt_decomp = tgt_meta_it->second.template decomp<3>();
        dispatch_cross_set_region_send<N, 3>(dt, partition, node, source_name, dim_map, tgt_region,
                                              get_partition_proxy<3>(dag_group), tgt_decomp);
    }
}

template <int N>
void ArrayDAGExecutorND<N>::execute_cross_set_region_node(DAGNode* node) {
    ASTNode* sr_root = nullptr;
    for (ASTNode* root : node->ast->roots) {
        if (static_cast<Opcode>(root->opcode) == Opcode::SET_REGION) {
            sr_root = root;
            break;
        }
    }
    if (!sr_root) {
        node_finished(node->id);
        return;
    }

    int target_name = sr_root->operands[0]->result_name;
    int source_name = sr_root->operands[1]->result_name;
    int target_ndims = sr_root->ndims;
    int source_ndims = sr_root->operands[1]->ndims;

    auto* dag_group = static_cast<ArrayDAGGroup*>(group);
    DType dt = determine_dtype<N>(node, partition->arrays);

    DBG_PRINT("[Chare %d] execute_cross_set_region_node<%d>: node_id=%d target=%d(nd=%d) "
              "source=%d(nd=%d)\n",
              partition->index[0], N, node->id, target_name, target_ndims, source_name,
              source_ndims);

    if (N == source_ndims) {
        // === SOURCE SIDE: send data to target partition ===
        dispatch_cross_send<N>(dt, partition, node, source_name, target_name, sr_root, dag_group);
        node_finished(node->id);

    } else if (N == target_ndims) {
        // === TARGET SIDE: set up to receive data from source partition ===
        auto tgt_it = partition->arrays.find(target_name);
        if (tgt_it == partition->arrays.end()) {
            node_finished(node->id);
            return;
        }

        auto nd_idx = partition->nd_index();
        auto tgt_decomp = tgt_it->second->decomp;
        auto r_chare_global = tgt_decomp.chare_region_global(nd_idx);
        auto* tgt_region = static_cast<ArrayRegion<N>*>(sr_root->region);

        // Preserve the target lattice for stepped target slices.
        auto [my_input, has_overlap] = intersect(*tgt_region, r_chare_global);
        if (!has_overlap) {
            node_finished(node->id);
            return;
        }

        // Compute expected messages from source partition
        auto meta_it = dag_group->array_meta.find(source_name);
        if (meta_it == dag_group->array_meta.end()) {
            CkAbort("Cross-partition SET_REGION: could not find source metadata for name=%d",
                     source_name);
            return;
        }
        int src_ndims = meta_it->second.ndims;
        int src_tile = array_tile(dag_group->array_meta, source_name, src_ndims);


        // Build dimension mapping (source dim → target dim)
        // We need to know which source dim corresponds to each target dim
        int dim_map[3] = {}; // dim_map[td] = source dim
        if (src_ndims > N) {
            // Higher→lower: non-singleton source dims map to target dims
            int td = 0;
            for (int sd = 0; sd < src_ndims && td < N; ++sd) {
                if (meta_it->second.global_shape[sd] > 1)
                    dim_map[td++] = sd;
            }
        } else {
            // Lower→higher: non-singleton target region dims receive source dims
            int sd = 0;
            for (int td = 0; td < N && sd < src_ndims; ++td) {
                if (tgt_region->size(td) > 1)
                    dim_map[td] = sd++;
                else
                    dim_map[td] = -1;
            }
        }

        // Count expected messages: for each target dim, how many source chares overlap
        int expected_msgs = 1;
        for (int td = 0; td < N; ++td) {
            if (dim_map[td] < 0) {
                // Singleton target dim: exactly 1 source "slice"
                continue;
            }
            int range_start = my_input.start[td];
            int range_stop = my_input.stop[td];
            int src_chare_start = range_start / src_tile;
            int src_chare_stop = (range_stop + src_tile - 1) / src_tile;
            expected_msgs *= (src_chare_stop - src_chare_start);
        }

        DBG_PRINT("[Chare %d]   target side: expected_msgs=%d\n", partition->index[0],
                  expected_msgs);

        // Set up PendingComm
        auto pre_it = pending.find(node->id);
        int pre_arrived = 0;
        std::unordered_map<int, std::vector<RemoteBuffer<N>>> pre_buffers;
        if (pre_it != pending.end()) {
            pre_arrived = -(pre_it->second.expected_msgs);
            pre_buffers = std::move(pre_it->second.remote_buffers);
        }
        int remaining = expected_msgs - pre_arrived;
        pending[node->id] = {node, remaining, {my_input}, std::move(pre_buffers)};
        if (remaining <= 0)
            on_comm_done(node->id);

    } else {
        // This partition is neither source nor target
        node_finished(node->id);
    }
}

/// Helper: send diagonal elements from a source partition to a target partition.
/// For 1D→2D: source is 1D, sends vector elements to 2D chares at diagonal positions.
/// For 2D→1D: source is 2D, sends diagonal elements to 1D chares.
template <int N_src, typename T>
static void diag_send(PartitionImpl<N_src>* partition, DAGNode* node, int source_name,
                       int result_name, int k_offset, ArrayDAGGroup* dag_group) {
    auto src_it = partition->arrays.find(source_name);
    if (src_it == partition->arrays.end() || src_it->second->local_size() == 0)
        return;

    auto* src = static_cast<Array<N_src, T>*>(src_it->second);

    if constexpr (N_src == 1) {
        // 1D → 2D: send vector elements to 2D chares at diagonal positions
        // tile_2d: the tile of the destination 2D array, to correctly target chares
        int tile_2d = array_tile(dag_group->array_meta, result_name, 2);


        auto nd_idx = partition->nd_index();
        int my_start = src_it->second->decomp.chare_start_global(0, nd_idx[0]);
        int local_size = src->region.size(0);

        // Precompute target chare boundaries on host (cheap integer math).
        // Elements mapping to the same target chare are contiguous in a 1D tile.
        struct ChareRange {
            ChareIndex<2> target_ci;
            int first_vec_idx, last_vec_idx; // inclusive
            int first_local, count;          // offset in src array, element count
        };
        std::vector<ChareRange> ranges;
        for (int i = 0; i < local_size; ) {
            int vec_idx = my_start + i;
            int row = (k_offset >= 0) ? vec_idx : vec_idx - k_offset;
            int col = (k_offset >= 0) ? vec_idx + k_offset : vec_idx;
            int target_row_chare = row / tile_2d;
            int target_col_chare = col / tile_2d;
            ChareIndex<2> target_ci;
            target_ci.idx[0] = target_row_chare;
            target_ci.idx[1] = target_col_chare;

            int j = i + 1;
            while (j < local_size) {
                int vj = my_start + j;
                int rj = (k_offset >= 0) ? vj : vj - k_offset;
                int cj = (k_offset >= 0) ? vj + k_offset : vj;
                if (rj / tile_2d != target_row_chare || cj / tile_2d != target_col_chare)
                    break;
                ++j;
            }
            ranges.push_back({target_ci, vec_idx, my_start + j - 1, i, j - i});
            i = j;
        }

        // Pack and send for each range
        for (auto& r : ranges) {
            int first_row = (k_offset >= 0) ? r.first_vec_idx : r.first_vec_idx - k_offset;
            int last_row = (k_offset >= 0) ? r.last_vec_idx : r.last_vec_idx - k_offset;
            int first_col = (k_offset >= 0) ? r.first_vec_idx + k_offset : r.first_vec_idx;
            int last_col = (k_offset >= 0) ? r.last_vec_idx + k_offset : r.last_vec_idx;

            int region_data[2 * 3];
            region_data[0] = first_row;
            region_data[1] = last_row + 1;
            region_data[2] = 1;
            region_data[3] = first_col;
            region_data[4] = last_col + 1;
            region_data[5] = 1;

            int64_t byte_size = r.count * sizeof(T);
#ifndef NDEBUG
            partition->comm_bytes_sent += byte_size;
#endif
#ifdef USE_KOKKOS
            // Pack on device: elements are contiguous in the 1D source array
            T* send_buf = static_cast<T*>(Kokkos::kokkos_malloc<DeviceSpace>(byte_size));
            T* src_device = static_cast<T*>(src->device_data_ptr()) + r.first_local;
            Kokkos::parallel_for(
                CT_COMM_POLICY(partition, r.count),
                KOKKOS_LAMBDA(int j) { send_buf[j] = src_device[j]; });
            device_pack_send<1, 2>(partition, dag_group->partition_proxy_2, r.target_ci,
                                   node->id, 0, source_name, region_data, byte_size, send_buf);
#else
            src->copyToHost();
            T* src_data = static_cast<T*>(src->data_ptr());
            T* send_buf = new T[r.count];
            for (int j = 0; j < r.count; ++j)
                send_buf[j] = src_data[r.first_local + j];
            proxy_at<2>(dag_group->partition_proxy_2, r.target_ci)
                .receive_data(node->id, /*input_index=*/0, source_name, 2, region_data,
                              byte_size, reinterpret_cast<char*>(send_buf));
            delete[] send_buf;
#endif
        }
    } else if constexpr (N_src == 2) {
        // 2D → 1D: extract diagonal elements and send to 1D chares
        int tile_1d = array_tile(dag_group->array_meta, result_name, 1);

        auto nd_idx = partition->nd_index();
        auto src_decomp = src_it->second->decomp;
        int row_start = src_decomp.chare_start_global(0, nd_idx[0]);
        int col_start = src_decomp.chare_start_global(1, nd_idx[1]);
        int local_rows = src->region.size(0);
        int local_cols = src->region.size(1);

        // Compute diagonal element range in this tile
        int row_lo = row_start;
        int row_hi = row_start + local_rows;
        int col_lo = col_start;
        int col_hi = col_start + local_cols;

        int diag_lo, diag_hi;
        if (k_offset >= 0) {
            diag_lo = std::max(row_lo, col_lo - k_offset);
            diag_hi = std::min(row_hi, col_hi - k_offset);
        } else {
            diag_lo = std::max(row_lo + k_offset, col_lo);
            diag_hi = std::min(row_hi + k_offset, col_hi);
        }

        if (diag_lo >= diag_hi)
            return;

        // Precompute ranges grouped by target 1D chare
        struct ChareRange {
            ChareIndex<1> target_ci;
            int first_vec_idx;
            int count;
        };
        std::vector<ChareRange> ranges;
        for (int vec_idx = diag_lo; vec_idx < diag_hi; ) {
            int target_chare = vec_idx / tile_1d;
            ChareIndex<1> target_ci;
            target_ci.idx[0] = target_chare;
            int end = std::min(diag_hi, (target_chare + 1) * tile_1d);
            ranges.push_back({target_ci, vec_idx, end - vec_idx});
            vec_idx = end;
        }

        for (auto& r : ranges) {
            int region_data[1 * 3];
            region_data[0] = r.first_vec_idx;
            region_data[1] = r.first_vec_idx + r.count;
            region_data[2] = 1;

            int64_t byte_size = r.count * sizeof(T);
#ifndef NDEBUG
            partition->comm_bytes_sent += byte_size;
#endif
#ifdef USE_KOKKOS
            // Extract diagonal elements on device
            T* send_buf = static_cast<T*>(Kokkos::kokkos_malloc<DeviceSpace>(byte_size));
            T* src_device = static_cast<T*>(src->device_data_ptr());
            int rs_cap = row_start, cs_cap = col_start, lc_cap = local_cols;
            int ko_cap = k_offset;
            int fvi_cap = r.first_vec_idx;
            Kokkos::parallel_for(
                CT_COMM_POLICY(partition, r.count),
                KOKKOS_LAMBDA(int j) {
                    int vec_idx = fvi_cap + j;
                    int row = (ko_cap >= 0) ? vec_idx : vec_idx - ko_cap;
                    int col = (ko_cap >= 0) ? vec_idx + ko_cap : vec_idx;
                    int local_row = row - rs_cap;
                    int local_col = col - cs_cap;
                    send_buf[j] = src_device[local_row * lc_cap + local_col];
                });
            device_pack_send<2, 1>(partition, dag_group->partition_proxy_1, r.target_ci,
                                   node->id, 0, source_name, region_data, byte_size, send_buf);
#else
            src->copyToHost();
            T* src_data = static_cast<T*>(src->data_ptr());
            T* send_buf = new T[r.count];
            for (int j = 0; j < r.count; ++j) {
                int vec_idx = r.first_vec_idx + j;
                int row, col;
                if (k_offset >= 0) {
                    row = vec_idx;
                    col = vec_idx + k_offset;
                } else {
                    row = vec_idx - k_offset;
                    col = vec_idx;
                }
                int local_row = row - row_start;
                int local_col = col - col_start;
                send_buf[j] = src_data[local_row * local_cols + local_col];
            }
            proxy_at<1>(dag_group->partition_proxy_1, r.target_ci)
                .receive_data(node->id, /*input_index=*/0, source_name, 1, region_data,
                              byte_size, reinterpret_cast<char*>(send_buf));
            delete[] send_buf;
#endif
        }
    }
}

/// Dispatch diag_send by DType.
template <int N_src>
static void dispatch_diag_send(DType dt, PartitionImpl<N_src>* partition, DAGNode* node,
                                int source_name, int result_name, int k_offset,
                                ArrayDAGGroup* dag_group) {
    switch (dt) {
    case DType::FLOAT32:
        diag_send<N_src, float>(partition, node, source_name, result_name, k_offset, dag_group);
        break;
    case DType::FLOAT64:
        diag_send<N_src, double>(partition, node, source_name, result_name, k_offset, dag_group);
        break;
    case DType::INT32:
        diag_send<N_src, int32_t>(partition, node, source_name, result_name, k_offset, dag_group);
        break;
    case DType::INT64:
        diag_send<N_src, int64_t>(partition, node, source_name, result_name, k_offset, dag_group);
        break;
    }
}

template <int N>
void ArrayDAGExecutorND<N>::execute_diag_node(DAGNode* node) {
    ASTNode* diag_root = nullptr;
    for (ASTNode* root : node->ast->roots) {
        if (static_cast<Opcode>(root->opcode) == Opcode::DIAG) {
            diag_root = root;
            break;
        }
    }
    if (!diag_root) {
        node_finished(node->id);
        return;
    }

    int input_name = diag_root->operands[0]->result_name;
    int result_name = diag_root->result_name;
    int k_offset = 0;
    if (diag_root->operands.size() >= 2 && diag_root->operands[1]->is_scalar)
        k_offset = (int)diag_root->operands[1]->scalar;

    auto* dag_group = static_cast<ArrayDAGGroup*>(group);
    DType dt = determine_dtype<N>(node, partition->arrays);

    // Look up input metadata to determine direction
    auto meta_it = dag_group->array_meta.find(input_name);
    if (meta_it == dag_group->array_meta.end()) {
        node_finished(node->id);
        return;
    }
    int input_ndims = meta_it->second.ndims;

    DBG_PRINT("[Chare %d] execute_diag_node<%d>: node_id=%d input=%d(nd=%d) k=%d\n",
              partition->index[0], N, node->id, input_name, input_ndims, k_offset);

    if (input_ndims == 1) {
        // ===== 1D → 2D: construct diagonal matrix =====
        if constexpr (N == 1) {
            // SOURCE SIDE: send vector elements to 2D chares
            dispatch_diag_send<1>(dt, partition, node, input_name, result_name, k_offset, dag_group);
            node_finished(node->id);
        } else if constexpr (N == 2) {
            // TARGET SIDE: receive diagonal elements, place into zero-initialized output
            int tile_1d = array_tile(dag_group->array_meta, input_name, 1);

            auto nd_idx = partition->nd_index();

            // Get output size and decomp from metadata
            auto result_meta_it = dag_group->array_meta.find(result_name);
            if (result_meta_it == dag_group->array_meta.end()) {
                node_finished(node->id);
                return;
            }
            int out_n = result_meta_it->second.global_shape[0];
            auto result_decomp = result_meta_it->second.template decomp<2>();
            auto result_chare = result_decomp.chare_region_global(nd_idx);
            int row_start = result_chare.start[0];
            int col_start = result_chare.start[1];
            int local_rows = result_chare.stop[0] - row_start;
            int local_cols = result_chare.stop[1] - col_start;
            if (local_rows <= 0 || local_cols <= 0) {
                node_finished(node->id);
                return;
            }

            // Count expected messages from 1D chares
            // Diagonal indices that fall in this 2D tile:
            int vec_len = meta_it->second.global_shape[0]; // input vector length
            int diag_lo, diag_hi;
            if (k_offset >= 0) {
                diag_lo = std::max(row_start, col_start - k_offset);
                diag_hi = std::min(row_start + local_rows, col_start + local_cols - k_offset);
            } else {
                diag_lo = std::max(row_start + k_offset, col_start);
                diag_hi = std::min(row_start + local_rows + k_offset, col_start + local_cols);
            }
            diag_lo = std::max(diag_lo, 0);
            diag_hi = std::min(diag_hi, vec_len);

            if (diag_lo >= diag_hi) {
                // No diagonal elements in this tile — create zero output so GET can collect it
                std::array<int, 2> out_start = {0, 0};
                std::array<int, 2> out_stop = {local_rows, local_cols};
                std::array<int, 2> out_step = {1, 1};
                std::array<int, 2> out_gs = {out_n, out_n};
                ArrayRegion<2> out_region(out_start, out_stop, out_step);
                ArrayDecomp<2> out_decomp = dag_group->array_meta[result_name].template decomp<2>();
                partition->arrays[result_name] =
                    partition->allocate_or_reuse(out_region, out_gs, result_name, dt, out_decomp);
#ifndef USE_KOKKOS
                // Array constructor already zero-initializes device memory under USE_KOKKOS
                std::memset(partition->arrays[result_name]->data_ptr(), 0,
                            local_rows * local_cols * dtype_size(dt));
#endif
                node_finished(node->id);
                return;
            }

            // Count how many 1D chares overlap [diag_lo, diag_hi)
            int first_1d_chare = diag_lo / tile_1d;
            int last_1d_chare = (diag_hi - 1) / tile_1d;
            int expected_msgs = last_1d_chare - first_1d_chare + 1;

            // Set up PendingComm
            std::array<int, 2> my_start = {row_start, col_start};
            std::array<int, 2> my_stop = {row_start + local_rows, col_start + local_cols};
            std::array<int, 2> my_step = {1, 1};
            ArrayRegion<2> my_input(my_start, my_stop, my_step);

            auto pre_it = pending.find(node->id);
            int pre_arrived = 0;
            std::unordered_map<int, std::vector<RemoteBuffer<2>>> pre_buffers;
            if (pre_it != pending.end()) {
                pre_arrived = -(pre_it->second.expected_msgs);
                pre_buffers = std::move(pre_it->second.remote_buffers);
            }
            int remaining = expected_msgs - pre_arrived;
            pending[node->id] = {node, remaining, {my_input}, std::move(pre_buffers)};
            if (remaining <= 0)
                on_comm_done(node->id);
        }
    } else if (input_ndims == 2) {
        // ===== 2D → 1D: extract diagonal =====
        if constexpr (N == 2) {
            // SOURCE SIDE: extract diagonal elements and send to 1D chares
            dispatch_diag_send<2>(dt, partition, node, input_name, result_name, k_offset, dag_group);
            node_finished(node->id);
        } else if constexpr (N == 1) {
            // TARGET SIDE: receive diagonal elements
            int tile_2d = array_tile(dag_group->array_meta, input_name, 2);

            auto nd_idx = partition->nd_index();

            // Get result size and decomp from metadata
            auto result_meta_it = dag_group->array_meta.find(result_name);
            if (result_meta_it == dag_group->array_meta.end()) {
                node_finished(node->id);
                return;
            }
            int diag_len = result_meta_it->second.global_shape[0];
            auto result_decomp = result_meta_it->second.template decomp<1>();
            auto result_chare = result_decomp.chare_region_global(nd_idx);
            int my_start_idx = result_chare.start[0];
            int local_size = result_chare.stop[0] - my_start_idx;
            if (local_size <= 0) {
                node_finished(node->id);
                return;
            }

            // My vector range: [my_start_idx, my_start_idx + local_size)
            // These correspond to 2D positions; count which 2D chares contribute
            int my_end_idx = my_start_idx + local_size;

            // Map vector indices to 2D coordinates and count source chares
            std::unordered_set<ChareIndex<2>, ChareIndexHash<2>> source_chares;
            int M = meta_it->second.global_shape[0];
            int N_cols = meta_it->second.global_shape[1];
            for (int vec_idx = my_start_idx; vec_idx < my_end_idx; ++vec_idx) {
                int row, col;
                if (k_offset >= 0) {
                    row = vec_idx;
                    col = vec_idx + k_offset;
                } else {
                    row = vec_idx - k_offset;
                    col = vec_idx;
                }
                if (row >= 0 && row < M && col >= 0 && col < N_cols) {
                    int src_row_chare = row / tile_2d;
                    int src_col_chare = col / tile_2d;
                    ChareIndex<2> src_ci;
                    src_ci.idx[0] = src_row_chare;
                    src_ci.idx[1] = src_col_chare;
                    source_chares.insert(src_ci);
                }
            }
            int expected_msgs = (int)source_chares.size();

            std::array<int, 1> my_start_arr = {my_start_idx};
            std::array<int, 1> my_stop_arr = {my_end_idx};
            std::array<int, 1> my_step_arr = {1};
            ArrayRegion<1> my_input(my_start_arr, my_stop_arr, my_step_arr);

            auto pre_it = pending.find(node->id);
            int pre_arrived = 0;
            std::unordered_map<int, std::vector<RemoteBuffer<1>>> pre_buffers;
            if (pre_it != pending.end()) {
                pre_arrived = -(pre_it->second.expected_msgs);
                pre_buffers = std::move(pre_it->second.remote_buffers);
            }
            int remaining = expected_msgs - pre_arrived;
            pending[node->id] = {node, remaining, {my_input}, std::move(pre_buffers)};
            if (remaining <= 0)
                on_comm_done(node->id);
        }
    } else {
        node_finished(node->id);
    }
}

/// Helper to get partition proxy by N_tgt.
template <int N_tgt>
struct PartitionProxyHelper;

template <>
struct PartitionProxyHelper<1> {
    static auto& get(ArrayDAGGroup* g) { return g->partition_proxy_1; }
};
template <>
struct PartitionProxyHelper<2> {
    static auto& get(ArrayDAGGroup* g) { return g->partition_proxy_2; }
};
template <>
struct PartitionProxyHelper<3> {
    static auto& get(ArrayDAGGroup* g) { return g->partition_proxy_3; }
};

/// Helper: send tile data from source partition to target partition chares.
/// Each source chare sends its data to every output chare whose tiled region
/// overlaps with this source chare's data (via modular mapping).
template <int N_src, int N_tgt, typename T>
static void tile_send(PartitionImpl<N_src>* partition, DAGNode* node, int source_name,
                       int result_name, std::array<int, 3> const& reps,
                       std::array<int, 3> const& input_shape, int out_ndims,
                       ArrayDAGGroup* dag_group) {
    auto src_it = partition->arrays.find(source_name);
    if (src_it == partition->arrays.end() || src_it->second->local_size() == 0)
        return;

    auto* src = static_cast<Array<N_src, T>*>(src_it->second);
    auto nd_idx = partition->nd_index();
    auto src_decomp = src_it->second->decomp;

    // Source chare's global region
    std::array<int, N_src> src_start, src_stop;
    for (int d = 0; d < N_src; ++d) {
        src_start[d] = src_decomp.chare_start_global(d, nd_idx[d]);
        src_stop[d] = std::min(src_start[d] + src->region.size(d),
                               src_decomp.offset[d] + src_decomp.global_shape[d]);
    }

    // Get result decomposition
    auto result_meta_it = dag_group->array_meta.find(result_name);
    if (result_meta_it == dag_group->array_meta.end())
        return;
    auto result_decomp = result_meta_it->second.template decomp<N_tgt>();

    // Compute output shape
    int delta = out_ndims - N_src;
    std::array<int, N_tgt> out_shape;
    for (int d = 0; d < N_tgt; ++d)
        out_shape[d] = result_meta_it->second.global_shape[d];

    // Find all output chares that need data from this source chare.
    // For dimension d of the output:
    //   If d < delta (prepended dims): any output position maps to input (since input dim is 1)
    //   If d >= delta: output positions p where (p % input_shape[d-delta]) falls in
    //                  [src_start[d-delta], src_stop[d-delta])
    //
    // For each output chare, we send the entire source tile data.
    // The receiver uses modular indexing to place data correctly.

    int result_tile = result_meta_it->second.tile;

    // Enumerate output chares that need our data.
    // For each dimension, compute which chare indices are relevant.
    std::array<std::vector<int>, N_tgt> chare_indices_per_dim;
    for (int d = 0; d < N_tgt; ++d) {
        int num_chares_d = result_decomp.num_chares(d);
        if (d < delta) {
            // Prepended dimension: all output chares need data
            for (int ci = 0; ci < num_chares_d; ++ci)
                chare_indices_per_dim[d].push_back(ci);
        } else {
            int src_d = d - delta;
            int in_size = input_shape[src_d];
            // For each output chare along this dim, check if any position in its range
            // maps (via mod) into our source range [src_start[src_d], src_stop[src_d])
            for (int ci = 0; ci < num_chares_d; ++ci) {
                int chare_lo = std::max(ci * result_tile, result_decomp.offset[d]);
                int chare_hi = std::min((ci + 1) * result_tile,
                                        result_decomp.offset[d] + out_shape[d]);
                if (chare_lo >= chare_hi)
                    continue;
                // Check if any position in [chare_lo, chare_hi) maps to
                // [src_start[src_d], src_stop[src_d]) via mod in_size
                bool overlaps = false;
                // If the chare range spans at least in_size elements, it definitely overlaps
                if (chare_hi - chare_lo >= in_size) {
                    overlaps = true;
                } else {
                    int mod_lo = chare_lo % in_size;
                    int mod_hi = (chare_hi - 1) % in_size;
                    if (mod_lo <= mod_hi) {
                        // Contiguous mod range [mod_lo, mod_hi]
                        overlaps = !(mod_hi < src_start[src_d] || mod_lo >= src_stop[src_d]);
                    } else {
                        // Wraps around: [mod_lo, in_size) ∪ [0, mod_hi]
                        overlaps = !(mod_hi < src_start[src_d] && mod_lo >= src_stop[src_d]);
                    }
                }
                if (overlaps)
                    chare_indices_per_dim[d].push_back(ci);
            }
        }
    }

    // Pack source data and send to each target chare
    int local_size = src->local_size();
    int64_t byte_size = local_size * sizeof(T);
    // Encode source region in target ndims space so receiver knows where data comes from
    // We send the source's global region (in input-space coords) using the first N_src dims

    // Iterate over all combinations of chare indices
    // For efficiency, we use a recursive approach for N_tgt dimensions
    std::function<void(int, ChareIndex<N_tgt>&)> send_to_chares;
    send_to_chares = [&](int dim, ChareIndex<N_tgt>& ci) {
        if (dim == N_tgt) {
            // Pack region_data: encode source global region in N_tgt-dimensional space
            int region_data[N_tgt * 3];
            for (int d = 0; d < N_tgt; ++d) {
                if (d < delta) {
                    region_data[d * 3 + 0] = 0;
                    region_data[d * 3 + 1] = 1;
                    region_data[d * 3 + 2] = 1;
                } else {
                    int src_d = d - delta;
                    region_data[d * 3 + 0] = src_start[src_d];
                    region_data[d * 3 + 1] = src_stop[src_d];
                    region_data[d * 3 + 2] = 1;
                }
            }

#ifndef USE_KOKKOS
            src->copyToHost();
            T* src_data = static_cast<T*>(src->data_ptr());
            T* send_buf = new T[local_size];
            std::memcpy(send_buf, src_data, byte_size);
#ifndef NDEBUG
            partition->comm_bytes_sent += byte_size;
#endif
            proxy_at<N_tgt>(
                PartitionProxyHelper<N_tgt>::get(dag_group), ci)
                .receive_data(node->id, /*input_index=*/0, source_name, N_tgt, region_data,
                              byte_size, reinterpret_cast<char*>(send_buf));
            delete[] send_buf;
#else
            T* send_buf = static_cast<T*>(Kokkos::kokkos_malloc<DeviceSpace>(byte_size));
            T* src_device = static_cast<T*>(src->device_data_ptr());
            Kokkos::parallel_for(
                CT_COMM_POLICY(partition, local_size),
                KOKKOS_LAMBDA(int j) { send_buf[j] = src_device[j]; });
            device_pack_send<N_src, N_tgt>(partition,
                PartitionProxyHelper<N_tgt>::get(dag_group), ci,
                node->id, 0, source_name, region_data, byte_size, send_buf);
#endif
            return;
        }
        for (int idx : chare_indices_per_dim[dim]) {
            ci.idx[dim] = idx;
            send_to_chares(dim + 1, ci);
        }
    };

    ChareIndex<N_tgt> ci;
    send_to_chares(0, ci);
}

/// Dispatch tile_send by DType.
template <int N_src, int N_tgt>
static void dispatch_tile_send(DType dt, PartitionImpl<N_src>* partition, DAGNode* node,
                                int source_name, int result_name,
                                std::array<int, 3> const& reps,
                                std::array<int, 3> const& input_shape, int out_ndims,
                                ArrayDAGGroup* dag_group) {
    switch (dt) {
    case DType::FLOAT32:
        tile_send<N_src, N_tgt, float>(partition, node, source_name, result_name, reps,
                                        input_shape, out_ndims, dag_group);
        break;
    case DType::FLOAT64:
        tile_send<N_src, N_tgt, double>(partition, node, source_name, result_name, reps,
                                         input_shape, out_ndims, dag_group);
        break;
    case DType::INT32:
        tile_send<N_src, N_tgt, int32_t>(partition, node, source_name, result_name, reps,
                                          input_shape, out_ndims, dag_group);
        break;
    case DType::INT64:
        tile_send<N_src, N_tgt, int64_t>(partition, node, source_name, result_name, reps,
                                          input_shape, out_ndims, dag_group);
        break;
    }
}

template <int N>
void ArrayDAGExecutorND<N>::execute_tile_node(DAGNode* node) {
    ASTNode* tile_root = nullptr;
    for (ASTNode* root : node->ast->roots) {
        if (static_cast<Opcode>(root->opcode) == Opcode::TILE) {
            tile_root = root;
            break;
        }
    }
    if (!tile_root) {
        node_finished(node->id);
        return;
    }

    int input_name = tile_root->operands[0]->result_name;
    int result_name = tile_root->result_name;
    int out_ndims = tile_root->ndims;

    // Extract reps from scalar operands
    std::array<int, 3> reps = {1, 1, 1};
    int num_reps = (int)tile_root->operands.size() - 1;
    for (int d = 0; d < num_reps && d < 3; d++) {
        if (tile_root->operands[d + 1]->is_scalar)
            reps[d] = (int)tile_root->operands[d + 1]->scalar;
    }

    auto* dag_group = static_cast<ArrayDAGGroup*>(group);
    DType dt = determine_dtype<N>(node, partition->arrays);

    // Look up input metadata
    auto meta_it = dag_group->array_meta.find(input_name);
    if (meta_it == dag_group->array_meta.end()) {
        node_finished(node->id);
        return;
    }
    int input_ndims = meta_it->second.ndims;
    std::array<int, 3> input_shape = meta_it->second.global_shape;

    DBG_PRINT("[Chare %d] execute_tile_node<%d>: node_id=%d input=%d(nd=%d) result_nd=%d "
              "reps=(%d,%d,%d)\n",
              partition->index[0], N, node->id, input_name, input_ndims, out_ndims,
              reps[0], reps[1], reps[2]);

    if (N == input_ndims) {
        // SOURCE SIDE: send local data to target chares
        // Dispatch based on (N_src, N_tgt) combination
        if (input_ndims == out_ndims) {
            // Same ndims
            switch (N) {
            case 1:
                dispatch_tile_send<1, 1>(dt, reinterpret_cast<PartitionImpl<1>*>(partition),
                                          node, input_name, result_name, reps, input_shape,
                                          out_ndims, dag_group);
                break;
            case 2:
                dispatch_tile_send<2, 2>(dt, reinterpret_cast<PartitionImpl<2>*>(partition),
                                          node, input_name, result_name, reps, input_shape,
                                          out_ndims, dag_group);
                break;
            case 3:
                dispatch_tile_send<3, 3>(dt, reinterpret_cast<PartitionImpl<3>*>(partition),
                                          node, input_name, result_name, reps, input_shape,
                                          out_ndims, dag_group);
                break;
            }
        } else {
            // Cross-ndims: input_ndims < out_ndims
            if (input_ndims == 1 && out_ndims == 2) {
                dispatch_tile_send<1, 2>(dt, reinterpret_cast<PartitionImpl<1>*>(partition),
                                          node, input_name, result_name, reps, input_shape,
                                          out_ndims, dag_group);
            } else if (input_ndims == 1 && out_ndims == 3) {
                dispatch_tile_send<1, 3>(dt, reinterpret_cast<PartitionImpl<1>*>(partition),
                                          node, input_name, result_name, reps, input_shape,
                                          out_ndims, dag_group);
            } else if (input_ndims == 2 && out_ndims == 3) {
                dispatch_tile_send<2, 3>(dt, reinterpret_cast<PartitionImpl<2>*>(partition),
                                          node, input_name, result_name, reps, input_shape,
                                          out_ndims, dag_group);
            }
        }
        // Source side finishes immediately after sending
        if (N != out_ndims) {
            node_finished(node->id);
        }
    }

    if (N == out_ndims) {
        // TARGET SIDE: set up PendingComm and wait for data
        auto nd_idx = partition->nd_index();

        auto result_meta_it = dag_group->array_meta.find(result_name);
        if (result_meta_it == dag_group->array_meta.end()) {
            node_finished(node->id);
            return;
        }
        auto result_decomp = result_meta_it->second.template decomp<N>();
        auto result_chare = result_decomp.chare_region_global(nd_idx);

        // Check this chare has nonzero extent
        bool has_extent = true;
        for (int d = 0; d < N; ++d) {
            if (result_chare.stop[d] <= result_chare.start[d]) {
                has_extent = false;
                break;
            }
        }
        if (!has_extent) {
            node_finished(node->id);
            return;
        }

        // Count expected messages: how many source chares will send to us?
        // For each source dimension, find which source chares overlap via modular mapping.
        int delta = out_ndims - input_ndims;
        auto input_decomp_meta = dag_group->array_meta.find(input_name);
        int input_tile = input_decomp_meta->second.tile;

        int expected_msgs = 1;
        for (int src_d = 0; src_d < input_ndims; ++src_d) {
            int out_d = src_d + delta;
            int in_size = input_shape[src_d];
            int chare_lo = result_chare.start[out_d];
            int chare_hi = result_chare.stop[out_d];

            // Find which source chares are needed
            std::set<int> needed_src_chares;
            // Walk through the output range, mapping each position to input via mod
            // Optimization: step by input_tile to cover ranges efficiently
            for (int p = chare_lo; p < chare_hi; ) {
                int inp_pos = p % in_size;
                int src_chare = inp_pos / input_tile;
                needed_src_chares.insert(src_chare);
                // Skip to next possible source chare boundary
                int next_boundary = (src_chare + 1) * input_tile - inp_pos + p;
                // Also skip to next mod-wrap boundary
                int next_wrap = p + (in_size - inp_pos);
                p = std::min({next_boundary, next_wrap, chare_hi});
            }
            expected_msgs *= (int)needed_src_chares.size();
        }

        // Set up PendingComm
        ArrayRegion<N> my_input;
        for (int d = 0; d < N; ++d) {
            my_input.start[d] = result_chare.start[d];
            my_input.stop[d] = result_chare.stop[d];
            my_input.step[d] = 1;
        }

        auto pre_it = pending.find(node->id);
        int pre_arrived = 0;
        std::unordered_map<int, std::vector<RemoteBuffer<N>>> pre_buffers;
        if (pre_it != pending.end()) {
            pre_arrived = -(pre_it->second.expected_msgs);
            pre_buffers = std::move(pre_it->second.remote_buffers);
        }
        int remaining = expected_msgs - pre_arrived;
        pending[node->id] = {node, remaining, {my_input}, std::move(pre_buffers)};
        if (remaining <= 0)
            on_comm_done(node->id);
    }
}

template <int N>
void ArrayDAGExecutorND<N>::on_comm_done(int node_id) {
    auto it = pending.find(node_id);
    if (it == pending.end())
        return;

    PendingComm<N>& comm = it->second;
    DAGNode* node = comm.node;
    DType dt = determine_dtype<N>(node, partition->arrays);
    dispatch_execute<N>(dt, static_cast<ArrayDAGGroup*>(group), node, partition, &comm);

#ifdef USE_NVIDIA
    // Defer cleanup + node_finished until GPU work completes on compute_stream
    {
        auto* p = new ComputeDoneParam<N>{partition, node_id, true};
        CkCallback hcb(compute_done_cb<N>, p);
        hapiAddCallback(partition->compute_stream_raw, &hcb);
    }
#else
    CT_KOKKOS_FENCE();
    comm.clear_remote_buffers();
    pending.erase(it);
    // on_comm_done is high-volume; node_finished already prints
    node_finished(node_id);
#endif
}

template <int N>
void ArrayDAGExecutorND<N>::on_matmatmul_receive(int node_id, int input_index) {
    if constexpr (N != 2) return;

    auto it = pending.find(node_id);
    if (it == pending.end()) return;
    PendingComm<N>& comm = it->second;

    // The just-arrived panel is the last element in remote_buffers[input_index]
    auto& arrived_bufs = comm.remote_buffers[input_index];
    if (arrived_bufs.empty()) return;
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
    if (!mm_root) return;

    int result_name = mm_root->result_name;
    auto arr_it = partition->arrays.find(result_name);
    if (arr_it == partition->arrays.end()) return;

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
            if (k_lo >= k_hi) continue;

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
    case DType::FLOAT32: { float* d = nullptr; compute_pair(d); break; }
    case DType::FLOAT64: { double* d = nullptr; compute_pair(d); break; }
    case DType::INT32: { int32_t* d = nullptr; compute_pair(d); break; }
    case DType::INT64: { int64_t* d = nullptr; compute_pair(d); break; }
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

template <int N>
int ArrayDAGExecutorND<N>::ast_visitor(ASTNode* node, DType dtype) {
    auto nd_idx = partition->nd_index();

    switch (static_cast<Opcode>(node->opcode)) {
    case Opcode::CREATE: {
        auto* dag_group = static_cast<ArrayDAGGroup*>(group);
        auto meta_it = dag_group->array_meta.find(node->result_name);
        auto decomp = meta_it->second.template decomp<N>();
        return partition->create(static_cast<ArrayRegion<N>*>(node->region), node->result_name,
                                 node->dtype, decomp);
    }
    case Opcode::SET_REGION: {
        // Get target array (type-erased) — may not exist on this chare
        // if the array is smaller than the chare grid.
        auto tgt_it = partition->arrays.find(node->operands[0]->result_name);
        if (tgt_it == partition->arrays.end())
            return node->result_name;
        CTArrayBase<N>* target = tgt_it->second;
        int esz = target->elem_size();

        auto* region = static_cast<ArrayRegion<N>*>(node->region);
        ASTNode* rhs_node = node->operands.size() > 1 ? node->operands[1] : nullptr;
        Region* rhs_region_base = node->get_operand_region(1);

        // Decomp-aware chare region and region-to-global translation
        auto tgt_chare = target->decomp.chare_region_global(nd_idx);
        std::array<int, N> g_reg_start, g_reg_stop;
        for (int d = 0; d < N; ++d) {
            g_reg_start[d] = region->start[d] + target->decomp.offset[d];
            g_reg_stop[d] = region->stop[d] + target->decomp.offset[d];
        }
        ArrayRegion<N> dst_region_global(g_reg_start, g_reg_stop, region->step);
        auto [overlap, has_overlap] = intersect(dst_region_global, tgt_chare);

        CTArrayBase<N>* source = nullptr;
        ArrayRegion<N> source_region_global;
        bool has_source_array = false;
        bool source_is_result_buffer = false;
        auto full_source_region_global = [&](CTArrayBase<N>* candidate) {
            std::array<int, N> start{}, stop{}, step{};
            for (int d = 0; d < N; ++d) {
                start[d] = candidate->decomp.offset[d];
                stop[d] = candidate->decomp.offset[d] + candidate->global_shape[d];
                step[d] = 1;
            }
            return ArrayRegion<N>(start, stop, step);
        };
        auto set_source = [&](CTArrayBase<N>* candidate, ArrayRegion<N> const& candidate_region) {
            source = candidate;
            source_region_global = candidate_region;
            has_source_array = true;
            source_is_result_buffer = false;
        };

        if (!has_source_array) {
            auto temp_it = partition->arrays.find(node->result_name);
            if (temp_it != partition->arrays.end()) {
                // Prefer the communicated/caller-visible SET_REGION result buffer
                // when it exists. For mixed decompositions (for example 65->129
                // prolongation), that buffer is already laid out on the target
                // slice's chare grid, while the raw RHS array may only contain
                // this chare's local source tile.
                source = temp_it->second;
                has_source_array = true;
                source_is_result_buffer = true;
            }
        }

        if (!has_source_array && rhs_node) {
            auto rhs_it = partition->arrays.find(rhs_node->result_name);
            auto rhs_op = static_cast<Opcode>(rhs_node->opcode);
            if (rhs_it != partition->arrays.end() &&
                (rhs_op == Opcode::NOOP || rhs_op == Opcode::CREATE)) {
                if (rhs_region_base && !rhs_region_base->is_global) {
                    auto* rhs_region = static_cast<ArrayRegion<N>*>(rhs_region_base);
                    set_source(rhs_it->second, rhs_it->second->decomp.to_global(*rhs_region));
                } else {
                    set_source(rhs_it->second, full_source_region_global(rhs_it->second));
                }
            }
        }

        if (!has_source_array && rhs_node) {
            auto rhs_it = partition->arrays.find(rhs_node->result_name);
            if (rhs_it != partition->arrays.end()) {
                if (rhs_region_base && !rhs_region_base->is_global) {
                    auto* rhs_region = static_cast<ArrayRegion<N>*>(rhs_region_base);
                    set_source(rhs_it->second, rhs_it->second->decomp.to_global(*rhs_region));
                } else {
                    set_source(rhs_it->second, full_source_region_global(rhs_it->second));
                }
            }
        }

        // Compute row-major strides for target
        std::array<int, N> tgt_strides;
        tgt_strides[N - 1] = 1;
        for (int d = N - 2; d >= 0; --d)
            tgt_strides[d] = tgt_strides[d + 1] * target->region.size(d + 1);

        if (!has_source_array) {
            // Scalar fill — convert scalar to target dtype
            if (has_overlap && node->operands.size() > 1 && node->operands[1]->is_scalar) {
                char scalar_buf[8];
                double s = node->operands[1]->scalar;
                switch (dtype) {
                case DType::FLOAT32: {
                    float v = (float)s;
                    memcpy(scalar_buf, &v, sizeof(v));
                    break;
                }
                case DType::FLOAT64: {
                    memcpy(scalar_buf, &s, sizeof(s));
                    break;
                }
                case DType::INT32: {
                    int32_t v = (int32_t)s;
                    memcpy(scalar_buf, &v, sizeof(v));
                    break;
                }
                case DType::INT64: {
                    int64_t v = (int64_t)s;
                    memcpy(scalar_buf, &v, sizeof(v));
                    break;
                }
                }

#ifdef USE_KOKKOS
                // Device-side scalar fill
                char* tgt_device = static_cast<char*>(target->device_data_ptr());
                // Copy scalar_buf to device (8 bytes as int64_t for capture)
                int64_t scalar_bits;
                memcpy(&scalar_bits, scalar_buf, sizeof(scalar_bits));

                int64_t overlap_total = overlap.size();
                int ol_start[N], ol_step[N], tgt_s[N], ol_sizes[N], tgt_cs_arr[N];
                for (int d = 0; d < N; d++) {
                    ol_start[d] = overlap.start[d];
                    ol_step[d] = overlap.step[d];
                    tgt_s[d] = tgt_strides[d];
                    ol_sizes[d] = overlap.size(d);
                    tgt_cs_arr[d] = tgt_chare.start[d];
                }

                Kokkos::parallel_for(
                    CT_COMPUTE_POLICY(partition, overlap_total), KOKKOS_LAMBDA(int flat_idx) {
                        int remaining = flat_idx;
                        int flat = 0;
                        for (int d = N - 1; d >= 0; --d) {
                            int coord_d = remaining % ol_sizes[d];
                            remaining /= ol_sizes[d];
                            int global_d = ol_start[d] + coord_d * ol_step[d];
                            flat += (global_d - tgt_cs_arr[d]) * tgt_s[d];
                        }
                        char* dst = tgt_device + flat * esz;
                        const char* src = reinterpret_cast<const char*>(&scalar_bits);
                        for (int b = 0; b < esz; b++)
                            dst[b] = src[b];
                    });
#else
                // Host-side odometer over the overlap region
                char* tgt_data = static_cast<char*>(target->data_ptr());
                std::array<int, N> idx;
                for (int d = 0; d < N; ++d)
                    idx[d] = overlap.start[d];
                while (true) {
                    int flat = 0;
                    for (int d = 0; d < N; ++d)
                        flat += (idx[d] - tgt_chare.start[d]) * tgt_strides[d];
                    memcpy(tgt_data + flat * esz, scalar_buf, esz);

                    int d = N - 1;
                    while (d >= 0) {
                        idx[d] += overlap.step[d];
                        if (idx[d] < overlap.stop[d])
                            break;
                        idx[d] = overlap.start[d];
                        --d;
                    }
                    if (d < 0)
                        break;
                }
#endif
            }
            return node->result_name;
        }

        if (has_overlap) {
            // Compute row-major strides for source
            std::array<int, N> src_strides;
            src_strides[N - 1] = 1;
            for (int d = N - 2; d >= 0; --d)
                src_strides[d] = src_strides[d + 1] * source->region.size(d + 1);

            auto src_chare = source->decomp.chare_region_global(nd_idx);

#ifdef USE_KOKKOS
            // Device-side element copy
            char* tgt_device = static_cast<char*>(target->device_data_ptr());
            char* src_device = static_cast<char*>(source->device_data_ptr());

            int64_t overlap_total = overlap.size();
            int ol_start[N], ol_step[N], tgt_s[N], src_s[N], ol_sizes[N], tgt_cs_arr2[N];
            int g_reg_s[N], reg_step[N], src_chare_s[N], src_reg_s[N], src_reg_step[N],
                src_region_sizes[N];
            for (int d = 0; d < N; d++) {
                ol_start[d] = overlap.start[d];
                ol_step[d] = overlap.step[d];
                tgt_s[d] = tgt_strides[d];
                src_s[d] = src_strides[d];
                ol_sizes[d] = overlap.size(d);
                tgt_cs_arr2[d] = tgt_chare.start[d];
                g_reg_s[d] = g_reg_start[d];
                reg_step[d] = region->step[d];
                src_chare_s[d] = src_chare.start[d];
                src_reg_s[d] = source_region_global.start[d];
                src_reg_step[d] = source_region_global.step[d];
                src_region_sizes[d] = source->region.size(d);
            }

            Kokkos::parallel_for(
                Kokkos::RangePolicy<Kokkos::Cuda>(partition->compute_exec, 0, overlap_total),
                KOKKOS_LAMBDA(int flat_idx) {
                    int remaining = flat_idx;
                    int dst_flat = 0, src_flat = 0;
                    bool src_valid = true;
                    for (int d = N - 1; d >= 0; --d) {
                        int coord_d = remaining % ol_sizes[d];
                        remaining /= ol_sizes[d];
                        int global_d = ol_start[d] + coord_d * ol_step[d];
                        dst_flat += (global_d - tgt_cs_arr2[d]) * tgt_s[d];
                        if (source_is_result_buffer) {
                            int src_local = (global_d - ol_start[d]) / ol_step[d];
                            if (src_local < 0 || src_local >= src_region_sizes[d]) {
                                src_valid = false;
                                break;
                            }
                            src_flat += src_local * src_s[d];
                        } else {
                            int logical_idx = (global_d - g_reg_s[d]) / reg_step[d];
                            int src_global = src_reg_s[d] + logical_idx * src_reg_step[d];
                            int src_local = src_global - src_chare_s[d];
                            if (src_local < 0 || src_local >= src_region_sizes[d]) {
                                src_valid = false;
                                break;
                            }
                            src_flat += src_local * src_s[d];
                        }
                    }
                    if (src_valid) {
                        char* dst = tgt_device + dst_flat * esz;
                        char* src = src_device + src_flat * esz;
                        for (int b = 0; b < esz; b++)
                            dst[b] = src[b];
                    }
                });
#else
            // Host-side odometer over the overlap region
            char* tgt_data = static_cast<char*>(target->data_ptr());
            char* src_data = static_cast<char*>(source->data_ptr());
            std::array<int, N> idx;
            for (int d = 0; d < N; ++d)
                idx[d] = overlap.start[d];
            while (true) {
                int dst_flat = 0, src_flat = 0;
                bool src_valid = true;
                for (int d = 0; d < N; ++d) {
                    dst_flat += (idx[d] - tgt_chare.start[d]) * tgt_strides[d];
                    if (source_is_result_buffer) {
                        int src_local = (idx[d] - overlap.start[d]) / overlap.step[d];
                        if (src_local < 0 || src_local >= source->region.size(d)) {
                            src_valid = false;
                            break;
                        }
                        src_flat += src_local * src_strides[d];
                    } else {
                        int logical_idx = (idx[d] - g_reg_start[d]) / region->step[d];
                        int src_global = source_region_global.start[d] +
                                         logical_idx * source_region_global.step[d];
                        int src_local = src_global - src_chare.start[d];
                        if (src_local < 0 || src_local >= source->region.size(d)) {
                            src_valid = false;
                            break;
                        }
                        src_flat += src_local * src_strides[d];
                    }
                }
                if (src_valid)
                    memcpy(tgt_data + dst_flat * esz, src_data + src_flat * esz, esz);

                int d = N - 1;
                while (d >= 0) {
                    idx[d] += overlap.step[d];
                    if (idx[d] < overlap.stop[d])
                        break;
                    idx[d] = overlap.start[d];
                    --d;
                }
                if (d < 0)
                    break;
            }
#endif
        }
        return node->result_name;
    }
    case Opcode::REDUCE: {
        // REDUCE is fully handled by execute_reduce_node + Charm++ reduction.
        // The ast_visitor should never be reached for REDUCE nodes.
        return node->result_name;
    }
    case Opcode::MATMUL: {
        if constexpr (N >= 2) {
            // Local matmul in ast_visitor (no-comm path, e.g. single chare)
            // This is reached when execute_matmul_node had total_expected == 0
            int mat_name = node->operands[0]->result_name;
            int vec_name = node->operands[1]->result_name;
            int result_name = node->result_name;

            auto mat_it = partition->arrays.find(mat_name);
            auto vec_it = partition->arrays.find(vec_name);
            if (mat_it == partition->arrays.end() || vec_it == partition->arrays.end()) {
                DBG_PRINT("[Chare %d] MATMUL ast_visitor: missing operand arrays\n",
                          partition->index[0]);
                return result_name;
            }

            CTArrayBase<N>* mat_base = mat_it->second;
            CTArrayBase<N>* vec_base = vec_it->second;
            int local_rows = mat_base->region.size(0);
            int local_cols = mat_base->region.size(1);
            int vec_len = vec_base->local_size();
            int esz = mat_base->elem_size();

            // Create result array if needed
            if (partition->arrays.find(result_name) == partition->arrays.end()) {
                std::array<int, N> out_start = {}, out_stop, out_step, out_gs;
                out_stop[0] = local_rows;
                out_step[0] = 1;
                out_gs[0] = mat_base->global_shape[0];
                if (N > 1) {
                    out_stop[1] = 1;
                    out_step[1] = 1;
                    out_gs[1] = 1;
                }
                ArrayRegion<N> out_region(out_start, out_stop, out_step);
                ArrayDecomp<N> out_decomp = static_cast<ArrayDAGGroup*>(group)->array_meta[result_name].template decomp<N>();
                // Use dtype dispatch to create the correctly typed result array
                partition->arrays[result_name] = partition->allocate_or_reuse(
                    out_region, out_gs, result_name, dtype, out_decomp);
            }

            CTArrayBase<N>* result_base = partition->arrays[result_name];
            int actual_cols = std::min(local_cols, vec_len);

#ifdef USE_KOKKOS
            // Perform matmul on device using KokkosBlas::gemv
            auto kokkos_gemv = [&](auto dummy) {
                using VT = decltype(dummy);
                Kokkos::View<VT**, Kokkos::LayoutRight, DeviceSpace,
                             Kokkos::MemoryTraits<Kokkos::Unmanaged>>
                    d_mat(static_cast<VT*>(mat_base->device_data_ptr()), local_rows, local_cols);
                auto d_sub = Kokkos::subview(d_mat, Kokkos::ALL,
                                             Kokkos::make_pair(0, actual_cols));
                Kokkos::View<VT*, DeviceSpace, Kokkos::MemoryTraits<Kokkos::Unmanaged>>
                    d_vec(static_cast<VT*>(vec_base->device_data_ptr()), actual_cols);
                Kokkos::View<VT*, DeviceSpace, Kokkos::MemoryTraits<Kokkos::Unmanaged>>
                    d_res(static_cast<VT*>(result_base->device_data_ptr()), local_rows);
                KokkosBlas::gemv("N", VT(1), d_sub, d_vec, VT(0), d_res);
            };
            switch (dtype) {
            case DType::FLOAT32: kokkos_gemv(float{}); break;
            case DType::FLOAT64: kokkos_gemv(double{}); break;
            case DType::INT32: kokkos_gemv(int32_t{}); break;
            case DType::INT64: kokkos_gemv(int64_t{}); break;
            }
#else
            // Perform matmul on host using Eigen gemv
            mat_base->copyToHost();
            vec_base->copyToHost();

            switch (dtype) {
            case DType::FLOAT32:
                eigen_gemv(static_cast<float*>(mat_base->data_ptr()), local_rows, local_cols,
                           static_cast<float*>(vec_base->data_ptr()), actual_cols,
                           static_cast<float*>(result_base->data_ptr()));
                break;
            case DType::FLOAT64:
                eigen_gemv(static_cast<double*>(mat_base->data_ptr()), local_rows, local_cols,
                           static_cast<double*>(vec_base->data_ptr()), actual_cols,
                           static_cast<double*>(result_base->data_ptr()));
                break;
            case DType::INT32:
                eigen_gemv(static_cast<int32_t*>(mat_base->data_ptr()), local_rows, local_cols,
                           static_cast<int32_t*>(vec_base->data_ptr()), actual_cols,
                           static_cast<int32_t*>(result_base->data_ptr()));
                break;
            case DType::INT64:
                eigen_gemv(static_cast<int64_t*>(mat_base->data_ptr()), local_rows, local_cols,
                           static_cast<int64_t*>(vec_base->data_ptr()), actual_cols,
                           static_cast<int64_t*>(result_base->data_ptr()));
                break;
            }
#endif
            DBG_PRINT("[Chare %d] MATMUL ast_visitor: %dx%d @ %d done\n", partition->index[0],
                      local_rows, actual_cols, vec_len);
            return result_name;
        } else {
            // N=1: MATMUL on 1D partition is handled via comm path, not ast_visitor
            return node->result_name;
        }
    }
    case Opcode::MATMATMUL: {
        if constexpr (N == 2) {
            // Local matmatmul in ast_visitor (no-comm path, e.g. single chare)
            int a_name = node->operands[0]->result_name;
            int b_name = node->operands[1]->result_name;
            int result_name = node->result_name;

            auto a_it = partition->arrays.find(a_name);
            auto b_it = partition->arrays.find(b_name);
            if (a_it == partition->arrays.end() || b_it == partition->arrays.end()) {
                DBG_PRINT("[Chare %d] MATMATMUL ast_visitor: missing operand arrays\n",
                          partition->index[0]);
                return result_name;
            }

            CTArrayBase<N>* a_base = a_it->second;
            CTArrayBase<N>* b_base = b_it->second;

            // Determine effective sub-block sizes from regions
            int a_local_rows = a_base->region.size(0);
            int a_local_cols = a_base->region.size(1);
            int b_local_rows = b_base->region.size(0);
            int b_local_cols = b_base->region.size(1);

            int sub_rows = a_local_rows;
            int k_size = std::min(a_local_cols, b_local_rows);
            int sub_cols = b_local_cols;

            // Handle slice regions if present
            int a_row_offset = 0, a_col_offset = 0;
            int b_row_offset = 0, b_col_offset = 0;
            if (node->operand_regions.size() >= 2) {
                auto* ar = static_cast<ArrayRegion<2>*>(node->operand_regions[0]);
                auto* br = static_cast<ArrayRegion<2>*>(node->operand_regions[1]);
                // Translate regions to global space using array decomps
                auto a_decomp = a_base->decomp;
                auto b_decomp = b_base->decomp;
                int a_rs = ar->start[0] + a_decomp.offset[0];
                int a_re = ar->stop[0] + a_decomp.offset[0];
                int a_cs = ar->start[1] + a_decomp.offset[1];
                int a_ce = ar->stop[1] + a_decomp.offset[1];
                int b_rs = br->start[0] + b_decomp.offset[0];
                int b_re = br->stop[0] + b_decomp.offset[0];
                int b_cs = br->start[1] + b_decomp.offset[1];
                int b_ce = br->stop[1] + b_decomp.offset[1];

                auto a_chare = a_decomp.chare_region_global(nd_idx);
                int chare_a_row_lo = std::max(a_chare.start[0], a_rs);
                int chare_a_row_hi = std::min(a_chare.stop[0], a_re);
                int chare_a_col_lo = std::max(a_chare.start[1], a_cs);
                int chare_a_col_hi = std::min(a_chare.stop[1], a_ce);
                a_row_offset = chare_a_row_lo - a_chare.start[0];
                a_col_offset = chare_a_col_lo - a_chare.start[1];
                sub_rows = chare_a_row_hi - chare_a_row_lo;

                auto b_chare = b_decomp.chare_region_global(nd_idx);
                int chare_b_row_lo = std::max(b_chare.start[0], b_rs);
                int chare_b_row_hi = std::min(b_chare.stop[0], b_re);
                int chare_b_col_lo = std::max(b_chare.start[1], b_cs);
                int chare_b_col_hi = std::min(b_chare.stop[1], b_ce);
                b_row_offset = chare_b_row_lo - b_chare.start[0];
                b_col_offset = chare_b_col_lo - b_chare.start[1];
                k_size = std::min(chare_a_col_hi - chare_a_col_lo,
                                  chare_b_row_hi - chare_b_row_lo);
                sub_cols = chare_b_col_hi - chare_b_col_lo;
            }

            if (sub_rows <= 0 || k_size <= 0 || sub_cols <= 0)
                return result_name;

            // Create result array
            auto* dag_group2 = static_cast<ArrayDAGGroup*>(group);
            auto c_meta = dag_group2->array_meta.find(result_name);
            int c_M = c_meta->second.global_shape[0];
            int c_N = c_meta->second.global_shape[1];

            if (partition->arrays.find(result_name) == partition->arrays.end()) {
                std::array<int, 2> out_start = {0, 0};
                std::array<int, 2> out_stop = {sub_rows, sub_cols};
                std::array<int, 2> out_step = {1, 1};
                std::array<int, 2> out_gs = {c_M, c_N};
                ArrayRegion<2> out_region(out_start, out_stop, out_step);
                ArrayDecomp<2> out_decomp = dag_group2->array_meta[result_name].template decomp<2>();
                partition->arrays[result_name] = partition->allocate_or_reuse(
                    out_region, out_gs, result_name, dtype, out_decomp);
            }

#ifdef USE_KOKKOS
            // Perform matmatmul on device using KokkosBlas::gemm
            auto kokkos_gemm = [&](auto dummy) {
                using VT = decltype(dummy);
                Kokkos::View<VT**, Kokkos::LayoutRight, DeviceSpace,
                             Kokkos::MemoryTraits<Kokkos::Unmanaged>>
                    d_a(static_cast<VT*>(a_base->device_data_ptr()), a_local_rows, a_local_cols);
                auto d_a_sub = Kokkos::subview(d_a,
                    Kokkos::make_pair(a_row_offset, a_row_offset + sub_rows),
                    Kokkos::make_pair(a_col_offset, a_col_offset + k_size));
                Kokkos::View<VT**, Kokkos::LayoutRight, DeviceSpace,
                             Kokkos::MemoryTraits<Kokkos::Unmanaged>>
                    d_b(static_cast<VT*>(b_base->device_data_ptr()), b_local_rows, b_local_cols);
                auto d_b_sub = Kokkos::subview(d_b,
                    Kokkos::make_pair(b_row_offset, b_row_offset + k_size),
                    Kokkos::make_pair(b_col_offset, b_col_offset + sub_cols));
                Kokkos::View<VT**, Kokkos::LayoutRight, DeviceSpace,
                             Kokkos::MemoryTraits<Kokkos::Unmanaged>>
                    d_c(static_cast<VT*>(partition->arrays[result_name]->device_data_ptr()),
                        sub_rows, sub_cols);
                KokkosBlas::gemm("N", "N", VT(1), d_a_sub, d_b_sub, VT(1), d_c);
            };
            switch (dtype) {
            case DType::FLOAT32: kokkos_gemm(float{}); break;
            case DType::FLOAT64: kokkos_gemm(double{}); break;
            case DType::INT32: kokkos_gemm(int32_t{}); break;
            case DType::INT64: kokkos_gemm(int64_t{}); break;
            }
#else
            a_base->copyToHost();
            b_base->copyToHost();

            switch (dtype) {
            case DType::FLOAT32:
                eigen_gemm_sub(static_cast<float*>(a_base->data_ptr()), a_local_cols,
                               a_row_offset, a_col_offset, sub_rows, k_size,
                               static_cast<float*>(b_base->data_ptr()), b_local_cols,
                               b_row_offset, b_col_offset, sub_cols,
                               static_cast<float*>(partition->arrays[result_name]->data_ptr()));
                break;
            case DType::FLOAT64:
                eigen_gemm_sub(static_cast<double*>(a_base->data_ptr()), a_local_cols,
                               a_row_offset, a_col_offset, sub_rows, k_size,
                               static_cast<double*>(b_base->data_ptr()), b_local_cols,
                               b_row_offset, b_col_offset, sub_cols,
                               static_cast<double*>(partition->arrays[result_name]->data_ptr()));
                break;
            case DType::INT32:
                eigen_gemm_sub(static_cast<int32_t*>(a_base->data_ptr()), a_local_cols,
                               a_row_offset, a_col_offset, sub_rows, k_size,
                               static_cast<int32_t*>(b_base->data_ptr()), b_local_cols,
                               b_row_offset, b_col_offset, sub_cols,
                               static_cast<int32_t*>(partition->arrays[result_name]->data_ptr()));
                break;
            case DType::INT64:
                eigen_gemm_sub(static_cast<int64_t*>(a_base->data_ptr()), a_local_cols,
                               a_row_offset, a_col_offset, sub_rows, k_size,
                               static_cast<int64_t*>(b_base->data_ptr()), b_local_cols,
                               b_row_offset, b_col_offset, sub_cols,
                               static_cast<int64_t*>(partition->arrays[result_name]->data_ptr()));
                break;
            }
#endif
            DBG_PRINT("[Chare %d] MATMATMUL ast_visitor: (%dx%d) @ (%dx%d) done\n",
                      partition->index[0], sub_rows, k_size, k_size, sub_cols);
            return result_name;
        } else {
            return node->result_name;
        }
    }
    case Opcode::DIAG:
        // DIAG is always handled via the comm path (execute_diag_node).
        return node->result_name;
    case Opcode::TILE:
        // TILE is always handled via the comm path (execute_tile_node).
        return node->result_name;
    default:
        CkAbort("Unknown opcode in interpreter: %d", node->opcode);
    }
}

// Explicit template instantiations for executor
template class ArrayDAGExecutorND<1>;
template class ArrayDAGExecutorND<2>;
template class ArrayDAGExecutorND<3>;
