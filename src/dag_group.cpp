#include "backend.hpp"
#include "backend_internal.hpp"
#include "jit.hpp"
#include <algorithm>
#include <queue>
#include <set>
#include <unordered_set>

// ---- ArrayDAGGroup ----

ArrayDAGGroup::ArrayDAGGroup() : num_compile(0) {
#ifdef USE_KOKKOS
    if (!Kokkos::is_initialized())
        Kokkos::initialize();
#endif
    register_reduce_dot_sum();
    jit = new MLIRJitCompiler();
    if (CkMyPe() == 0) {
        partition_proxy_1 = CProxy_Partition1D::ckNew();
        partition_proxy_2 = CProxy_Partition2D::ckNew();
        partition_proxy_3 = CProxy_Partition3D::ckNew();
        thisProxy.set_proxies(partition_proxy_1, partition_proxy_2, partition_proxy_3);
    }
}

void ArrayDAGGroup::set_proxies(CkArrayID p1, CkArrayID p2, CkArrayID p3) {
    partition_proxy_1 = CProxy_Partition1D(p1);
    partition_proxy_2 = CProxy_Partition2D(p2);
    partition_proxy_3 = CProxy_Partition3D(p3);
    CkCallback cb(CkReductionTarget(ArrayDAGGroup, proxies_ready), thisProxy[0]);
    contribute(cb);
}

void ArrayDAGGroup::proxies_ready() { 
    Server<CProxy_ArrayDAGGroup>::initialize(thisProxy); 
}

ArrayDAGGroup::~ArrayDAGGroup() {
    delete jit;
#ifdef USE_KOKKOS
    if (Kokkos::is_initialized())
        Kokkos::finalize();
#endif
}

/// Runtime region deserializer: reads start/stop/step for ndims dimensions
/// and returns the correctly-typed ArrayRegion<N>.
template <int N>
static Region* deserialize_array_region_impl(char*& msg) {
    std::array<int, N> s, e, st;
    for (int d = 0; d < N; ++d) {
        s[d] = extract<int>(msg);
        e[d] = extract<int>(msg);
        st[d] = extract<int>(msg);
    }
    return new ArrayRegion<N>(s, e, st);
}

static Region* deserialize_array_region(int ndims, char*& msg) {
    switch (ndims) {
    case 1:
        return deserialize_array_region_impl<1>(msg);
    case 2:
        return deserialize_array_region_impl<2>(msg);
    case 3:
        return deserialize_array_region_impl<3>(msg);
    default:
        CkAbort("Unsupported ndims=%d in deserialize_array_region", ndims);
        return nullptr;
    }
}

static bool region_to_shape(Region* region, int ndims, std::array<int, 3>& shape_out) {
    shape_out = {0, 0, 0};
    if (region == nullptr || region->is_global)
        return false;

    switch (ndims) {
    case 1: {
        auto* r = static_cast<ArrayRegion<1>*>(region);
        shape_out[0] = r->size(0);
        return true;
    }
    case 2: {
        auto* r = static_cast<ArrayRegion<2>*>(region);
        shape_out[0] = r->size(0);
        shape_out[1] = r->size(1);
        return true;
    }
    case 3: {
        auto* r = static_cast<ArrayRegion<3>*>(region);
        shape_out[0] = r->size(0);
        shape_out[1] = r->size(1);
        shape_out[2] = r->size(2);
        return true;
    }
    default:
        return false;
    }
}

template <int N>
static void expand_partition_nd(CProxy_ArrayDAGGroup proxy,
                                typename PartitionTraits<N>::ProxyType& part_proxy,
                                int* current_grid, Region* region_base, int tile);

static void expand_partition_for_shape(CProxy_ArrayDAGGroup proxy, ArrayDAGGroup& dag_group,
                                       int epoch, int ndims,
                                       const std::array<int, 3>& shape) {
    int tile = ct_tile(ndims);
    if (dag_group.partition_grid[ndims].start_epoch < 0)
        dag_group.partition_grid[ndims].start_epoch = epoch;

    switch (ndims) {
    case 1: {
        std::array<int, 1> s = {0}, e = {shape[0]}, st = {1};
        ArrayRegion<1> result_region(s, e, st);
        expand_partition_nd<1>(proxy, dag_group.partition_proxy_1,
                               dag_group.partition_grid[ndims].grid, &result_region, tile);
        break;
    }
    case 2: {
        std::array<int, 2> s = {0, 0}, e = {shape[0], shape[1]}, st = {1, 1};
        ArrayRegion<2> result_region(s, e, st);
        expand_partition_nd<2>(proxy, dag_group.partition_proxy_2,
                               dag_group.partition_grid[ndims].grid, &result_region, tile);
        break;
    }
    case 3: {
        std::array<int, 3> s = {0, 0, 0}, e = {shape[0], shape[1], shape[2]}, st = {1, 1, 1};
        ArrayRegion<3> result_region(s, e, st);
        expand_partition_nd<3>(proxy, dag_group.partition_proxy_3,
                               dag_group.partition_grid[ndims].grid, &result_region, tile);
        break;
    }
    default:
        CkAbort("Unsupported ndims=%d for partition expansion", ndims);
    }
}

static bool infer_result_shape_from_operands(
    ASTNode* ast_node, const std::unordered_map<int, ArrayDAGGroup::ArrayMetadata>& array_meta,
    std::array<int, 3>& shape_out) {
    shape_out = {0, 0, 0};
    bool found = false;

    for (int op_idx = 0; op_idx < (int)ast_node->operands.size(); ++op_idx) {
        ASTNode* operand = ast_node->operands[op_idx];
        if (operand == nullptr || operand->is_scalar)
            continue;

        std::array<int, 3> candidate = {0, 0, 0};
        bool have_candidate = false;

        Region* operand_region = ast_node->get_operand_region(op_idx);
        if (operand_region && !operand_region->is_global)
            have_candidate = region_to_shape(operand_region, operand->ndims, candidate);

        if (!have_candidate) {
            auto meta_it = array_meta.find(operand->result_name);
            if (meta_it != array_meta.end()) {
                candidate = meta_it->second.global_shape;
                have_candidate = true;
            }
        }

        if (!have_candidate)
            continue;

        shape_out = candidate;
        found = true;

        // Prefer a non-broadcast operand when available; size-1 broadcast
        // operands are only a fallback when every array-like operand is scalar-shaped.
        if (!operand->is_broadcast)
            return true;
    }

    return found;
}

template <typename Fn>
static void for_each_node_topo(DAG* dag, Fn&& fn) {
    std::queue<DAGNode*> topo_queue;
    std::unordered_map<DAGNode*, int> remaining_parents;
    remaining_parents.reserve(dag->nodes.size());

    for (auto& [id, node] : dag->nodes) {
        remaining_parents[node] = node->num_parents;
        if (node->num_parents == 0)
            topo_queue.push(node);
    }

    int processed = 0;
    while (!topo_queue.empty()) {
        DAGNode* dag_node = topo_queue.front();
        topo_queue.pop();
        processed++;
        fn(dag_node);

        for (DAGNode* child : dag_node->children) {
            auto it = remaining_parents.find(child);
            if (it == remaining_parents.end())
                continue;
            if (--it->second == 0)
                topo_queue.push(child);
        }
    }

    if (processed != static_cast<int>(dag->nodes.size()))
        CkAbort("for_each_node_topo processed %d/%d nodes", processed,
                static_cast<int>(dag->nodes.size()));
}


/// Helper: insert a single chare into the partition array at the given N-D index.
template <int N>
static void insert_chare(typename PartitionTraits<N>::ProxyType& part_proxy,
                         CProxy_ArrayDAGGroup proxy, const std::array<int, N>& idx) {
    ChareIndex<N> ci;
    for (int d = 0; d < N; ++d)
        ci.idx[d] = idx[d];
    proxy_at<N>(part_proxy, ci).insert(proxy);
}

/// Expand a Partition chare array to cover the given region.
/// Only iterates over new indices — those where at least one dimension
/// exceeds the previous grid.
template <int N>
static void expand_partition_nd(CProxy_ArrayDAGGroup proxy,
                                typename PartitionTraits<N>::ProxyType& part_proxy,
                                int* current_grid, Region* region_base, int tile) {
    auto* region = static_cast<ArrayRegion<N>*>(region_base);
    int needed[N];

    bool needs_expansion = false;
    for (int d = 0; d < N; ++d) {
        needed[d] = (region->size(d) + tile - 1) / tile;
        if (needed[d] > current_grid[d])
            needs_expansion = true;
    }

    if (!needs_expansion)
        return;

    int expanded[N];
    for (int d = 0; d < N; ++d)
        expanded[d] = std::max(current_grid[d], needed[d]);

    if (CkMyPe() == 0) {
        part_proxy.beginInserting();

        // Insert new chares by iterating dimension-by-dimension over the
        // "L-shaped" expansion region.  For each dimension d where the grid
        // grew, iterate over the slab [old..expanded) in that dimension
        // with [0..expanded) in dimensions > d and [0..old) in dimensions < d
        // (the latter are already covered by earlier slabs).
        int inserted = 0;
        for (int dim = 0; dim < N; ++dim) {
            if (expanded[dim] <= current_grid[dim])
                continue; // this dimension didn't grow

            // Iterate over the slab: dimension dim ranges [current..expanded),
            // dimensions < dim range [0..current_grid[d]) (already existing),
            // dimensions > dim range [0..expanded[d]).
            int slab_total = 1;
            int slab_extent[N];
            int slab_start[N];
            for (int d = 0; d < N; ++d) {
                if (d < dim) {
                    slab_start[d] = 0;
                    slab_extent[d] = current_grid[d];
                } else if (d == dim) {
                    slab_start[d] = current_grid[d];
                    slab_extent[d] = expanded[d] - current_grid[d];
                } else {
                    slab_start[d] = 0;
                    slab_extent[d] = expanded[d];
                }
                slab_total *= slab_extent[d];
            }

            std::array<int, N> idx;
            for (int d = 0; d < N; ++d)
                idx[d] = slab_start[d];

            for (int i = 0; i < slab_total; ++i) {
                insert_chare<N>(part_proxy, proxy, idx);
                inserted++;

                // Advance odometer within the slab
                for (int d = N - 1; d >= 0; --d) {
                    if (++idx[d] < slab_start[d] + slab_extent[d])
                        break;
                    idx[d] = slab_start[d];
                }
            }
        }

        part_proxy.doneInserting();
        DBG_PRINT("Partition<%d>: expanded grid, inserted %d chares\n", N, inserted);
    }

    for (int d = 0; d < N; ++d)
        current_grid[d] = expanded[d];
}

void ArrayDAGGroup::receive_dag(int epoch, int size, char* serialized_dag) {
    DAG* dag = DAG::deserialize(serialized_dag, deserialize_array_region);

    // Collect which ndims are used in this DAG
    std::set<int> active_ndims;
    for (auto& [id, dag_node] : dag->nodes) {
        for (ASTNode* ast_node : dag_node->ast->roots) {
            if (ast_node->ndims >= 1 && ast_node->ndims <= 3)
                active_ndims.insert(ast_node->ndims);
            for (ASTNode* operand : ast_node->operands)
                if (!operand->is_scalar && !operand->is_broadcast && operand->ndims >= 1 && operand->ndims <= 3)
                    active_ndims.insert(operand->ndims);
            // DIAG produces a result with different ndims than its input
            if (static_cast<Opcode>(ast_node->opcode) == Opcode::DIAG) {
                active_ndims.insert(1);
                active_ndims.insert(2);
            }
            // TILE may produce a result with different ndims than its input
            if (static_cast<Opcode>(ast_node->opcode) == Opcode::TILE) {
                int input_nd = ast_node->operands[0]->ndims;
                int result_nd = ast_node->ndims;
                active_ndims.insert(input_nd);
                active_ndims.insert(result_nd);
            }
        }
    }
    DBG_PRINT("[PE %d] receive_dag epoch=%d: active_ndims={", CkMyPe(), epoch);
    for (int nd : active_ndims)
        DBG_PRINT(" %d", nd);
    DBG_PRINT(" }, num_nodes=%d\n", (int)dag->nodes.size());

    // -----------------------------------------------------------------------
    // Pass 1: Populate array_meta shapes in topological order.
    // NO partition expansion here — tile sizes haven't been decided yet.
    // Topological order ensures each array's operands are visited before it.
    // -----------------------------------------------------------------------
    {
        for_each_node_topo(dag, [&](DAGNode* dag_node) {
            for (ASTNode* ast_node : dag_node->ast->roots) {
                Opcode op = static_cast<Opcode>(ast_node->opcode);

                if (op == Opcode::CREATE && ast_node->region) {
                    int nd = ast_node->ndims;
                    switch (nd) {
                    case 1: {
                        auto* r = static_cast<ArrayRegion<1>*>(ast_node->region);
                        array_meta[ast_node->result_name] = {nd, {r->size(0), 0, 0}, {}, 0};
                        break;
                    }
                    case 2: {
                        auto* r = static_cast<ArrayRegion<2>*>(ast_node->region);
                        array_meta[ast_node->result_name] = {nd, {r->size(0), r->size(1), 0}, {}, 0};
                        break;
                    }
                    case 3: {
                        auto* r = static_cast<ArrayRegion<3>*>(ast_node->region);
                        array_meta[ast_node->result_name] = {nd, {r->size(0), r->size(1), r->size(2)}, {}, 0};
                        break;
                    }
                    default:
                        CkAbort("Unsupported ndims=%d for partition creation", nd);
                    }
                } else if (op == Opcode::REDUCE) {
                    array_meta[ast_node->result_name] = {1, {1, 0, 0}, {}, 0};
                } else if (op == Opcode::MATMUL) {
                    int mat_op_name = ast_node->operands[0]->result_name;
                    auto mat_it = array_meta.find(mat_op_name);
                    if (mat_it != array_meta.end()) {
                        int mat_ndims = mat_it->second.ndims;
                        int mat_rows = mat_it->second.global_shape[0];
                        if (ast_node->operand_regions.size() >= 1) {
                            if (mat_ndims == 3) {
                                auto* mr = static_cast<ArrayRegion<3>*>(ast_node->operand_regions[0]);
                                int dropped = -1;
                                for (int d = 0; d < 3; d++) {
                                    if (mr->stop[d] - mr->start[d] == 1) { dropped = d; break; }
                                }
                                int row_dim = (dropped == 0) ? 1 : 0;
                                mat_rows = mr->stop[row_dim] - mr->start[row_dim];
                            } else {
                                auto* mr = static_cast<ArrayRegion<2>*>(ast_node->operand_regions[0]);
                                mat_rows = mr->stop[0] - mr->start[0];
                            }
                        }
                        array_meta[ast_node->result_name] = {1, {mat_rows, 0, 0}, {}, 0};
                    }
                } else if (op == Opcode::MATMATMUL) {
                    int lhs_name = ast_node->operands[0]->result_name;
                    int rhs_name = ast_node->operands[1]->result_name;
                    auto lhs_it = array_meta.find(lhs_name);
                    auto rhs_it = array_meta.find(rhs_name);
                    if (lhs_it != array_meta.end() && rhs_it != array_meta.end()) {
                        int M, N_cols;
                        if (ast_node->operand_regions.size() >= 2) {
                            int lhs_ndims = lhs_it->second.ndims;
                            int rhs_ndims = rhs_it->second.ndims;
                            if (lhs_ndims == 3) {
                                auto* lr = static_cast<ArrayRegion<3>*>(ast_node->operand_regions[0]);
                                int dd = -1;
                                for (int d = 0; d < 3; d++)
                                    if (lr->stop[d] - lr->start[d] == 1) { dd = d; break; }
                                int rd = (dd == 0) ? 1 : 0;
                                M = lr->stop[rd] - lr->start[rd];
                            } else {
                                auto* lr = static_cast<ArrayRegion<2>*>(ast_node->operand_regions[0]);
                                M = lr->stop[0] - lr->start[0];
                            }
                            if (rhs_ndims == 3) {
                                auto* rr = static_cast<ArrayRegion<3>*>(ast_node->operand_regions[1]);
                                int dd = -1;
                                for (int d = 0; d < 3; d++)
                                    if (rr->stop[d] - rr->start[d] == 1) { dd = d; break; }
                                int cd = (dd <= 1) ? 2 : 1;
                                N_cols = rr->stop[cd] - rr->start[cd];
                            } else {
                                auto* rr = static_cast<ArrayRegion<2>*>(ast_node->operand_regions[1]);
                                N_cols = rr->stop[1] - rr->start[1];
                            }
                        } else {
                            M = lhs_it->second.global_shape[0];
                            N_cols = rhs_it->second.global_shape[1];
                        }
                        array_meta[ast_node->result_name] = {2, {M, N_cols, 0}, {}, 0};
                    }
                } else if (op == Opcode::DIAG) {
                    int input_name = ast_node->operands[0]->result_name;
                    auto input_it = array_meta.find(input_name);
                    if (input_it != array_meta.end()) {
                        int input_ndims = input_it->second.ndims;
                        int k_offset = 0;
                        if (ast_node->operands.size() >= 2 && ast_node->operands[1]->is_scalar)
                            k_offset = (int)ast_node->operands[1]->scalar;
                        if (input_ndims == 1) {
                            int vec_len = input_it->second.global_shape[0];
                            int n = vec_len + std::abs(k_offset);
                            array_meta[ast_node->result_name] = {2, {n, n, 0}, {}, 0};
                        } else if (input_ndims == 2) {
                            int M = input_it->second.global_shape[0];
                            int N_cols = input_it->second.global_shape[1];
                            int diag_len;
                            if (k_offset >= 0)
                                diag_len = std::max(0, std::min(M, N_cols - k_offset));
                            else
                                diag_len = std::max(0, std::min(M + k_offset, N_cols));
                            array_meta[ast_node->result_name] = {1, {diag_len, 0, 0}, {}, 0};
                        }
                    }
                } else if (op == Opcode::TILE) {
                    int input_name = ast_node->operands[0]->result_name;
                    auto input_it = array_meta.find(input_name);
                    if (input_it != array_meta.end()) {
                        int input_ndims = input_it->second.ndims;
                        int out_ndims = ast_node->ndims;
                        // Extract reps from scalar operands[1..N]
                        std::array<int, 3> reps = {1, 1, 1};
                        int num_reps = (int)ast_node->operands.size() - 1;
                        for (int d = 0; d < num_reps && d < 3; d++) {
                            if (ast_node->operands[d + 1]->is_scalar)
                                reps[d] = (int)ast_node->operands[d + 1]->scalar;
                        }
                        // Pad input shape: prepend 1s if out_ndims > input_ndims
                        int delta = out_ndims - input_ndims;
                        std::array<int, 3> out_shape = {0, 0, 0};
                        for (int d = 0; d < out_ndims; d++) {
                            int in_dim = (d < delta) ? 1 : input_it->second.global_shape[d - delta];
                            out_shape[d] = in_dim * reps[d];
                        }
                        array_meta[ast_node->result_name] = {out_ndims, out_shape, {}, 0};
                    }
                } else if (op == Opcode::SET_REGION) {
                    // SET_REGION writes into the target array's partition;
                    // it does not create a new array and needs no metadata entry.
                } else if (is_elementwise(op) || op == Opcode::COPY) {
                    if (ast_node->is_temp)
                        continue;
                    std::array<int, 3> inferred_shape = {0, 0, 0};
                    if (infer_result_shape_from_operands(ast_node, array_meta, inferred_shape)) {
                        array_meta[ast_node->result_name] = {ast_node->ndims, inferred_shape, {}, 0};
                    } else {
                        array_meta[ast_node->result_name] = {1, {1, 0, 0}, {}, 0};
                    }
                } else if (op != Opcode::NOOP) {
                    bool found = false;
                    for (ASTNode* operand : ast_node->operands) {
                        if (operand->is_scalar || operand->is_broadcast)
                            continue;
                        auto it = array_meta.find(operand->result_name);
                        if (it != array_meta.end()) {
                            array_meta[ast_node->result_name] = it->second;
                            found = true;
                            break;
                        }
                    }
                    if (!found)
                        array_meta[ast_node->result_name] = {1, {1, 0, 0}, {}, 0};
                }
            }
        });
    }
    DBG_PRINT("[PE %d] receive_dag epoch=%d: metadata pass complete\n", CkMyPe(), epoch);

    // Assign tile sizes and offsets to all arrays before any partition is expanded.
    // This is the key ordering guarantee: tile drives chare count, so it must be
    // decided here, before expand_partition_nd is called in Pass 2 below.
    compute_decompositions(dag);
    for (auto& [name, live_meta] : live_array_meta) {
        auto meta_it = array_meta.find(name);
        if (meta_it == array_meta.end()) {
            array_meta[name] = live_meta;
            continue;
        }
        meta_it->second.ndims = live_meta.ndims;
        meta_it->second.global_shape = live_meta.global_shape;
        meta_it->second.tile = live_meta.tile;
        meta_it->second.offset = live_meta.offset;
    }

    // -----------------------------------------------------------------------
    // Pass 2: Expand partitions now that tile sizes have been assigned.
    // All expand_partition_nd calls use array_meta[name].tile rather than
    // the hard-coded ct_tile(ndims) constant.
    // -----------------------------------------------------------------------
    {
        for_each_node_topo(dag, [&](DAGNode* dag_node) {
            for (ASTNode* ast_node : dag_node->ast->roots) {
                Opcode op = static_cast<Opcode>(ast_node->opcode);

                if (op == Opcode::CREATE && ast_node->region) {
                    int nd = ast_node->ndims;
                    int tile = array_meta[ast_node->result_name].tile;
                    if (partition_grid[nd].start_epoch < 0)
                        partition_grid[nd].start_epoch = epoch;
                    switch (nd) {
                    case 1:
                        expand_partition_nd<1>(thisProxy, partition_proxy_1,
                                               partition_grid[nd].grid, ast_node->region, tile);
                        break;
                    case 2:
                        expand_partition_nd<2>(thisProxy, partition_proxy_2,
                                               partition_grid[nd].grid, ast_node->region, tile);
                        break;
                    case 3:
                        expand_partition_nd<3>(thisProxy, partition_proxy_3,
                                               partition_grid[nd].grid, ast_node->region, tile);
                        break;
                    default:
                        CkAbort("Unsupported ndims=%d in expansion pass", nd);
                    }
                } else if (op == Opcode::MATMUL) {
                    auto res_it = array_meta.find(ast_node->result_name);
                    if (res_it != array_meta.end()) {
                        int mat_rows = res_it->second.global_shape[0];
                        int tile_1d  = res_it->second.tile;
                        std::array<int, 1> s = {0}, e = {mat_rows}, st = {1};
                        ArrayRegion<1> result_region(s, e, st);
                        expand_partition_nd<1>(thisProxy, partition_proxy_1,
                                               partition_grid[1].grid, &result_region, tile_1d);
                    }
                } else if (op == Opcode::MATMATMUL) {
                    auto res_it = array_meta.find(ast_node->result_name);
                    if (res_it != array_meta.end()) {
                        int M      = res_it->second.global_shape[0];
                        int N_cols = res_it->second.global_shape[1];
                        int tile_2d = res_it->second.tile;
                        std::array<int, 2> s = {0, 0}, e = {M, N_cols}, st = {1, 1};
                        ArrayRegion<2> result_region(s, e, st);
                        expand_partition_nd<2>(thisProxy, partition_proxy_2,
                                               partition_grid[2].grid, &result_region, tile_2d);
                    }
                } else if (op == Opcode::DIAG) {
                    auto res_it = array_meta.find(ast_node->result_name);
                    if (res_it != array_meta.end()) {
                        int res_nd = res_it->second.ndims;
                        int tile   = res_it->second.tile;
                        if (res_nd == 2) {
                            // 1D → 2D diagonal matrix
                            int n = res_it->second.global_shape[0];
                            std::array<int, 2> s = {0, 0}, e = {n, n}, st = {1, 1};
                            ArrayRegion<2> result_region(s, e, st);
                            if (partition_grid[2].start_epoch < 0)
                                partition_grid[2].start_epoch = epoch;
                            expand_partition_nd<2>(thisProxy, partition_proxy_2,
                                                   partition_grid[2].grid, &result_region, tile);
                        } else {
                            // 2D → 1D diagonal extraction
                            int diag_len = res_it->second.global_shape[0];
                            std::array<int, 1> s = {0}, e = {diag_len}, st = {1};
                            ArrayRegion<1> result_region(s, e, st);
                            if (partition_grid[1].start_epoch < 0)
                                partition_grid[1].start_epoch = epoch;
                            expand_partition_nd<1>(thisProxy, partition_proxy_1,
                                                   partition_grid[1].grid, &result_region, tile);
                        }
                    }
                } else if (op == Opcode::TILE) {
                    auto res_it = array_meta.find(ast_node->result_name);
                    if (res_it != array_meta.end()) {
                        int res_nd = res_it->second.ndims;
                        int tile   = res_it->second.tile;
                        auto& sh   = res_it->second.global_shape;
                        if (partition_grid[res_nd].start_epoch < 0)
                            partition_grid[res_nd].start_epoch = epoch;
                        switch (res_nd) {
                        case 1: {
                            std::array<int, 1> s = {0}, e = {sh[0]}, st = {1};
                            ArrayRegion<1> result_region(s, e, st);
                            expand_partition_nd<1>(thisProxy, partition_proxy_1,
                                                   partition_grid[res_nd].grid, &result_region, tile);
                            break;
                        }
                        case 2: {
                            std::array<int, 2> s = {0, 0}, e = {sh[0], sh[1]}, st = {1, 1};
                            ArrayRegion<2> result_region(s, e, st);
                            expand_partition_nd<2>(thisProxy, partition_proxy_2,
                                                   partition_grid[res_nd].grid, &result_region, tile);
                            break;
                        }
                        case 3: {
                            std::array<int, 3> s = {0, 0, 0}, e = {sh[0], sh[1], sh[2]}, st = {1, 1, 1};
                            ArrayRegion<3> result_region(s, e, st);
                            expand_partition_nd<3>(thisProxy, partition_proxy_3,
                                                   partition_grid[res_nd].grid, &result_region, tile);
                            break;
                        }
                        }
                    }
                } else if (is_elementwise(op) || op == Opcode::COPY) {
                    auto res_it = array_meta.find(ast_node->result_name);
                    if (res_it != array_meta.end() && res_it->second.global_shape[0] > 0) {
                        int nd   = res_it->second.ndims;
                        int tile = res_it->second.tile;
                        auto& sh = res_it->second.global_shape;
                        if (partition_grid[nd].start_epoch < 0)
                            partition_grid[nd].start_epoch = epoch;
                        switch (nd) {
                        case 1: {
                            std::array<int, 1> s = {0}, e = {sh[0]}, st = {1};
                            ArrayRegion<1> region(s, e, st);
                            expand_partition_nd<1>(thisProxy, partition_proxy_1,
                                                   partition_grid[nd].grid, &region, tile);
                            break;
                        }
                        case 2: {
                            std::array<int, 2> s = {0, 0}, e = {sh[0], sh[1]}, st = {1, 1};
                            ArrayRegion<2> region(s, e, st);
                            expand_partition_nd<2>(thisProxy, partition_proxy_2,
                                                   partition_grid[nd].grid, &region, tile);
                            break;
                        }
                        case 3: {
                            std::array<int, 3> s = {0, 0, 0},
                                               e = {sh[0], sh[1], sh[2]},
                                               st = {1, 1, 1};
                            ArrayRegion<3> region(s, e, st);
                            expand_partition_nd<3>(thisProxy, partition_proxy_3,
                                                   partition_grid[nd].grid, &region, tile);
                            break;
                        }
                        }
                    }
                }
                // REDUCE, SET_REGION, NOOP, and other ops don't expand partitions.
            }
        });
    }
    DBG_PRINT("[PE %d] receive_dag epoch=%d: partition expansion pass complete\n",
              CkMyPe(), epoch);

    compile(dag);

    // Store a copy of the DAG for each active partition type
    bool first = true;
    for (int nd : active_ndims) {
        if (first) {
            add_dag(nd, epoch, dag);
            first = false;
        } else {
            add_dag(nd, epoch, dag->copy());
        }
    }

    // Store empty DAGs for ndims that have active partitions but no work in this epoch.
    // This ensures their executors can advance past this epoch instead of spinning on NONE.
    for (auto& [nd, pg] : partition_grid) {
        if (pg.grid[0] > 0 && active_ndims.find(nd) == active_ndims.end()) {
            add_dag(nd, epoch, new DAG());
            DBG_PRINT("[PE %d] receive_dag epoch=%d: stored empty DAG for ndims=%d\n",
                      CkMyPe(), epoch, nd);
        }
    }

    // Wake up all partition chares so they can check for new work.
    partition_proxy_1.run();
    partition_proxy_2.run();
    partition_proxy_3.run();
}

void ArrayDAGGroup::receive_get_request(int ndims, int epoch, int name, int size, int dtype) {
    DAGGroup::receive_get_request(ndims, epoch, name, size, dtype);

    // Store empty DAGs for ndims that have active partitions but aren't involved in this GET.
    for (auto& [nd, pg] : partition_grid) {
        if (pg.grid[0] > 0 && nd != ndims) {
            add_dag(nd, epoch, new DAG());
        }
    }

    // Wake up all partition chares so they can check for new work.
    partition_proxy_1.run();
    partition_proxy_2.run();
    partition_proxy_3.run();

    // Only PE 0 allocates the gather buffer for assembly
    // size is element count; convert to bytes using the wire dtype
    int elem_size = dtype_size(static_cast<DType>(dtype));
    int64_t byte_size = (int64_t)size * elem_size;
    if (CkMyPe() == 0) {
        gather_buffers[epoch] = new char[byte_size];
        gather_total[epoch] = byte_size;
        if (gather_counts.find(epoch) == gather_counts.end())
            gather_counts[epoch] = 0;

        // Flush any gather fragments that arrived before this allocation
        auto early_it = gather_early.find(epoch);
        if (early_it != gather_early.end()) {
            for (auto& frag : early_it->second) {
                memcpy(gather_buffers[epoch] + frag.offset, frag.data, frag.size);
                delete[] frag.data;
            }
            gather_early.erase(early_it);
            if (gather_counts[epoch] >= gather_total[epoch]) {
                Server<CProxy_ArrayDAGGroup>::send_reply(epoch, byte_size, gather_buffers[epoch]);
                gather_counts.erase(epoch);
                gather_total.erase(epoch);
                delete[] gather_buffers[epoch];
                gather_buffers.erase(epoch);
            }
        }
    }
}

void ArrayDAGGroup::gather(int epoch, int name, int64_t offset, int64_t size, char* data) {
    // offset and size are in bytes
    if (gather_buffers.find(epoch) == gather_buffers.end()) {
        char* buf = new char[size];
        memcpy(buf, data, size);
        gather_early[epoch].push_back({offset, size, buf});
        gather_counts[epoch] += size;
        return;
    }

    memcpy(gather_buffers[epoch] + offset, data, size);
    gather_counts[epoch] += size;
    if (gather_counts[epoch] >= gather_total[epoch]) {
        Server<CProxy_ArrayDAGGroup>::send_reply(epoch, gather_total[epoch], gather_buffers[epoch]);
        gather_counts.erase(epoch);
        gather_total.erase(epoch);
        delete[] gather_buffers[epoch];
        gather_buffers.erase(epoch);
    }
}

void ArrayDAGGroup::compute_decompositions(DAG* dag) {
    auto zero_offset = std::array<int, 3>{0, 0, 0};
    for (auto& [name, meta] : array_meta) {
        if (meta.tile == 0)
            meta.tile = ct_tile(meta.ndims);
        if (!meta.decomp_final)
            meta.offset = zero_offset;
    }

    if (dag != nullptr) {
        struct OffsetConstraint {
            std::array<int, 3> desired;
            int64_t weight;
        };

        std::unordered_map<int, OffsetConstraint> best_constraint;
        std::unordered_set<int> matmul_arrays;

        auto positive_mod = [](int value, int mod) {
            int r = value % mod;
            return r < 0 ? r + mod : r;
        };

        auto gcd_positive = [](int a, int b) {
            if (a < 0)
                a = -a;
            if (b < 0)
                b = -b;
            while (b != 0) {
                int t = a % b;
                a = b;
                b = t;
            }
            return a;
        };

        auto region_stride_factor = [&](Region* region_base, int ndims) {
            if (region_base == nullptr || region_base->is_global)
                return 1;

            int factor = 0;
            auto fold_step = [&](int step) {
                if (step < 0)
                    step = -step;
                if (step <= 1)
                    return;
                factor = (factor == 0) ? step : gcd_positive(factor, step);
            };

            switch (ndims) {
            case 1: {
                auto* r = static_cast<ArrayRegion<1>*>(region_base);
                fold_step(r->step[0]);
                break;
            }
            case 2: {
                auto* r = static_cast<ArrayRegion<2>*>(region_base);
                fold_step(r->step[0]);
                fold_step(r->step[1]);
                break;
            }
            case 3: {
                auto* r = static_cast<ArrayRegion<3>*>(region_base);
                fold_step(r->step[0]);
                fold_step(r->step[1]);
                fold_step(r->step[2]);
                break;
            }
            default:
                break;
            }

            return factor > 1 ? factor : 1;
        };

        auto region_is_dense = [](Region* region_base, int ndims) {
            if (region_base == nullptr || region_base->is_global)
                return true;

            switch (ndims) {
            case 1: {
                auto* r = static_cast<ArrayRegion<1>*>(region_base);
                return r->step[0] == 1;
            }
            case 2: {
                auto* r = static_cast<ArrayRegion<2>*>(region_base);
                return r->step[0] == 1 && r->step[1] == 1;
            }
            case 3: {
                auto* r = static_cast<ArrayRegion<3>*>(region_base);
                return r->step[0] == 1 && r->step[1] == 1 && r->step[2] == 1;
            }
            default:
                return false;
            }
        };

        auto tile_fits_grid = [](const ArrayMetadata& meta, int tile) {
            return tile > 0;
        };

        auto try_assign_tile = [&](int array_name, int candidate_tile) {
            auto array_it = array_meta.find(array_name);
            if (array_it == array_meta.end())
                return false;

            auto& meta = array_it->second;
            if (meta.decomp_final)
                return false;
            if (candidate_tile <= 0)
                return false;
            if (meta.tile > 0 && candidate_tile >= meta.tile)
                return false;
            if (!tile_fits_grid(meta, candidate_tile))
                return false;

            meta.tile = candidate_tile;
            return true;
        };

        auto propose_tile_from_access = [&](int array_name, int ndims, int source_name,
                                            Region* access_region) {
            if (source_name == array_name)
                return false;

            auto src_it = array_meta.find(source_name);
            if (src_it == array_meta.end() || src_it->second.ndims != ndims)
                return false;

            int candidate_tile = src_it->second.tile;
            int stride_factor = region_stride_factor(access_region, ndims);
            if (stride_factor > 1)
                candidate_tile = std::max(1, candidate_tile / stride_factor);

            return try_assign_tile(array_name, candidate_tile);
        };

        auto collect_tile_candidates = [&](auto&& self, int array_name, int ndims, ASTNode* node,
                                           Region* carried_region) -> bool {
            if (node == nullptr || node->is_scalar || node->is_broadcast)
                return false;

            auto meta_it = array_meta.find(node->result_name);
            if (meta_it != array_meta.end()) {
                if (meta_it->second.ndims == ndims)
                    return propose_tile_from_access(array_name, ndims, node->result_name,
                                                    carried_region);
                return false;
            }

            bool changed = false;
            for (int child_idx = 0; child_idx < (int)node->operands.size(); ++child_idx) {
                changed = self(self, array_name, ndims, node->operands[child_idx],
                               node->get_operand_region(child_idx)) || changed;
            }
            return changed;
        };

        // Phase 1: infer smaller tiles from strided source accesses, then
        // propagate those tiles through same-scale elementwise pipelines.
        bool tiles_changed = true;
        int max_tile_passes = std::max(64, static_cast<int>(array_meta.size()));
        for (int iter = 0; iter < max_tile_passes && tiles_changed; ++iter) {
            tiles_changed = false;

            for_each_node_topo(dag, [&](DAGNode* dag_node) {
                for (ASTNode* ast_node : dag_node->ast->roots) {
                    Opcode op = static_cast<Opcode>(ast_node->opcode);

                    if (op == Opcode::SET_REGION) {
                        if (ast_node->operands.size() < 2)
                            continue;

                        int dst_name = ast_node->operands[0]->result_name;
                        auto dst_it = array_meta.find(dst_name);
                        if (dst_it == array_meta.end())
                            continue;
                        if (!region_is_dense(ast_node->region, dst_it->second.ndims))
                            continue;

                        for (int op_idx = 1; op_idx < (int)ast_node->operands.size(); ++op_idx) {
                            tiles_changed =
                                collect_tile_candidates(collect_tile_candidates, dst_name,
                                                        dst_it->second.ndims,
                                                        ast_node->operands[op_idx],
                                                        ast_node->get_operand_region(op_idx)) ||
                                tiles_changed;
                        }
                        continue;
                    }

                    if (is_elementwise(op) || op == Opcode::COPY) {
                        auto out_it = array_meta.find(ast_node->result_name);
                        if (out_it == array_meta.end())
                            continue;

                        for (int op_idx = 0; op_idx < (int)ast_node->operands.size(); ++op_idx) {
                            tiles_changed =
                                collect_tile_candidates(collect_tile_candidates,
                                                        ast_node->result_name,
                                                        out_it->second.ndims,
                                                        ast_node->operands[op_idx],
                                                        ast_node->get_operand_region(op_idx)) ||
                                tiles_changed;
                        }
                    }
                }
            });
        }

        auto extract_region_start = [](Region* region_base, int ndims) {
            std::array<int, 3> region_start = {0, 0, 0};
            if (region_base == nullptr || region_base->is_global)
                return region_start;
            switch (ndims) {
            case 1: {
                auto* r = static_cast<ArrayRegion<1>*>(region_base);
                region_start[0] = r->start[0];
                break;
            }
            case 2: {
                auto* r = static_cast<ArrayRegion<2>*>(region_base);
                region_start[0] = r->start[0];
                region_start[1] = r->start[1];
                break;
            }
            case 3: {
                auto* r = static_cast<ArrayRegion<3>*>(region_base);
                region_start[0] = r->start[0];
                region_start[1] = r->start[1];
                region_start[2] = r->start[2];
                break;
            }
            default:
                break;
            }
            return region_start;
        };

        auto extract_region_step = [](Region* region_base, int ndims) {
            std::array<int, 3> region_step = {1, 1, 1};
            if (region_base == nullptr || region_base->is_global)
                return region_step;
            switch (ndims) {
            case 1: {
                auto* r = static_cast<ArrayRegion<1>*>(region_base);
                region_step[0] = r->step[0];
                break;
            }
            case 2: {
                auto* r = static_cast<ArrayRegion<2>*>(region_base);
                region_step[0] = r->step[0];
                region_step[1] = r->step[1];
                break;
            }
            case 3: {
                auto* r = static_cast<ArrayRegion<3>*>(region_base);
                region_step[0] = r->step[0];
                region_step[1] = r->step[1];
                region_step[2] = r->step[2];
                break;
            }
            default:
                break;
            }
            return region_step;
        };

        // Phase 2: Collect all shift edges for offset optimization.
        //
        // For each array, we gather "desired offset" constraints from two sources:
        //  (a) Producer edges: for elementwise ops producing array C from input A[R],
        //      the desired offset for C is floor((o_A + start(R)) / stride(R)) mod tile_C.
        //  (b) Consumer edges: for SET_REGION T[R] = S, the desired offset for S
        //      is (o_T + start(R)) mod tile_S.
        //
        // The optimal offset per array is the per-dimension weighted median of all
        // desired offsets, which minimizes total misalignment (= communication volume).

        struct DesiredOffset {
            std::array<int, 3> desired;
            int64_t weight;
        };
        std::unordered_map<int, std::vector<DesiredOffset>> all_desired;

        // Helper: collect all leaf operands from an AST subtree and add
        // producer-side desired offsets for a given output array.
        auto collect_producer_edges = [&](auto&& self, ASTNode* node, int out_name,
                                          int nd, int out_tile) -> void {
            if (node == nullptr || node->is_scalar || node->is_broadcast)
                return;

            bool is_leaf = node->operands.empty();
            for (ASTNode* child : node->operands) {
                if (child != nullptr && !child->is_scalar && !child->is_broadcast) {
                    is_leaf = false;
                    break;
                }
            }

            if (is_leaf) {
                int src_name = node->result_name;
                if (src_name == out_name)
                    return;

                auto src_it = array_meta.find(src_name);
                if (src_it == array_meta.end() || src_it->second.ndims != nd)
                    return;

                if (out_tile <= 0)
                    return;

                std::array<int, 3> desired = {0, 0, 0};
                for (int d = 0; d < nd; ++d)
                    desired[d] = positive_mod(src_it->second.offset[d], out_tile);

                int64_t weight = 1;
                for (int d = 0; d < nd; ++d)
                    weight *= static_cast<int64_t>(src_it->second.global_shape[d]);

                all_desired[out_name].push_back({desired, weight});
                return;
            }

            // For non-leaf nodes, recurse into children.
            // If this node has operand_regions, extract the region for each child
            // and compute the desired offset accounting for the access shift/stride.
            for (int i = 0; i < (int)node->operands.size(); ++i) {
                ASTNode* child = node->operands[i];
                if (child == nullptr || child->is_scalar || child->is_broadcast)
                    continue;

                // Check if this child is a leaf with an access region
                Region* child_region = node->get_operand_region(i);
                bool child_is_leaf = true;
                for (ASTNode* gc : child->operands) {
                    if (gc != nullptr && !gc->is_scalar && !gc->is_broadcast) {
                        child_is_leaf = false;
                        break;
                    }
                }

                if (child_is_leaf && child_region != nullptr && !child_region->is_global) {
                    int src_name = child->result_name;
                    if (src_name == out_name) continue;
                    auto src_it = array_meta.find(src_name);
                    if (src_it == array_meta.end() || src_it->second.ndims != nd)
                        continue;
                    if (out_tile <= 0) continue;

                    auto rstart = extract_region_start(child_region, nd);
                    auto rstep = extract_region_step(child_region, nd);

                    std::array<int, 3> desired = {0, 0, 0};
                    for (int d = 0; d < nd; ++d) {
                        int step = (rstep[d] > 1) ? rstep[d] : 1;
                        int raw = src_it->second.offset[d] + rstart[d];
                        if (step > 1)
                            desired[d] = positive_mod(raw / step, out_tile);
                        else
                            desired[d] = positive_mod(raw, out_tile);
                    }

                    int64_t weight = 1;
                    for (int d = 0; d < nd; ++d)
                        weight *= static_cast<int64_t>(src_it->second.global_shape[d]);

                    all_desired[out_name].push_back({desired, weight});
                } else {
                    self(self, child, out_name, nd, out_tile);
                }
            }
        };

        // Helper: collect all leaf operands from a SET_REGION RHS subtree and
        // add consumer-side desired offsets for the source arrays.
        auto collect_consumer_edges = [&](auto&& self, ASTNode* node,
                                          int target_name, int nd,
                                          const ArrayMetadata& target_meta,
                                          const std::array<int, 3>& region_start) -> void {
            if (node == nullptr || node->is_scalar || node->is_broadcast)
                return;

            bool is_leaf = node->operands.empty();
            for (ASTNode* child : node->operands) {
                if (child != nullptr && !child->is_scalar && !child->is_broadcast) {
                    is_leaf = false;
                    break;
                }
            }

            if (is_leaf) {
                int src_name = node->result_name;
                if (src_name == target_name)
                    return;

                auto src_it = array_meta.find(src_name);
                if (src_it == array_meta.end() || src_it->second.ndims != nd)
                    return;

                int tile = src_it->second.tile;
                if (tile <= 0)
                    return;

                // For SET_REGION T[R] = S: desired offset for S = o_T + start(R)
                std::array<int, 3> desired = {0, 0, 0};
                for (int d = 0; d < nd; ++d)
                    desired[d] = positive_mod(target_meta.offset[d] + region_start[d], tile);

                int64_t weight = 1;
                for (int d = 0; d < nd; ++d)
                    weight *= static_cast<int64_t>(src_it->second.global_shape[d]);

                all_desired[src_name].push_back({desired, weight});
                return;
            }

            for (ASTNode* child : node->operands)
                self(self, child, target_name, nd, target_meta, region_start);
        };

        // Traverse all DAG nodes to collect edges.
        for_each_node_topo(dag, [&](DAGNode* dag_node) {
            for (ASTNode* ast_node : dag_node->ast->roots) {
                Opcode op = static_cast<Opcode>(ast_node->opcode);

                if (op == Opcode::SET_REGION) {
                    if (ast_node->operands.size() < 2 || ast_node->region == nullptr)
                        continue;

                    int target_name = ast_node->operands[0]->result_name;
                    auto target_it = array_meta.find(target_name);
                    if (target_it == array_meta.end())
                        continue;

                    int nd = target_it->second.ndims;
                    auto region_start = extract_region_start(ast_node->region, nd);

                    // Consumer edges: desired offsets for the RHS source arrays
                    for (int op_idx = 1; op_idx < (int)ast_node->operands.size(); ++op_idx)
                        collect_consumer_edges(collect_consumer_edges,
                                               ast_node->operands[op_idx],
                                               target_name, nd,
                                               target_it->second, region_start);
                    continue;
                }

                if (is_elementwise(op) || op == Opcode::COPY) {
                    int out_name = ast_node->result_name;
                    auto out_it = array_meta.find(out_name);
                    if (out_it == array_meta.end())
                        continue;
                    int nd = out_it->second.ndims;
                    int out_tile = out_it->second.tile;

                    // Producer edges: desired offsets for the output from its inputs
                    for (int op_idx = 0; op_idx < (int)ast_node->operands.size(); ++op_idx) {
                        ASTNode* operand = ast_node->operands[op_idx];
                        if (operand == nullptr || operand->is_scalar || operand->is_broadcast)
                            continue;

                        Region* op_region = ast_node->get_operand_region(op_idx);
                        bool operand_is_leaf = true;
                        for (ASTNode* gc : operand->operands) {
                            if (gc != nullptr && !gc->is_scalar && !gc->is_broadcast) {
                                operand_is_leaf = false;
                                break;
                            }
                        }

                        if (operand_is_leaf) {
                            int src_name = operand->result_name;
                            if (src_name == out_name) continue;
                            auto src_it = array_meta.find(src_name);
                            if (src_it == array_meta.end() || src_it->second.ndims != nd)
                                continue;
                            if (out_tile <= 0) continue;

                            std::array<int, 3> desired = {0, 0, 0};
                            if (op_region != nullptr && !op_region->is_global) {
                                auto rstart = extract_region_start(op_region, nd);
                                auto rstep = extract_region_step(op_region, nd);
                                for (int d = 0; d < nd; ++d) {
                                    int step = (rstep[d] > 1) ? rstep[d] : 1;
                                    int raw = src_it->second.offset[d] + rstart[d];
                                    if (step > 1)
                                        desired[d] = positive_mod(raw / step, out_tile);
                                    else
                                        desired[d] = positive_mod(raw, out_tile);
                                }
                            } else {
                                for (int d = 0; d < nd; ++d)
                                    desired[d] = positive_mod(src_it->second.offset[d], out_tile);
                            }

                            int64_t weight = 1;
                            for (int d = 0; d < nd; ++d)
                                weight *= static_cast<int64_t>(src_it->second.global_shape[d]);

                            all_desired[out_name].push_back({desired, weight});
                        } else {
                            collect_producer_edges(collect_producer_edges, operand,
                                                   out_name, nd, out_tile);
                        }
                    }
                    continue;
                }

                if (op == Opcode::MATMUL || op == Opcode::MATMATMUL) {
                    for (ASTNode* operand : ast_node->operands)
                        if (operand != nullptr && !operand->is_scalar)
                            matmul_arrays.insert(operand->result_name);
                    matmul_arrays.insert(ast_node->result_name);
                }
            }
        });

        // Phase 3: Compute optimal offsets via per-dimension weighted median.
        //
        // For each array with desired offset constraints, find the offset value
        // that minimizes the total weighted circular distance.
        // Since tile <= 64, we enumerate all possible offsets per dimension.
        auto circ_dist = [](int a, int b, int mod) -> int {
            int d = ((a - b) % mod + mod) % mod;
            return std::min(d, mod - d);
        };

        for (auto& [name, constraints] : all_desired) {
            if (constraints.empty())
                continue;

            auto meta_it = array_meta.find(name);
            if (meta_it == array_meta.end())
                continue;

            // Skip matmul arrays (will be reset to zero)
            if (matmul_arrays.count(name))
                continue;

            // Skip arrays whose decomposition was finalized in a prior epoch
            if (meta_it->second.decomp_final)
                continue;

            int nd = meta_it->second.ndims;
            int tile = meta_it->second.tile;
            if (tile <= 0)
                continue;

            std::array<int, 3> best_offset = {0, 0, 0};
            for (int d = 0; d < nd; ++d) {
                int64_t best_cost = std::numeric_limits<int64_t>::max();
                int best_o = 0;

                for (int o = 0; o < tile; ++o) {
                    int64_t cost = 0;
                    for (auto& c : constraints)
                        cost += c.weight * circ_dist(o, c.desired[d], tile);

                    if (cost < best_cost) {
                        best_cost = cost;
                        best_o = o;
                    }
                }
                best_offset[d] = best_o;
            }
            meta_it->second.offset = best_offset;
        }

        for (int name : matmul_arrays) {
            auto meta_it = array_meta.find(name);
            if (meta_it != array_meta.end() && !meta_it->second.decomp_final)
                meta_it->second.offset = zero_offset;
        }
    }

    // Mark all arrays as finalized so subsequent epochs do not reset their
    // offsets.  This is critical for correctness: chares from earlier epochs
    // may still be executing and looking up decompositions from array_meta.
    for (auto& [name, meta] : array_meta)
        meta.decomp_final = true;

    DBG_PRINT("[PE %d] compute_decompositions: assigned tiles and offsets for %d arrays\n",
              CkMyPe(), (int)array_meta.size());
    for (auto& [name, meta] : array_meta) {
        DBG_PRINT("  array %d: ndims=%d shape=(%d,%d,%d) tile=%d offset=(%d,%d,%d)\n",
                  name, meta.ndims,
                  meta.global_shape[0], meta.global_shape[1], meta.global_shape[2],
                  meta.tile, meta.offset[0], meta.offset[1], meta.offset[2]);
    }

}

void* ArrayDAGGroup::compile_node(DAGNode* node) {
    MLIRJitCompiler node_jit;
    node_jit.buildFromAST(node->ast);
    node_jit.optimizeAndFuse();
    void* moduleHandle = nullptr;
    void* funcPtr = nullptr;

#if defined(USE_NVIDIA)
    std::string ptx = node_jit.generateNVIDIA();
    if (ptx.empty())
        return nullptr;
    if (!node_jit.loadNVIDIA(ptx, "fused_kernel", &moduleHandle, &funcPtr))
        return nullptr;
#elif defined(USE_AMD)
    std::string gcn = node_jit.generateAMD();
    if (gcn.empty())
        return nullptr;
    if (!node_jit.loadAMD(gcn, "fused_kernel", &moduleHandle, &funcPtr))
        return nullptr;
#elif defined(USE_INTEL)
    std::string spirv = node_jit.generateIntel();
    if (spirv.empty())
        return nullptr;
    if (!node_jit.loadIntel(spirv, "fused_kernel", &moduleHandle, &funcPtr))
        return nullptr;
#else
    auto engine = node_jit.generateCPU();
    if (!engine)
        return nullptr;
    if (!node_jit.loadCPU(engine, "fused_kernel", &moduleHandle, &funcPtr))
        return nullptr;
#endif

    module_cache[node->identifier] = moduleHandle;
    return funcPtr;
}

void ArrayDAGGroup::compile(DAG* dag) {
    int newly_compiled = 0;
    for (auto& [id, node] : dag->nodes) {
        auto it = compile_cache.find(node->identifier);
        if (it == compile_cache.end() && node->fusible) {
            num_compile++;
            newly_compiled++;
            void* compiled_fn = compile_node(node);
            compile_cache[node->identifier] = compiled_fn;
        }
    }
    DBG_PRINT("[PE %d] compile: %d new kernels, %d cached kernels total\n",
              CkMyPe(), newly_compiled, (int)compile_cache.size());
}

// Explicit template instantiations for partition expansion helper
template void expand_partition_nd<1>(CProxy_ArrayDAGGroup, CProxy_Partition1D&, int*, Region*, int);
template void expand_partition_nd<2>(CProxy_ArrayDAGGroup, CProxy_Partition2D&, int*, Region*, int);
template void expand_partition_nd<3>(CProxy_ArrayDAGGroup, CProxy_Partition3D&, int*, Region*, int);
