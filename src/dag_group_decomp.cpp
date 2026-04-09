#include "backend_internal.hpp"
#include "dag_group_internal.hpp"

#include <algorithm>
#include <functional>
#include <limits>
#include <queue>
#include <stack>
#include <unordered_set>
#include <vector>

void ArrayDAGGroup::compute_decompositions(DAG* dag) {
    auto zero_offset = std::array<int, 3>{0, 0, 0};

    auto compute_tile_count = [](const ArrayMetadata& m) -> int {
        int count = 1;
        for (int d = 0; d < m.ndims; d++)
            count *= (m.global_shape[d] + m.tile - 1) / m.tile;
        return count;
    };

    auto clamp_tile_for_max_count = [&](ArrayMetadata& m) {
        int max_tiles = odf * CkNumPes();
        while (compute_tile_count(m) > max_tiles)
            m.tile *= 2;
    };

    for (auto& [name, meta] : array_meta) {
        if (meta.tile == 0)
            meta.tile = ct_min_tile(meta.ndims);

        if (!meta.decomp_final) {
            // Ensure tile is a power of 2 and at least ct_min_tile
            meta.tile = std::max(next_pow2(meta.tile), ct_min_tile(meta.ndims));
            // Ensure tile count does not exceed ODF * CkNumPes
            clamp_tile_for_max_count(meta);

            meta.offset = zero_offset;
        }
    }

    if (dag != nullptr) {
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

        auto tile_fits_grid = [](const ArrayMetadata& meta, int tile) { return tile > 0; };

        auto try_assign_tile = [&](int array_name, int candidate_tile) {
            auto array_it = array_meta.find(array_name);
            if (array_it == array_meta.end())
                return false;

            auto& meta = array_it->second;
            if (meta.decomp_final)
                return false;
            if (candidate_tile <= 0)
                return false;

            // Clamp to power-of-2 and minimum tile size
            candidate_tile = std::max(next_pow2(candidate_tile), ct_min_tile(meta.ndims));

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

        // collect_tile_candidates needs a temp-name -> defining-root map so it
        // can follow through fused temps (flat leaf references) to find the
        // ultimate source arrays and their strided access regions.
        auto collect_tile_candidates =
            [&](auto&& self, int array_name, int ndims, ASTNode* node, Region* carried_region,
                const std::unordered_map<int, ASTNode*>& temp_defs) -> bool {
            if (node == nullptr || node->is_scalar || node->is_broadcast)
                return false;

            auto meta_it = array_meta.find(node->result_name);
            if (meta_it != array_meta.end()) {
                if (meta_it->second.ndims == ndims)
                    return propose_tile_from_access(array_name, ndims, node->result_name,
                                                    carried_region);
                return false;
            }

            // If this is a leaf referencing a temp, follow through its
            // defining root's operands to find the real source arrays.
            if (node->operands.empty()) {
                auto def_it = temp_defs.find(node->result_name);
                if (def_it != temp_defs.end()) {
                    ASTNode* def_root = def_it->second;
                    bool changed = false;
                    for (int i = 0; i < (int)def_root->operands.size(); ++i) {
                        changed = self(self, array_name, ndims, def_root->operands[i],
                                       def_root->get_operand_region(i), temp_defs) ||
                                  changed;
                    }
                    return changed;
                }
                return false;
            }

            bool changed = false;
            for (int child_idx = 0; child_idx < (int)node->operands.size(); ++child_idx) {
                changed = self(self, array_name, ndims, node->operands[child_idx],
                               node->get_operand_region(child_idx), temp_defs) ||
                          changed;
            }
            return changed;
        };

        // Phase 1: infer smaller tiles from strided source accesses, then
        // propagate those tiles through same-scale elementwise pipelines.
        bool tiles_changed = true;
        int max_tile_passes = std::max(64, static_cast<int>(array_meta.size()));
        for (int iter = 0; iter < max_tile_passes && tiles_changed; ++iter) {
            tiles_changed = false;

            dag_group_for_each_node_topo(dag, [&](DAGNode* dag_node) {
                // Build a map from temp names to their defining AST roots
                // within this fused DAG node, so collect_tile_candidates can
                // follow through flat temp leaf references.
                std::unordered_map<int, ASTNode*> temp_defs;
                for (ASTNode* root : dag_node->ast->roots) {
                    if (root->is_temp)
                        temp_defs[root->result_name] = root;
                }

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
                                                        ast_node->get_operand_region(op_idx),
                                                        temp_defs) ||
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
                                                        ast_node->get_operand_region(op_idx),
                                                        temp_defs) ||
                                tiles_changed;
                        }
                    }
                }
            });
        }

        // Re-enforce max tile count after Phase 1 stride-based reductions.
        for (auto& [name, meta] : array_meta) {
            if (!meta.decomp_final)
                clamp_tile_for_max_count(meta);
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

        // ---------------------------------------------------------------
        // Phase 2: Build equivalence classes via Union-Find.
        //
        // Arrays connected by shift-0, stride-1, same-tile edges in
        // elementwise ops must share the same offset for zero
        // communication.  We group them into equivalence classes.
        // ---------------------------------------------------------------

        auto circ_dist = [](int a, int b, int mod) -> int {
            int d = ((a - b) % mod + mod) % mod;
            return std::min(d, mod - d);
        };

        // --- Union-Find ---
        std::unordered_map<int, int> uf_parent, uf_rank;

        auto uf_make = [&](int x) {
            if (uf_parent.find(x) == uf_parent.end()) {
                uf_parent[x] = x;
                uf_rank[x] = 0;
            }
        };

        std::function<int(int)> uf_find = [&](int x) -> int {
            if (uf_parent[x] != x)
                uf_parent[x] = uf_find(uf_parent[x]);
            return uf_parent[x];
        };

        auto uf_unite = [&](int x, int y) {
            int rx = uf_find(x), ry = uf_find(y);
            if (rx == ry)
                return;
            if (uf_rank[rx] < uf_rank[ry])
                std::swap(rx, ry);
            uf_parent[ry] = rx;
            if (uf_rank[rx] == uf_rank[ry])
                uf_rank[rx]++;
        };

        for (auto& [name, meta] : array_meta)
            uf_make(name);

        // Helper: check whether an ASTNode is a leaf (has no non-scalar
        // children that correspond to materialized arrays).
        auto is_ast_leaf = [](ASTNode* node) -> bool {
            for (ASTNode* child : node->operands)
                if (child != nullptr && !child->is_scalar && !child->is_broadcast)
                    return false;
            return true;
        };

        // Helper: for a leaf operand accessed with a given region, check
        // whether the edge is shift-0, stride-1, same-tile and union the
        // source with the output if so.
        auto try_union_edge = [&](int out_name, int nd, int out_tile, ASTNode* leaf,
                                  Region* region) {
            int src_name = leaf->result_name;
            if (src_name == out_name)
                return;
            auto src_it = array_meta.find(src_name);
            if (src_it == array_meta.end() || src_it->second.ndims != nd)
                return;
            if (src_it->second.tile != out_tile)
                return;

            if (region == nullptr || region->is_global) {
                uf_unite(out_name, src_name);
                return;
            }

            auto rstart = extract_region_start(region, nd);
            auto rstep = extract_region_step(region, nd);
            for (int d = 0; d < nd; ++d) {
                if (rstart[d] != 0 || rstep[d] != 1)
                    return;
            }
            uf_unite(out_name, src_name);
        };

        // Recursive leaf visitor that calls try_union_edge on every leaf.
        auto visit_leaves_for_union = [&](auto&& self, ASTNode* node, int out_name, int nd,
                                          int out_tile, Region* carried_region) -> void {
            if (node == nullptr || node->is_scalar || node->is_broadcast)
                return;
            if (is_ast_leaf(node)) {
                try_union_edge(out_name, nd, out_tile, node, carried_region);
                return;
            }
            for (int i = 0; i < (int)node->operands.size(); ++i) {
                ASTNode* child = node->operands[i];
                if (child == nullptr || child->is_scalar || child->is_broadcast)
                    continue;
                Region* child_region = node->get_operand_region(i);
                if (is_ast_leaf(child)) {
                    try_union_edge(out_name, nd, out_tile, child,
                                   child_region ? child_region : carried_region);
                } else {
                    self(self, child, out_name, nd, out_tile, child_region);
                }
            }
        };

        // Identify source (CREATE) arrays and build unions for elementwise ops.
        std::unordered_set<int> source_arrays;

        dag_group_for_each_node_topo(dag, [&](DAGNode* dag_node) {
            for (ASTNode* ast_node : dag_node->ast->roots) {
                Opcode op = static_cast<Opcode>(ast_node->opcode);

                if (op == Opcode::CREATE) {
                    source_arrays.insert(ast_node->result_name);
                    continue;
                }

                if (is_elementwise(op) || op == Opcode::COPY) {
                    int out_name = ast_node->result_name;
                    auto out_it = array_meta.find(out_name);
                    if (out_it == array_meta.end())
                        continue;
                    int nd = out_it->second.ndims;
                    int out_tile = out_it->second.tile;

                    for (int op_idx = 0; op_idx < (int)ast_node->operands.size(); ++op_idx) {
                        ASTNode* operand = ast_node->operands[op_idx];
                        if (operand == nullptr || operand->is_scalar || operand->is_broadcast)
                            continue;
                        Region* op_region = ast_node->get_operand_region(op_idx);
                        if (is_ast_leaf(operand)) {
                            try_union_edge(out_name, nd, out_tile, operand, op_region);
                        } else {
                            visit_leaves_for_union(visit_leaves_for_union, operand, out_name, nd,
                                                   out_tile, op_region);
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

        // Build per-class metadata.
        struct ClassInfo {
            int tile = 0;
            int dp_tile = 0; // capped at ct_min_tile(ndims) to keep DP cost bounded
            int ndims = 0;
            bool has_fixed_offset = false;
            std::array<int, 3> fixed_offset = {0, 0, 0};
        };
        std::unordered_map<int, ClassInfo> class_info;

        for (auto& [name, meta] : array_meta) {
            int rep = uf_find(name);
            auto it = class_info.find(rep);
            bool is_fixed = meta.decomp_final || source_arrays.count(name);
            if (it == class_info.end()) {
                int dp_tile = std::min(meta.tile, CT_MAX_OFFSET);
                class_info[rep] = {meta.tile, dp_tile, meta.ndims, is_fixed, meta.offset};
            } else if (is_fixed) {
                it->second.has_fixed_offset = true;
                it->second.fixed_offset = meta.offset;
            }
        }

        // ---------------------------------------------------------------
        // Phase 3: Build the shift graph between equivalence classes.
        //
        // Edges arise from operations where the access region has a
        // non-zero shift, stride > 1, or different tile.  Each edge
        // records the shift/stride and a weight (element count).
        // ---------------------------------------------------------------

        struct ShiftEdge {
            int from_class;
            int to_class;
            std::array<int, 3> shift;
            std::array<int, 3> stride;
            int64_t weight;
            bool forward; // true = original direction from_class -> to_class
        };
        std::vector<ShiftEdge> shift_edges;

        // Helper: for a leaf operand that was NOT unioned, add a shift edge.
        auto add_shift_edge_if_needed = [&](int out_name, int nd, ASTNode* leaf, Region* region) {
            int src_name = leaf->result_name;
            if (src_name == out_name)
                return;
            auto src_it = array_meta.find(src_name);
            if (src_it == array_meta.end() || src_it->second.ndims != nd)
                return;

            int class_src = uf_find(src_name);
            int class_out = uf_find(out_name);
            if (class_src == class_out)
                return;

            std::array<int, 3> rstart = {0, 0, 0};
            std::array<int, 3> rstep = {1, 1, 1};
            if (region != nullptr && !region->is_global) {
                rstart = extract_region_start(region, nd);
                rstep = extract_region_step(region, nd);
            }

            int64_t weight = 1;
            for (int d = 0; d < nd; ++d)
                weight *= static_cast<int64_t>(src_it->second.global_shape[d]);

            shift_edges.push_back({class_src, class_out, rstart, rstep, weight, true});
        };

        // Recursive leaf visitor for shift edge collection.
        auto visit_leaves_for_edges = [&](auto&& self, ASTNode* node, int out_name, int nd,
                                          Region* carried_region) -> void {
            if (node == nullptr || node->is_scalar || node->is_broadcast)
                return;
            if (is_ast_leaf(node)) {
                add_shift_edge_if_needed(out_name, nd, node, carried_region);
                return;
            }
            for (int i = 0; i < (int)node->operands.size(); ++i) {
                ASTNode* child = node->operands[i];
                if (child == nullptr || child->is_scalar || child->is_broadcast)
                    continue;
                Region* child_region = node->get_operand_region(i);
                if (is_ast_leaf(child)) {
                    add_shift_edge_if_needed(out_name, nd, child,
                                             child_region ? child_region : carried_region);
                } else {
                    self(self, child, out_name, nd, child_region);
                }
            }
        };

        // Collect SET_REGION leaf visitor.
        auto visit_set_region_leaves = [&](auto&& self, ASTNode* node, int target_name, int nd,
                                           const std::array<int, 3>& region_start) -> void {
            if (node == nullptr || node->is_scalar || node->is_broadcast)
                return;
            if (is_ast_leaf(node)) {
                int src_name = node->result_name;
                if (src_name == target_name)
                    return;
                auto src_it = array_meta.find(src_name);
                if (src_it == array_meta.end() || src_it->second.ndims != nd)
                    return;

                int class_src = uf_find(src_name);
                int class_tgt = uf_find(target_name);
                if (class_src == class_tgt)
                    return;

                int64_t weight = 1;
                for (int d = 0; d < nd; ++d)
                    weight *= static_cast<int64_t>(src_it->second.global_shape[d]);

                // SET_REGION T[R] = S: element i of S maps to position i+start
                // in T. For zero communication we need o_S = o_T + start.
                shift_edges.push_back(
                    {class_tgt, class_src, region_start, {1, 1, 1}, weight, true});
                return;
            }
            for (ASTNode* child : node->operands)
                self(self, child, target_name, nd, region_start);
        };

        dag_group_for_each_node_topo(dag, [&](DAGNode* dag_node) {
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

                    for (int op_idx = 1; op_idx < (int)ast_node->operands.size(); ++op_idx)
                        visit_set_region_leaves(visit_set_region_leaves,
                                                ast_node->operands[op_idx], target_name, nd,
                                                region_start);
                    continue;
                }

                if (is_elementwise(op) || op == Opcode::COPY) {
                    int out_name = ast_node->result_name;
                    auto out_it = array_meta.find(out_name);
                    if (out_it == array_meta.end())
                        continue;
                    int nd = out_it->second.ndims;

                    for (int op_idx = 0; op_idx < (int)ast_node->operands.size(); ++op_idx) {
                        ASTNode* operand = ast_node->operands[op_idx];
                        if (operand == nullptr || operand->is_scalar || operand->is_broadcast)
                            continue;
                        Region* op_region = ast_node->get_operand_region(op_idx);
                        if (is_ast_leaf(operand)) {
                            add_shift_edge_if_needed(out_name, nd, operand, op_region);
                        } else {
                            visit_leaves_for_edges(visit_leaves_for_edges, operand, out_name, nd,
                                                   op_region);
                        }
                    }
                }
            }
        });

        // ---------------------------------------------------------------
        // Phase 4: Tree DP on the shift graph.
        //
        // Build an undirected adjacency list, find connected components
        // (detect cycles), root each tree at a fixed-offset class, and
        // run bottom-up / top-down DP to find globally optimal offsets.
        // Dimensions are independent so the DP runs per-dimension.
        // ---------------------------------------------------------------

        struct AdjEdge {
            int neighbor;
            std::array<int, 3> shift;
            std::array<int, 3> stride;
            int64_t weight;
            bool forward; // original edge direction: from_class -> to_class
            int from_class;
            int to_class;
        };
        std::unordered_map<int, std::vector<AdjEdge>> adj;

        for (auto& e : shift_edges) {
            if (e.from_class == e.to_class)
                continue;
            adj[e.from_class].push_back(
                {e.to_class, e.shift, e.stride, e.weight, true, e.from_class, e.to_class});
            adj[e.to_class].push_back(
                {e.from_class, e.shift, e.stride, e.weight, false, e.from_class, e.to_class});
        }

        // Find connected components via BFS, build spanning tree, detect cycles.
        std::unordered_map<int, int> comp_id;
        std::unordered_map<int, int> tree_parent;
        std::unordered_map<int, std::vector<int>> tree_children;
        std::unordered_set<int> cyclic_components;
        int num_components = 0;

        std::unordered_set<int> all_classes;
        for (auto& [name, _] : array_meta)
            all_classes.insert(uf_find(name));

        std::unordered_map<int, int> comp_root;

        for (int cls : all_classes) {
            if (comp_id.count(cls))
                continue;
            int cid = num_components++;
            comp_root[cid] = cls;

            std::queue<int> bfs;
            bfs.push(cls);
            comp_id[cls] = cid;
            tree_parent[cls] = -1;

            while (!bfs.empty()) {
                int v = bfs.front();
                bfs.pop();
                // Deduplicate neighbors so that multi-edges between the
                // same pair of classes (common for stencils) are not
                // misdetected as back edges / cycles.
                std::unordered_set<int> seen_neighbors;
                for (auto& e : adj[v]) {
                    if (seen_neighbors.count(e.neighbor))
                        continue;
                    seen_neighbors.insert(e.neighbor);
                    if (!comp_id.count(e.neighbor)) {
                        comp_id[e.neighbor] = cid;
                        tree_parent[e.neighbor] = v;
                        tree_children[v].push_back(e.neighbor);
                        bfs.push(e.neighbor);
                    } else if (e.neighbor != tree_parent[v]) {
                        cyclic_components.insert(cid);
                    }
                }
                // Prefer fixed-offset class as root.
                auto ci = class_info.find(v);
                if (ci != class_info.end() && ci->second.has_fixed_offset)
                    comp_root[cid] = v;
            }
        }

        // Re-root each tree at the chosen root if it differs from the BFS root.
        tree_parent.clear();
        tree_children.clear();

        for (auto& [cid, root] : comp_root) {
            std::queue<int> bfs;
            bfs.push(root);
            tree_parent[root] = -1;

            std::unordered_set<int> visited;
            visited.insert(root);

            while (!bfs.empty()) {
                int v = bfs.front();
                bfs.pop();
                for (auto& e : adj[v]) {
                    if (!visited.count(e.neighbor) && comp_id[e.neighbor] == cid) {
                        visited.insert(e.neighbor);
                        tree_parent[e.neighbor] = v;
                        tree_children[v].push_back(e.neighbor);
                        bfs.push(e.neighbor);
                    }
                }
            }
        }

        // Bottom-up DP.
        // dp[class][dim][offset] = minimum communication cost for the subtree
        // rooted at class when class is assigned offset in dimension dim.
        constexpr int64_t INF = std::numeric_limits<int64_t>::max() / 2;

        std::unordered_map<int, std::array<std::vector<int64_t>, 3>> dp;

        // Compute post-order traversal for each tree.
        std::vector<int> post_order;
        {
            for (auto& [cid, root] : comp_root) {
                if (cyclic_components.count(cid))
                    continue;

                std::stack<std::pair<int, bool>> stk;
                stk.push({root, false});
                while (!stk.empty()) {
                    auto [v, processed] = stk.top();
                    stk.pop();
                    if (processed) {
                        post_order.push_back(v);
                        continue;
                    }
                    stk.push({v, true});
                    for (int c : tree_children[v])
                        stk.push({c, false});
                }
            }
        }

        for (int v : post_order) {
            auto ci = class_info.find(v);
            if (ci == class_info.end())
                continue;
            int nd = ci->second.ndims;
            int dp_tile = ci->second.dp_tile;
            if (dp_tile <= 0)
                dp_tile = 1;

            auto& dp_v = dp[v];
            for (int d = 0; d < 3; ++d) {
                dp_v[d].assign(dp_tile, 0);
                if (d < nd && ci->second.has_fixed_offset) {
                    int fixed = ci->second.fixed_offset[d] % dp_tile;
                    for (int o = 0; o < dp_tile; ++o)
                        dp_v[d][o] = (o == fixed) ? 0 : INF;
                }
            }

            for (int c : tree_children[v]) {
                auto cc = class_info.find(c);
                if (cc == class_info.end())
                    continue;
                int dp_tile_c = cc->second.dp_tile;
                if (dp_tile_c <= 0)
                    dp_tile_c = 1;

                // Collect all adjacency edges between v and c.
                std::vector<const AdjEdge*> edges_vc;
                for (auto& e : adj[v])
                    if (e.neighbor == c)
                        edges_vc.push_back(&e);

                for (int d = 0; d < nd; ++d) {
                    // h[ov] = min over oc of [transition(ov,oc) + dp[c][d][oc]]
                    std::vector<int64_t> h(dp_tile, INF);
                    for (int ov = 0; ov < dp_tile; ++ov) {
                        for (int oc = 0; oc < dp_tile_c; ++oc) {
                            int64_t cost = dp[c][d][oc];
                            if (cost >= INF)
                                continue;
                            for (auto* ep : edges_vc) {
                                // Compute desired offset based on original edge direction.
                                int o_from, dp_tile_to, o_to;
                                if (ep->forward) {
                                    o_from = ov;
                                    dp_tile_to = dp_tile_c;
                                    o_to = oc;
                                } else {
                                    o_from = oc;
                                    dp_tile_to = dp_tile;
                                    o_to = ov;
                                }
                                int step = ep->stride[d] > 1 ? ep->stride[d] : 1;
                                int desired = positive_mod((o_from + ep->shift[d]) / step,
                                                           dp_tile_to);
                                cost += ep->weight * circ_dist(desired, o_to, dp_tile_to);
                                if (cost >= INF)
                                    break;
                            }
                            if (cost < h[ov])
                                h[ov] = cost;
                        }
                    }
                    for (int ov = 0; ov < dp_tile; ++ov) {
                        if (dp_v[d][ov] < INF && h[ov] < INF)
                            dp_v[d][ov] += h[ov];
                        else
                            dp_v[d][ov] = INF;
                    }
                }
            }
        }

        // Top-down: recover optimal offsets.
        std::unordered_map<int, std::array<int, 3>> class_offset;

        // Assign roots.
        for (auto& [cid, root] : comp_root) {
            if (cyclic_components.count(cid))
                continue;
            auto ci = class_info.find(root);
            if (ci == class_info.end())
                continue;
            int nd = ci->second.ndims;
            int dp_tile = ci->second.dp_tile;
            if (dp_tile <= 0)
                dp_tile = 1;

            std::array<int, 3> best = {0, 0, 0};
            for (int d = 0; d < nd; ++d) {
                int64_t best_cost = INF;
                for (int o = 0; o < dp_tile; ++o) {
                    if (dp[root][d][o] < best_cost) {
                        best_cost = dp[root][d][o];
                        best[d] = o;
                    }
                }
            }
            class_offset[root] = best;
        }

        // BFS top-down to recover children's offsets.
        {
            std::queue<int> work;
            for (auto& [cid, root] : comp_root) {
                if (!cyclic_components.count(cid))
                    work.push(root);
            }

            while (!work.empty()) {
                int v = work.front();
                work.pop();
                auto ci_v = class_info.find(v);
                if (ci_v == class_info.end())
                    continue;
                int nd = ci_v->second.ndims;
                int dp_tile_v = ci_v->second.dp_tile;
                if (dp_tile_v <= 0)
                    dp_tile_v = 1;

                for (int c : tree_children[v]) {
                    auto ci_c = class_info.find(c);
                    if (ci_c == class_info.end())
                        continue;
                    int dp_tile_c = ci_c->second.dp_tile;
                    if (dp_tile_c <= 0)
                        dp_tile_c = 1;

                    std::vector<const AdjEdge*> edges_vc;
                    for (auto& e : adj[v])
                        if (e.neighbor == c)
                            edges_vc.push_back(&e);

                    std::array<int, 3> best_c = {0, 0, 0};
                    for (int d = 0; d < nd; ++d) {
                        int ov = class_offset[v][d];
                        int64_t best_cost = INF;
                        for (int oc = 0; oc < dp_tile_c; ++oc) {
                            int64_t cost = dp[c][d][oc];
                            if (cost >= INF)
                                continue;
                            for (auto* ep : edges_vc) {
                                int o_from, dp_tile_to, o_to;
                                if (ep->forward) {
                                    o_from = ov;
                                    dp_tile_to = dp_tile_c;
                                    o_to = oc;
                                } else {
                                    o_from = oc;
                                    dp_tile_to = dp_tile_v;
                                    o_to = ov;
                                }
                                int step = ep->stride[d] > 1 ? ep->stride[d] : 1;
                                int desired = positive_mod((o_from + ep->shift[d]) / step,
                                                           dp_tile_to);
                                cost += ep->weight * circ_dist(desired, o_to, dp_tile_to);
                                if (cost >= INF)
                                    break;
                            }
                            if (cost < best_cost) {
                                best_cost = cost;
                                best_c[d] = oc;
                            }
                        }
                    }
                    class_offset[c] = best_c;
                    work.push(c);
                }
            }
        }

        // ---------------------------------------------------------------
        // Phase 5: Assign offsets to all arrays from their class
        // representatives. For cyclic components, fall back to the
        // per-array weighted-median heuristic.
        // ---------------------------------------------------------------

        // Assign offsets from the tree DP.
        for (auto& [name, meta] : array_meta) {
            if (meta.decomp_final)
                continue;
            if (matmul_arrays.count(name))
                continue;
            int rep = uf_find(name);
            auto it = class_offset.find(rep);
            if (it != class_offset.end())
                meta.offset = it->second;
        }

        // Fallback: for arrays in cyclic components, collect desired offsets
        // per array and pick the per-dimension weighted median.
        if (!cyclic_components.empty()) {
            DBG_PRINT("Warning: Fallback to per-array weighted median for cyclic components\n");

            struct DesiredOffset {
                std::array<int, 3> desired;
                int64_t weight;
            };
            std::unordered_map<int, std::vector<DesiredOffset>> all_desired;

            auto collect_fallback_producer =
                [&](auto&& self, ASTNode* node, int out_name, int nd, int out_tile) -> void {
                if (node == nullptr || node->is_scalar || node->is_broadcast)
                    return;
                if (is_ast_leaf(node)) {
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
                for (int i = 0; i < (int)node->operands.size(); ++i) {
                    ASTNode* child = node->operands[i];
                    if (child == nullptr || child->is_scalar || child->is_broadcast)
                        continue;
                    Region* child_region = node->get_operand_region(i);
                    if (is_ast_leaf(child) && child_region != nullptr && !child_region->is_global) {
                        int src_name = child->result_name;
                        if (src_name == out_name)
                            continue;
                        auto src_it = array_meta.find(src_name);
                        if (src_it == array_meta.end() || src_it->second.ndims != nd)
                            continue;
                        if (out_tile <= 0)
                            continue;
                        auto rstart = extract_region_start(child_region, nd);
                        auto rstep = extract_region_step(child_region, nd);
                        std::array<int, 3> desired = {0, 0, 0};
                        for (int d = 0; d < nd; ++d) {
                            int step = (rstep[d] > 1) ? rstep[d] : 1;
                            int raw = src_it->second.offset[d] + rstart[d];
                            desired[d] =
                                (step > 1) ? positive_mod(raw / step, out_tile)
                                           : positive_mod(raw, out_tile);
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

            auto collect_fallback_consumer =
                [&](auto&& self, ASTNode* node, int target_name, int nd,
                    const ArrayMetadata& target_meta, const std::array<int, 3>& region_start)
                -> void {
                if (node == nullptr || node->is_scalar || node->is_broadcast)
                    return;
                if (is_ast_leaf(node)) {
                    int src_name = node->result_name;
                    if (src_name == target_name)
                        return;
                    auto src_it = array_meta.find(src_name);
                    if (src_it == array_meta.end() || src_it->second.ndims != nd)
                        return;
                    int tile = src_it->second.tile;
                    if (tile <= 0)
                        return;
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

            // Only collect for arrays in cyclic components.
            auto is_cyclic_array = [&](int name) {
                int rep = uf_find(name);
                auto cit = comp_id.find(rep);
                return cit != comp_id.end() && cyclic_components.count(cit->second);
            };

            dag_group_for_each_node_topo(dag, [&](DAGNode* dag_node) {
                for (ASTNode* ast_node : dag_node->ast->roots) {
                    Opcode op = static_cast<Opcode>(ast_node->opcode);

                    if (op == Opcode::SET_REGION) {
                        if (ast_node->operands.size() < 2 || !ast_node->region)
                            continue;
                        int target_name = ast_node->operands[0]->result_name;
                        auto target_it = array_meta.find(target_name);
                        if (target_it == array_meta.end())
                            continue;
                        int nd = target_it->second.ndims;
                        auto region_start = extract_region_start(ast_node->region, nd);
                        for (int i = 1; i < (int)ast_node->operands.size(); ++i)
                            collect_fallback_consumer(collect_fallback_consumer,
                                                      ast_node->operands[i], target_name, nd,
                                                      target_it->second, region_start);
                        continue;
                    }

                    if (is_elementwise(op) || op == Opcode::COPY) {
                        int out_name = ast_node->result_name;
                        if (!is_cyclic_array(out_name))
                            continue;
                        auto out_it = array_meta.find(out_name);
                        if (out_it == array_meta.end())
                            continue;
                        int nd = out_it->second.ndims;
                        int out_tile = out_it->second.tile;
                        for (int i = 0; i < (int)ast_node->operands.size(); ++i) {
                            ASTNode* operand = ast_node->operands[i];
                            if (!operand || operand->is_scalar || operand->is_broadcast)
                                continue;
                            collect_fallback_producer(collect_fallback_producer, operand, out_name,
                                                      nd, out_tile);
                        }
                    }
                }
            });

            for (auto& [name, constraints] : all_desired) {
                if (constraints.empty())
                    continue;
                if (!is_cyclic_array(name))
                    continue;
                auto meta_it = array_meta.find(name);
                if (meta_it == array_meta.end() || meta_it->second.decomp_final)
                    continue;
                if (matmul_arrays.count(name))
                    continue;

                int nd = meta_it->second.ndims;
                int tile = meta_it->second.tile;
                if (tile <= 0)
                    continue;
                int dp_tile = std::min(tile, CT_MAX_OFFSET);

                std::array<int, 3> best_offset = {0, 0, 0};
                for (int d = 0; d < nd; ++d) {
                    int64_t best_cost = std::numeric_limits<int64_t>::max();
                    int best_o = 0;
                    for (int o = 0; o < dp_tile; ++o) {
                        int64_t cost = 0;
                        for (auto& c : constraints)
                            cost += c.weight * circ_dist(o, c.desired[d], dp_tile);
                        if (cost < best_cost) {
                            best_cost = cost;
                            best_o = o;
                        }
                    }
                    best_offset[d] = best_o;
                }
                meta_it->second.offset = best_offset;
            }
        }

        for (int name : matmul_arrays) {
            auto meta_it = array_meta.find(name);
            if (meta_it != array_meta.end() && !meta_it->second.decomp_final)
                meta_it->second.offset = zero_offset;
        }
    }

    // Mark all arrays as finalized so subsequent epochs do not reset their
    // offsets. This is critical for correctness: chares from earlier epochs
    // may still be executing and looking up decompositions from array_meta.
    for (auto& [name, meta] : array_meta)
        meta.decomp_final = true;

    DBG_PRINT("[PE %d] compute_decompositions: assigned tiles and offsets for %d arrays\n",
              CkMyPe(), (int)array_meta.size());
    for (auto& [name, meta] : array_meta) {
        DBG_PRINT("  array %d: ndims=%d shape=(%d,%d,%d) tile=%d offset=(%d,%d,%d)\n", name,
                  meta.ndims, meta.global_shape[0], meta.global_shape[1],
                  meta.global_shape[2], meta.tile, meta.offset[0], meta.offset[1],
                  meta.offset[2]);
    }
}
