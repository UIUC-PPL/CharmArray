/**
 * @file test_jit.cpp
 * @brief Standalone test program for the MLIRJitCompiler.
 *
 * Builds AST trees by hand and exercises buildFromAST + optimizeAndFuse.
 * Prints MLIR IR before and after fusion so you can visually verify
 * the generated code.
 *
 * Build (adjust paths to your MLIR/LLVM install):
 *
 *   clang++ -std=c++17 -O2 \
 *       -I${CHARMTYLES_HOME}/include \
 *       -I${CHARMTYLES_HOME}/example/charmnumeric/src \
 *       $(llvm-config --cxxflags) \
 *       test_jit.cpp \
 *       $(llvm-config --ldflags --libs all) \
 *       -lMLIR -lMLIRLinalgDialect -lMLIRAffineDialect -lMLIRGPUDialect \
 *       -lMLIRArithDialect -lMLIRMemRefDialect -lMLIRFuncDialect \
 *       -lMLIRPass -lMLIRTransforms \
 *       -o test_jit
 */

#include <cassert>
#include <cstdio>
#include <iostream>
#include <vector>

#include "jit.hpp"

// ------------------------------------------------------------------ //
//  Helpers to build AST nodes
// ------------------------------------------------------------------ //

/// Create a leaf (NOOP) node representing an existing array.
static ASTNode* makeLeaf(int name, int ndims = 1) {
    return new ASTNode(name, static_cast<int>(Opcode::NOOP),
                       /*is_temp=*/false, /*is_scalar=*/false,
                       /*scalar=*/0.0f, /*ndims=*/ndims);
}

/// Create a scalar leaf node.
static ASTNode* makeScalar(float value) {
    static int scalar_counter = -1;
    return new ASTNode(scalar_counter--, static_cast<int>(Opcode::NOOP),
                       /*is_temp=*/false, /*is_scalar=*/true, value);
}

/// Create a binary op node (ADD, SUB, MUL, DIV).
static ASTNode* makeBinOp(int result_name, Opcode op, ASTNode* lhs, ASTNode* rhs,
                          bool is_temp = false) {
    auto* node = new ASTNode(result_name, static_cast<int>(op), is_temp);
    node->add_operand(lhs);
    node->add_operand(rhs);
    return node;
}

/// Create a GET_REGION node with an n-dimensional ArrayRegion.
static ASTNode* makeGetRegion(int result_name, ASTNode* src, std::vector<int> start,
                              std::vector<int> stop, std::vector<int> step) {
    auto* node = new ASTNode(result_name, static_cast<int>(Opcode::GET_REGION));
    node->add_operand(src);
    node->region = new ArrayRegion(start, stop, step);
    return node;
}

// ------------------------------------------------------------------ //
//  Test cases
// ------------------------------------------------------------------ //

/// Test 1: Simple  C = A + B
void test_simple_add() {
    std::cout << "=== Test 1: C = A + B ===" << std::endl;

    ASTNode* a = makeLeaf(0);                     // array A (name=0)
    ASTNode* b = makeLeaf(1);                     // array B (name=1)
    ASTNode* c = makeBinOp(2, Opcode::ADD, a, b); // C = A + B

    // Build a flat AST: leaves are implicit, roots = [c]
    AST ast({c});

    MLIRJitCompiler jit;
    jit.buildFromAST(&ast);

    std::cout << "--- Before fusion ---" << std::endl;
    jit.dumpMLIR();

    bool ok = jit.optimizeAndFuse();
    std::cout << "--- After fusion (ok=" << ok << ") ---" << std::endl;
    jit.dumpMLIR();
    std::cout << std::endl;
}

/// Test 2: Chained ops  D = (A + B) * C  — should fuse into one loop
void test_fused_add_mul() {
    std::cout << "=== Test 2: D = (A + B) * C ===" << std::endl;

    ASTNode* a = makeLeaf(0);
    ASTNode* b = makeLeaf(1);
    ASTNode* c = makeLeaf(2);

    // tmp = A + B  (temporary, should be fused away)
    ASTNode* tmp = makeBinOp(100, Opcode::ADD, a, b, /*is_temp=*/true);
    // D = tmp * C
    ASTNode* d = makeBinOp(3, Opcode::MUL, tmp, c);

    AST ast({tmp, d}); // flat order: tmp first, then d

    MLIRJitCompiler jit;
    jit.buildFromAST(&ast);

    std::cout << "--- Before fusion ---" << std::endl;
    jit.dumpMLIR();

    bool ok = jit.optimizeAndFuse();
    std::cout << "--- After fusion (ok=" << ok << ") ---" << std::endl;
    jit.dumpMLIR();
    std::cout << std::endl;
}

/// Test 3: Scalar broadcast  C = 2.0 * A
void test_scalar_mul() {
    std::cout << "=== Test 3: C = 2.0 * A ===" << std::endl;

    ASTNode* a = makeLeaf(0);
    ASTNode* two = makeScalar(2.0f);

    ASTNode* c = makeBinOp(1, Opcode::MUL, two, a);

    AST ast({c});

    MLIRJitCompiler jit;
    jit.buildFromAST(&ast);

    std::cout << "--- Before fusion ---" << std::endl;
    jit.dumpMLIR();

    bool ok = jit.optimizeAndFuse();
    std::cout << "--- After fusion (ok=" << ok << ") ---" << std::endl;
    jit.dumpMLIR();
    std::cout << std::endl;
}

/// Test 4: Subview  B = A[10:50:1] + A[10:50:1]
void test_subview() {
    std::cout << "=== Test 4: subview A[10:50] ===" << std::endl;

    ASTNode* a = makeLeaf(0);

    // slice = A[10:50:1]
    ASTNode* slice = makeGetRegion(100, a, {10}, {50}, {1});

    // B = slice + slice
    ASTNode* b = makeBinOp(1, Opcode::ADD, slice, slice);

    AST ast({slice, b});

    MLIRJitCompiler jit;
    jit.buildFromAST(&ast);

    std::cout << "--- Before fusion ---" << std::endl;
    jit.dumpMLIR();

    bool ok = jit.optimizeAndFuse();
    std::cout << "--- After fusion (ok=" << ok << ") ---" << std::endl;
    jit.dumpMLIR();
    std::cout << std::endl;
}

/// Test 5: All four binary ops chained  E = (A + B) - (C * D)
void test_all_binops() {
    std::cout << "=== Test 5: E = (A + B) - (C * D) ===" << std::endl;

    ASTNode* a = makeLeaf(0);
    ASTNode* b = makeLeaf(1);
    ASTNode* c = makeLeaf(2);
    ASTNode* d = makeLeaf(3);

    ASTNode* ab = makeBinOp(100, Opcode::ADD, a, b, /*is_temp=*/true);
    ASTNode* cd = makeBinOp(101, Opcode::MUL, c, d, /*is_temp=*/true);
    ASTNode* e = makeBinOp(4, Opcode::SUB, ab, cd);

    AST ast({ab, cd, e});

    MLIRJitCompiler jit;
    jit.buildFromAST(&ast);

    std::cout << "--- Before fusion ---" << std::endl;
    jit.dumpMLIR();

    bool ok = jit.optimizeAndFuse();
    std::cout << "--- After fusion (ok=" << ok << ") ---" << std::endl;
    jit.dumpMLIR();
    std::cout << std::endl;
}

/// Test 6: 2D add  C = A + B  where A, B are 2D arrays
void test_2d_add() {
    std::cout << "=== Test 6: 2D C = A + B ===" << std::endl;

    ASTNode* a = makeLeaf(0, /*ndims=*/2); // memref<?x?xf32>
    ASTNode* b = makeLeaf(1, /*ndims=*/2);
    ASTNode* c = makeBinOp(2, Opcode::ADD, a, b);

    AST ast({c});

    MLIRJitCompiler jit;
    jit.buildFromAST(&ast);

    std::cout << "--- Before fusion ---" << std::endl;
    jit.dumpMLIR();

    bool ok = jit.optimizeAndFuse();
    std::cout << "--- After fusion (ok=" << ok << ") ---" << std::endl;
    jit.dumpMLIR();
    std::cout << std::endl;
}

/// Test 7: 2D subview  B = A[2:8, 0:10]
void test_2d_subview() {
    std::cout << "=== Test 7: 2D subview A[2:8, 0:10] ===" << std::endl;

    ASTNode* a = makeLeaf(0, /*ndims=*/2);

    ASTNode* slice = makeGetRegion(100, a, {2, 0}, {8, 10}, {1, 1});

    // B = slice + slice
    ASTNode* b = makeBinOp(1, Opcode::ADD, slice, slice);

    AST ast({slice, b});

    MLIRJitCompiler jit;
    jit.buildFromAST(&ast);

    std::cout << "--- Before fusion ---" << std::endl;
    jit.dumpMLIR();

    bool ok = jit.optimizeAndFuse();
    std::cout << "--- After fusion (ok=" << ok << ") ---" << std::endl;
    jit.dumpMLIR();
    std::cout << std::endl;
}

/// Test 8: aligned packed fragments keep dense strides for stepped regions.
void test_align_fragments_step_strides() {
    std::cout << "=== Test 8: align stepped packed fragments ===" << std::endl;

    ArrayRegion<1> parent({0}, {64}, {2});
    ArrayRegion<1> left({0}, {32}, {2});
    ArrayRegion<1> right({32}, {64}, {2});

    std::vector<float> packed_full(32);
    std::vector<float> packed_left(16);
    std::vector<float> packed_right(16);
    for (int i = 0; i < 32; ++i)
        packed_full[i] = static_cast<float>(i);
    for (int i = 0; i < 16; ++i) {
        packed_left[i] = static_cast<float>(100 + i);
        packed_right[i] = static_cast<float>(200 + i);
    }

    std::vector<std::vector<FragmentData<1, float>>> inputs(2);
    inputs[0].push_back({parent, packed_full.data()});
    inputs[1].push_back({left, packed_left.data()});
    inputs[1].push_back({right, packed_right.data()});

    auto aligned = align_fragments<1, float>(inputs, {parent, parent});
    assert(aligned.size() == 2);
    assert(aligned[0].size() == 2);
    assert(aligned[1].size() == 2);

    assert(aligned[0][0].memref.sizes[0] == 16);
    assert(aligned[0][1].memref.sizes[0] == 16);
    assert(aligned[0][0].memref.strides[0] == 1);
    assert(aligned[0][1].memref.strides[0] == 1);
    assert(aligned[0][0].memref.aligned == packed_full.data());
    assert(aligned[0][1].memref.aligned == packed_full.data() + 16);
    assert(aligned[1][0].memref.aligned == packed_left.data());
    assert(aligned[1][1].memref.aligned == packed_right.data());

    std::vector<float> dense_base(64);
    for (int i = 0; i < 64; ++i)
        dense_base[i] = static_cast<float>(300 + i);
    FragmentData<1, float> local_dense{parent, dense_base.data()};
    local_dense.src_strides[0] = 1;

    std::vector<std::vector<FragmentData<1, float>>> local_inputs(2);
    local_inputs[0].push_back(local_dense);
    local_inputs[1].push_back({left, packed_left.data()});
    local_inputs[1].push_back({right, packed_right.data()});

    auto local_aligned = align_fragments<1, float>(local_inputs, {parent, parent});
    assert(local_aligned[0].size() == 2);
    assert(local_aligned[0][0].memref.strides[0] == 2);
    assert(local_aligned[0][1].memref.strides[0] == 2);
    assert(local_aligned[0][0].memref.aligned == dense_base.data());
    assert(local_aligned[0][1].memref.aligned == dense_base.data() + 32);

    std::cout << "step alignment checks passed" << std::endl;
    std::cout << std::endl;
}

/// Test 9: mapping preserves the full logical extent of stepped regions.
void test_map_preserves_strided_extent() {
    std::cout << "=== Test 9: map preserves stepped extent across tile boundaries ==="
              << std::endl;

    ArrayRegion<1> parent_input({2}, {127}, {2});
    ArrayRegion<1> tile_input({64}, {127}, {2});
    ArrayRegion<1> parent_output({1}, {64}, {1});

    auto mapped = map(tile_input, parent_input, parent_output);
    assert(mapped.start[0] == 32);
    assert(mapped.stop[0] == 64);
    assert(mapped.step[0] == 1);
    assert(mapped.size(0) == tile_input.size(0));

    ArrayRegion<1> output_fragment({32}, {64}, {1});
    auto mapped_back = map(output_fragment, parent_output, parent_input);
    assert(mapped_back.start[0] == 64);
    assert(mapped_back.stop[0] == 128);
    assert(mapped_back.step[0] == 2);
    assert(mapped_back.size(0) == output_fragment.size(0));

    std::cout << "strided map checks passed" << std::endl;
    std::cout << std::endl;
}

// ------------------------------------------------------------------ //
//  Main
// ------------------------------------------------------------------ //

int main() {
    test_simple_add();
    test_fused_add_mul();
    test_scalar_mul();
    test_subview();
    test_all_binops();
    test_2d_add();
    test_2d_subview();
    test_align_fragments_step_strides();
    test_map_preserves_strided_extent();

    std::cout << "All JIT tests passed." << std::endl;
    return 0;
}
