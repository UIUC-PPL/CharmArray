#include "backend.hpp"
#include "jit.hpp"

#include <string>

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
