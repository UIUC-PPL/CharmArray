#include "jit.hpp"

MLIRJitCompiler::MLIRJitCompiler() {
    // 1. Load required dialects
    context.getOrLoadDialect<mlir::linalg::LinalgDialect>();
    context.getOrLoadDialect<mlir::affine::AffineDialect>();
    context.getOrLoadDialect<mlir::gpu::GPUDialect>();
    context.getOrLoadDialect<mlir::arith::ArithDialect>();
    context.getOrLoadDialect<mlir::memref::MemRefDialect>();
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::scf::SCFDialect>();
    context.getOrLoadDialect<mlir::LLVM::LLVMDialect>();
    context.getOrLoadDialect<mlir::NVVM::NVVMDialect>();
    context.getOrLoadDialect<mlir::ROCDL::ROCDLDialect>();
    context.getOrLoadDialect<mlir::spirv::SPIRVDialect>();
    context.getOrLoadDialect<mlir::math::MathDialect>();

    // 2. Register LLVM IR translation interfaces (needed by ExecutionEngine)
    mlir::DialectRegistry registry;
    mlir::registerAllToLLVMIRTranslations(registry);
    context.appendDialectRegistry(registry);

    // 3. Initialize Module
    module = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
    builder = std::make_unique<mlir::OpBuilder>(&context);
}

void MLIRJitCompiler::buildFromAST(void* astPtr) {
    using namespace mlir;
    AST* ast = static_cast<AST*>(astPtr);
    auto loc = builder->getUnknownLoc();

    // --- 1. Collect leaf nodes ---
    std::vector<LeafUse> memrefLeaves;     // non-scalar leaves → memref func args
    std::vector<ASTNode*> scalarLeaves;    // scalar leaves → constants
    std::vector<ASTNode*> broadcastLeaves; // size-1 arrays → scalar func args
    collectLeaves(ast, memrefLeaves, scalarLeaves, broadcastLeaves);

    // Collect non-temp root operations as output slots.
    // Each gets a caller-provided output memref appended after the input args.
    std::vector<ASTNode*> outputRoots;
    for (ASTNode* op : ast->roots) {
        auto opc = static_cast<Opcode>(op->opcode);
        if (opc != Opcode::NOOP && opc != Opcode::CREATE && !op->is_temp)
            outputRoots.push_back(op);
    }

    // Determine element type from the AST's dtype
    mlir::Type elemType;
    DType astDtype = DType::FLOAT32;
    for (ASTNode* op : ast->roots)
        if (op->dtype != DType::FLOAT32) {
            astDtype = op->dtype;
            break;
        }
    switch (astDtype) {
    case DType::FLOAT64:
        elemType = Float64Type::get(&context);
        break;
    case DType::INT32:
        elemType = IntegerType::get(&context, 32);
        break;
    case DType::INT64:
        elemType = IntegerType::get(&context, 64);
        break;
    default:
        elemType = Float32Type::get(&context);
        break;
    }

    auto dynamicStridedMemRefType = [&](int64_t rank) {
        SmallVector<int64_t> dynShape(rank, ShapedType::kDynamic);
        SmallVector<int64_t> dynStrides(rank, ShapedType::kDynamic);
        auto layout = StridedLayoutAttr::get(&context, ShapedType::kDynamic, dynStrides);
        return MemRefType::get(dynShape, elemType, layout);
    };

    // --- 2. Create a func.func: memref inputs, broadcast scalars, then output memrefs ---
    SmallVector<Type> argTypes;
    for (const LeafUse& leaf : memrefLeaves)
        argTypes.push_back(dynamicStridedMemRefType(leaf.operand->ndims));
    // Broadcast leaves are scalar function arguments (runtime values, not constants)
    for (size_t i = 0; i < broadcastLeaves.size(); ++i)
        argTypes.push_back(elemType);
    for (ASTNode* root : outputRoots)
        argTypes.push_back(dynamicStridedMemRefType(root->ndims));
    auto funcType = FunctionType::get(&context, argTypes, /*results=*/{});
    auto funcOp = func::FuncOp::create(loc, "fused_kernel", funcType);
    module->push_back(funcOp);

    Block* entry = funcOp.addEntryBlock();
    builder->setInsertionPointToStart(entry);

    // Seed memref leaves with block arguments
    nodeValues.clear();
    leafValues.clear();
    tempValues.clear();
    for (size_t i = 0; i < memrefLeaves.size(); ++i)
        leafValues[leafUseKey(memrefLeaves[i].operand, memrefLeaves[i].region)] =
            entry->getArgument(i);

    // Seed broadcast leaves with their scalar block arguments
    for (size_t i = 0; i < broadcastLeaves.size(); ++i)
        nodeValues[broadcastLeaves[i]->result_name] =
            entry->getArgument(memrefLeaves.size() + i);

    // Seed scalar leaves as arith.constant values
    for (ASTNode* sn : scalarLeaves) {
        mlir::TypedAttr attr;
        if (mlir::isa<IntegerType>(elemType))
            attr = builder->getIntegerAttr(elemType, static_cast<int64_t>(sn->scalar));
        else
            attr = builder->getFloatAttr(elemType, static_cast<double>(sn->scalar));
        auto val = arith::ConstantOp::create(*builder, loc, attr);
        nodeValues[sn->result_name] = val.getResult();
    }

    // --- 3. Process each operation sequentially (flat list) ---
    for (ASTNode* op : ast->roots)
        emitOp(op, loc);

    // --- 3b. Copy each non-temp root result into its caller-provided output arg ---
    //
    // Use linalg.generic instead of memref.copy: the default-layout memref
    // type (memref<?x?xf32>) causes memref.copy to lower to a flat memcpy,
    // which is incorrect when source and destination have different strides
    // (e.g., a 14×14 contiguous temp copied into a 14×14 view of a 16×16
    // array with stride [16,1]).  A linalg.generic copy lowers to affine
    // loops that respect each operand's stride descriptor and participates
    // in affine loop fusion, eliminating the intermediate allocation.
    size_t outArgIdx = memrefLeaves.size() + broadcastLeaves.size();
    for (ASTNode* root : outputRoots) {
        Value result = nodeValues[root->result_name];
        Value outArg = entry->getArgument(outArgIdx++);
        if (mlir::isa<MemRefType>(result.getType())) {
            auto outType = mlir::cast<MemRefType>(outArg.getType());
            int64_t rank = outType.getRank();
            auto identityMap = builder->getMultiDimIdentityMap(rank);
            SmallVector<AffineMap, 2> copyMaps = {identityMap, identityMap};
            SmallVector<utils::IteratorType> copyIters(rank, utils::IteratorType::parallel);
            linalg::GenericOp::create(*builder, loc, TypeRange{}, ValueRange{result},
                                               ValueRange{outArg}, copyMaps, copyIters,
                                               [](OpBuilder& nb, Location nl, ValueRange args) {
                                                   linalg::YieldOp::create(nb, nl, args[0]);
                                               });
        } else {
            // Scalar result (e.g., SET_REGION with scalar RHS) — fill output
            linalg::FillOp::create(*builder, loc, result, outArg);
        }
    }

    // --- 4. Deallocate temporaries (after all consumers have read) ---
    for (mlir::Value tmp : tempValues)
        memref::DeallocOp::create(*builder, loc, tmp);

    // --- 5. Terminate the function body ---
    func::ReturnOp::create(*builder, loc);
}

bool MLIRJitCompiler::optimizeAndFuse() {
    mlir::PassManager pm(&context);
    auto& funcPM = pm.nest<mlir::func::FuncOp>();

#if defined(USE_NVIDIA) || defined(USE_AMD) || defined(USE_INTEL)
    // GPU path: fuse at Linalg level, keeping linalg.generic ops intact
    // for later conversion to scf.parallel → gpu.launch.
    funcPM.addPass(mlir::createLinalgElementwiseOpFusionPass());
#else
    // CPU path: convert to Affine and use the powerful Affine Fusion pass.
    // A. Convert Linalg to Affine loops
    funcPM.addPass(mlir::createConvertLinalgToAffineLoopsPass());

    // B. Maximal Loop Fusion: removes intermediate array results
    // by merging producer loops into consumer loops.
    funcPM.addPass(mlir::affine::createLoopFusionPass(0,          /* fastMemorySpace */
                                                      UINT64_MAX, /* localBufSizeThreshold */
                                                      true,       /* maximalFusion */
                                                      mlir::affine::FusionMode::ProducerConsumer));

    // C. Scalar Replacement: Promotes intermediate MemRefs to registers/SSA
    funcPM.addPass(mlir::affine::createAffineScalarReplacementPass());
#endif

    return mlir::succeeded(pm.run(*module));
}

std::unique_ptr<mlir::ExecutionEngine> MLIRJitCompiler::generateCPU() {
    if (!lowerToCPU())
        return nullptr;

    // Initialize native target (required for JIT on the host CPU)
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();

    mlir::ExecutionEngineOptions engineOptions;
    auto maybeEngine = mlir::ExecutionEngine::create(*module, engineOptions);
    if (!maybeEngine) {
        llvm::errs() << "Failed to create ExecutionEngine\n";
        return nullptr;
    }

    return std::move(*maybeEngine);
}

bool MLIRJitCompiler::loadCPU(std::unique_ptr<mlir::ExecutionEngine>& engine,
                              const char* kernelName, void** moduleOut, void** functionOut) {
    if (!engine)
        return false;

    auto funcPtrOrErr = engine->lookupPacked(kernelName);
    if (!funcPtrOrErr)
        return false;

    *moduleOut = (void*)engine.release();
    *functionOut = (void*)*funcPtrOrErr;
    return true;
}

void MLIRJitCompiler::dumpMLIR() { module->dump(); }

// ---- Private helpers ----

void MLIRJitCompiler::collectLeaves(AST* ast, std::vector<LeafUse>& memrefLeaves,
                                    std::vector<ASTNode*>& scalarLeaves,
                                    std::vector<ASTNode*>& broadcastLeaves) {
    // Build set of result_names produced internally by roots in this fused kernel.
    // Operands referencing these names are internal intermediates, not external inputs.
    std::unordered_set<int> internalResults;
    for (ASTNode* root : ast->roots)
        internalResults.insert(root->result_name);

    std::unordered_set<int> seenBroadcasts;

    for (ASTNode* root : ast->roots) {
        auto root_opc = static_cast<Opcode>(root->opcode);

        for (int op_idx = 0; op_idx < (int)root->operands.size(); ++op_idx) {
            ASTNode* operand = root->operands[op_idx];
            if (!operand)
                continue;

            if (internalResults.count(operand->result_name))
                continue;

            // Skip SET_REGION's target operand (operands[0]) — it's a write
            // destination handled by the copy-back, not a JIT input.
            if (root_opc == Opcode::SET_REGION && op_idx == 0)
                continue;

            auto op = static_cast<Opcode>(operand->opcode);
            if (op == Opcode::NOOP || op == Opcode::CREATE) {
                if (operand->is_scalar) {
                    scalarLeaves.push_back(operand);
                } else if (operand->is_broadcast) {
                    if (seenBroadcasts.insert(operand->result_name).second)
                        broadcastLeaves.push_back(operand);
                } else {
                    memrefLeaves.push_back({operand, root->get_operand_region(op_idx)});
                }
            }
        }
    }
}

std::string MLIRJitCompiler::leafUseKey(ASTNode* operand, Region* region) const {
    std::string key = std::to_string(operand->result_name) + "|" + std::to_string(operand->ndims);
    if (region == nullptr)
        return key + "|null";
    if (region->is_global)
        return key + "|global";

    switch (operand->ndims) {
    case 1:
        return key + "|" + fmt_region(*static_cast<ArrayRegion<1>*>(region));
    case 2:
        return key + "|" + fmt_region(*static_cast<ArrayRegion<2>*>(region));
    case 3:
        return key + "|" + fmt_region(*static_cast<ArrayRegion<3>*>(region));
    default:
        return key + "|nd=" + std::to_string(operand->ndims);
    }
}

mlir::Value MLIRJitCompiler::lookupOperand(ASTNode* operand, Region* region) {
    if (region != nullptr) {
        auto leafIt = leafValues.find(leafUseKey(operand, region));
        if (leafIt != leafValues.end())
            return leafIt->second;
    }
    auto it = nodeValues.find(operand->result_name);
    if (it != nodeValues.end())
        return it->second;
    return {}; // should not happen if AST is well-formed
}

void MLIRJitCompiler::emitOp(ASTNode* node, mlir::Location loc) {
    using namespace mlir;

    auto op = static_cast<Opcode>(node->opcode);
    Value result;

    switch (op) {
    case Opcode::NOOP:
    case Opcode::CREATE:
        return;

    case Opcode::SET_REGION: {
        result = lookupOperand(node->operands[1], node->get_operand_region(1));
        break;
    }

    default: {
        // Binary elementwise ops
        auto& binReg = binaryOpRegistry();
        auto binIt = binReg.find(node->opcode);
        if (binIt != binReg.end()) {
            Value lhs = lookupOperand(node->operands[0], node->get_operand_region(0));
            Value rhs = lookupOperand(node->operands[1], node->get_operand_region(1));

            bool lhsIsScalar = !mlir::isa<MemRefType>(lhs.getType());
            bool rhsIsScalar = !mlir::isa<MemRefType>(rhs.getType());

            auto elemType = lhsIsScalar ? lhs.getType() :
                mlir::cast<MemRefType>(lhs.getType()).getElementType();
            bool isInt = mlir::isa<IntegerType>(elemType);
            const auto& entry = binIt->second;
            const auto& bodyFn = (isInt && entry.int_body) ? entry.int_body : entry.float_body;

            if (lhsIsScalar && rhsIsScalar) {
                // Both operands are scalars — emit a plain scalar op
                result = bodyFn(*builder, loc, lhs, rhs);
                break;
            }

            Value arrayOperand = lhsIsScalar ? rhs : lhs;
            auto srcType = mlir::cast<MemRefType>(arrayOperand.getType());
            int64_t rank = srcType.getRank();
            SmallVector<int64_t> dynShape(rank, ShapedType::kDynamic);
            auto outType = MemRefType::get(dynShape, elemType);

            SmallVector<Value> dimSizes;
            for (int64_t d = 0; d < rank; ++d)
                dimSizes.push_back(memref::DimOp::create(*builder, loc, arrayOperand, d).getResult());
            result = memref::AllocOp::create(*builder, loc, outType, ValueRange(dimSizes));

            emitGenericArrayOp(lhs, rhs, result, bodyFn);

            if (node->is_temp)
                tempValues.push_back(result);
            break;
        }

        // Unary elementwise ops
        auto& unReg = unaryOpRegistry();
        auto unIt = unReg.find(node->opcode);
        if (unIt != unReg.end()) {
            Value input = lookupOperand(node->operands[0], node->get_operand_region(0));
            auto srcType = mlir::cast<MemRefType>(input.getType());
            int64_t rank = srcType.getRank();
            auto elemType = srcType.getElementType();
            SmallVector<int64_t> dynShape(rank, ShapedType::kDynamic);
            auto outType = MemRefType::get(dynShape, elemType);

            SmallVector<Value> dimSizes;
            for (int64_t d = 0; d < rank; ++d)
                dimSizes.push_back(memref::DimOp::create(*builder, loc, input, d).getResult());
            result = memref::AllocOp::create(*builder, loc, outType, ValueRange(dimSizes));

            bool isInt = mlir::isa<IntegerType>(elemType);
            const auto& entry = unIt->second;
            const auto& bodyFn = (isInt && entry.int_body) ? entry.int_body : entry.float_body;
            emitGenericUnaryOp(input, result, bodyFn);

            if (node->is_temp)
                tempValues.push_back(result);
            break;
        }

        llvm::errs() << "Unknown opcode in JIT emitOp: " << node->opcode << "\n";
        return;
    }
    }

    nodeValues[node->result_name] = result;
}

bool MLIRJitCompiler::lowerToCPU() {
    mlir::PassManager pm(&context);
    // Lower any remaining Linalg ops to Affine loops (no-op if optimizeAndFuse() ran first)
    pm.addPass(mlir::createConvertLinalgToAffineLoopsPass());
    // Lower Affine to SCF (structured control flow)
    pm.addPass(mlir::createLowerAffinePass());
    // Lower SCF to CF (unstructured control flow)
    pm.addPass(mlir::createSCFToControlFlowPass());
    // Decompose memref.subview and similar ops into lower-level memref ops
    pm.addPass(mlir::memref::createExpandStridedMetadataPass());
    // Lower CF, Arith, MemRef, Func to LLVM dialect
    pm.addPass(mlir::createConvertControlFlowToLLVMPass());
    pm.addPass(mlir::createConvertMathToLLVMPass());
    pm.addPass(mlir::createArithToLLVMConversionPass());
    pm.addPass(mlir::createConvertIndexToLLVMPass());
    pm.addPass(mlir::createFinalizeMemRefToLLVMConversionPass());
    pm.addPass(mlir::createConvertFuncToLLVMPass());
    // Reconcile unrealized conversion casts left by partial lowerings
    pm.addPass(mlir::createReconcileUnrealizedCastsPass());
    return mlir::succeeded(pm.run(*module));
}

// ---- Elementwise op registries ----

const std::unordered_map<int, BinaryOpEntry>& MLIRJitCompiler::binaryOpRegistry() {
    static const std::unordered_map<int, BinaryOpEntry> registry = {
        {(int)Opcode::ADD,
         {[](mlir::OpBuilder& b, mlir::Location loc, mlir::Value lhs, mlir::Value rhs) {
              return mlir::arith::AddFOp::create(b, loc, lhs, rhs).getResult();
          },
          [](mlir::OpBuilder& b, mlir::Location loc, mlir::Value lhs, mlir::Value rhs) {
              return mlir::arith::AddIOp::create(b, loc, lhs, rhs).getResult();
          }}},
        {(int)Opcode::SUB,
         {[](mlir::OpBuilder& b, mlir::Location loc, mlir::Value lhs, mlir::Value rhs) {
              return mlir::arith::SubFOp::create(b, loc, lhs, rhs).getResult();
          },
          [](mlir::OpBuilder& b, mlir::Location loc, mlir::Value lhs, mlir::Value rhs) {
              return mlir::arith::SubIOp::create(b, loc, lhs, rhs).getResult();
          }}},
        {(int)Opcode::MUL,
         {[](mlir::OpBuilder& b, mlir::Location loc, mlir::Value lhs, mlir::Value rhs) {
              return mlir::arith::MulFOp::create(b, loc, lhs, rhs).getResult();
          },
          [](mlir::OpBuilder& b, mlir::Location loc, mlir::Value lhs, mlir::Value rhs) {
              return mlir::arith::MulIOp::create(b, loc, lhs, rhs).getResult();
          }}},
        {(int)Opcode::DIV,
         {[](mlir::OpBuilder& b, mlir::Location loc, mlir::Value lhs, mlir::Value rhs) {
              return mlir::arith::DivFOp::create(b, loc, lhs, rhs).getResult();
          },
          [](mlir::OpBuilder& b, mlir::Location loc, mlir::Value lhs, mlir::Value rhs) {
              return mlir::arith::DivSIOp::create(b, loc, lhs, rhs).getResult();
          }}},
    };
    return registry;
}

const std::unordered_map<int, UnaryOpEntry>& MLIRJitCompiler::unaryOpRegistry() {
    static const std::unordered_map<int, UnaryOpEntry> registry = {
        {(int)Opcode::TANH,
         {[](mlir::OpBuilder& b, mlir::Location loc, mlir::Value v) {
              return mlir::math::TanhOp::create(b, loc, v).getResult();
          },
          nullptr}},
        {(int)Opcode::EXP,
         {[](mlir::OpBuilder& b, mlir::Location loc, mlir::Value v) {
              return mlir::math::ExpOp::create(b, loc, v).getResult();
          },
          nullptr}},
    };
    return registry;
}

#ifdef JIT_ENABLE_GPU_BACKEND

std::string MLIRJitCompiler::generateNVIDIA() {
    if (!lowerToGPU())
        return "";

    // Lower ops inside gpu.module to LLVM + NVVM dialects
    mlir::PassManager pm(&context);
    auto& gpuPM = pm.nest<mlir::gpu::GPUModuleOp>();
    gpuPM.addPass(mlir::createConvertMathToLLVMPass());
    gpuPM.addPass(mlir::createArithToLLVMConversionPass());
    gpuPM.addPass(mlir::createConvertIndexToLLVMPass());
    gpuPM.addPass(mlir::createFinalizeMemRefToLLVMConversionPass());
    gpuPM.addPass(mlir::createConvertFuncToLLVMPass());
    gpuPM.addPass(mlir::createConvertGpuOpsToNVVMOps());
    gpuPM.addPass(mlir::createReconcileUnrealizedCastsPass());
    if (mlir::failed(pm.run(*module)))
        return "";

    return serializeToPTX();
}

std::string MLIRJitCompiler::generateAMD() {
    if (!lowerToGPU())
        return "";

    // Lower ops inside gpu.module to LLVM + ROCDL dialects
    mlir::PassManager pm(&context);
    auto& gpuPM = pm.nest<mlir::gpu::GPUModuleOp>();
    gpuPM.addPass(mlir::createConvertMathToLLVMPass());
    gpuPM.addPass(mlir::createArithToLLVMConversionPass());
    gpuPM.addPass(mlir::createConvertIndexToLLVMPass());
    gpuPM.addPass(mlir::createFinalizeMemRefToLLVMConversionPass());
    gpuPM.addPass(mlir::createConvertFuncToLLVMPass());
    gpuPM.addPass(mlir::createConvertGpuOpsToROCDLOps());
    gpuPM.addPass(mlir::createReconcileUnrealizedCastsPass());
    if (mlir::failed(pm.run(*module)))
        return "";

    return serializeToGCN();
}

std::string MLIRJitCompiler::generateIntel() {
    if (!lowerToGPU())
        return "";

    // Convert GPU module contents to SPIR-V
    mlir::PassManager pm(&context);
    pm.addPass(mlir::createConvertGPUToSPIRVPass());
    auto& spvPM = pm.nest<mlir::spirv::ModuleOp>();
    spvPM.addPass(mlir::spirv::createSPIRVLowerABIAttributesPass());
    spvPM.addPass(mlir::spirv::createSPIRVUpdateVCEPass());
    if (mlir::failed(pm.run(*module)))
        return "";

    return serializeToSPIRV();
}

bool MLIRJitCompiler::loadNVIDIA(const std::string& ptx, const char* kernelName, void** moduleOut,
                                 void** functionOut) {
#if defined(USE_NVIDIA)
    CUmodule cuModule;
    CUfunction cuFunction;
    CUresult res;

    res = cuModuleLoadDataEx(&cuModule, ptx.c_str(), 0, 0, 0);
    if (res != CUDA_SUCCESS)
        return false;

    res = cuModuleGetFunction(&cuFunction, cuModule, kernelName);
    if (res != CUDA_SUCCESS)
        return false;

    *moduleOut = (void*)cuModule;
    *functionOut = (void*)cuFunction;
    return true;
#else
    return false;
#endif
}

bool MLIRJitCompiler::loadAMD(const std::string& gcn, const char* kernelName, void** moduleOut,
                              void** functionOut) {
#if defined(USE_AMD)
    hipModule_t hipModule;
    hipFunction_t hipFunction;
    hipError_t res;

    res = hipModuleLoadData(&hipModule, gcn.data());
    if (res != hipSuccess)
        return false;

    res = hipModuleGetFunction(&hipFunction, hipModule, kernelName);
    if (res != hipSuccess)
        return false;

    *moduleOut = (void*)hipModule;
    *functionOut = (void*)hipFunction;
    return true;
#else
    return false;
#endif
}

bool MLIRJitCompiler::loadIntel(const std::string& spirv, const char* kernelName, void** moduleOut,
                                void** functionOut) {
#if defined(USE_INTEL)
    // Initialize Level Zero
    ze_result_t res = zeInit(ZE_INIT_FLAG_GPU_ONLY);
    if (res != ZE_RESULT_SUCCESS)
        return false;

    // Get driver
    uint32_t driverCount = 1;
    ze_driver_handle_t driver;
    res = zeDriverGet(&driverCount, &driver);
    if (res != ZE_RESULT_SUCCESS)
        return false;

    // Get device
    uint32_t deviceCount = 1;
    ze_device_handle_t device;
    res = zeDeviceGet(driver, &deviceCount, &device);
    if (res != ZE_RESULT_SUCCESS)
        return false;

    // Create context
    ze_context_desc_t ctxDesc = {ZE_STRUCTURE_TYPE_CONTEXT_DESC, nullptr, 0};
    ze_context_handle_t zeContext;
    res = zeContextCreate(driver, &ctxDesc, &zeContext);
    if (res != ZE_RESULT_SUCCESS)
        return false;

    // Create module from SPIR-V binary
    ze_module_desc_t moduleDesc = {};
    moduleDesc.stype = ZE_STRUCTURE_TYPE_MODULE_DESC;
    moduleDesc.format = ZE_MODULE_FORMAT_IL_SPIRV;
    moduleDesc.inputSize = spirv.size();
    moduleDesc.pInputModule = reinterpret_cast<const uint8_t*>(spirv.data());

    ze_module_handle_t zeModule;
    ze_module_build_log_handle_t buildLog;
    res = zeModuleCreate(zeContext, device, &moduleDesc, &zeModule, &buildLog);
    if (res != ZE_RESULT_SUCCESS)
        return false;

    // Get kernel
    ze_kernel_desc_t kernelDesc = {};
    kernelDesc.stype = ZE_STRUCTURE_TYPE_KERNEL_DESC;
    kernelDesc.pKernelName = kernelName;
    ze_kernel_handle_t zeKernel;
    res = zeKernelCreate(zeModule, &kernelDesc, &zeKernel);
    if (res != ZE_RESULT_SUCCESS)
        return false;

    *moduleOut = reinterpret_cast<void*>(zeModule);
    *functionOut = reinterpret_cast<void*>(zeKernel);
    return true;
#else
    return false;
#endif
}

bool MLIRJitCompiler::lowerToGPU() {
    mlir::PassManager pm(&context);
    // Convert linalg.generic to scf.parallel (NOT affine.for)
    pm.addPass(mlir::createConvertLinalgToParallelLoopsPass());
    // Annotate scf.parallel loops with GPU mapping attributes (block/thread dims)
    pm.nest<mlir::func::FuncOp>().addPass(mlir::createGpuMapParallelLoopsPass());
    // Convert annotated scf.parallel to gpu.launch
    pm.addPass(mlir::createConvertParallelLoopToGpuPass());
    // Outline gpu.launch bodies into gpu.func inside gpu.module
    pm.addPass(mlir::createGpuKernelOutliningPass());
    return mlir::succeeded(pm.run(*module));
}

std::string MLIRJitCompiler::translateToTarget(llvm::StringRef triple, llvm::StringRef cpu,
                                               llvm::StringRef features,
                                               llvm::CodeGenFileType fileType) {
    // 1. Translate MLIR module to LLVM IR
    llvm::LLVMContext llvmContext;
    auto llvmModule = mlir::translateModuleToLLVMIR(*module, llvmContext);
    if (!llvmModule)
        return "";

    llvmModule->setTargetTriple(triple);

    // 2. Look up the LLVM target
    std::string error;
    const llvm::Target* target = llvm::TargetRegistry::lookupTarget(triple, error);
    if (!target)
        return "";

    // 3. Create a TargetMachine
    llvm::TargetOptions opts;
    std::unique_ptr<llvm::TargetMachine> tm(
        target->createTargetMachine(triple, cpu, features, opts, llvm::Reloc::Model::PIC_));
    if (!tm)
        return "";

    llvmModule->setDataLayout(tm->createDataLayout());

    // 4. Emit to a string (assembly or object depending on fileType)
    std::string outStr;
    llvm::raw_string_ostream os(outStr);
    llvm::legacy::PassManager pass;
    if (tm->addPassesToEmitFile(pass, os, nullptr, fileType))
        return "";

    pass.run(*llvmModule);
    os.flush();
    return outStr;
}

std::string MLIRJitCompiler::serializeToPTX() {
    llvm::InitializeAllTargets();
    llvm::InitializeAllTargetMCs();
    llvm::InitializeAllAsmPrinters();
    mlir::registerNVVMDialectTranslation(context);

    return translateToTarget(
        /*triple=*/"nvptx64-nvidia-cuda",
        /*cpu=*/"sm_70",
        /*features=*/"+ptx60", llvm::CodeGenFileType::AssemblyFile);
}

std::string MLIRJitCompiler::serializeToGCN() {
    llvm::InitializeAllTargets();
    llvm::InitializeAllTargetMCs();
    llvm::InitializeAllAsmPrinters();
    mlir::registerROCDLDialectTranslation(context);

    // AMD HSA requires an ELF code object, not assembly text
    return translateToTarget(
        /*triple=*/"amdgcn-amd-amdhsa",
        /*cpu=*/"gfx908",
        /*features=*/"", llvm::CodeGenFileType::ObjectFile);
}

std::string MLIRJitCompiler::serializeToSPIRV() {
    std::string result;

    // Walk the module looking for spirv.module ops and serialize each one
    module->walk([&](mlir::spirv::ModuleOp spvModule) {
        llvm::SmallVector<uint32_t, 0> binary;
        if (mlir::succeeded(mlir::spirv::serialize(spvModule, binary))) {
            // Append raw SPIR-V words to the output
            result.append(reinterpret_cast<const char*>(binary.data()),
                          binary.size() * sizeof(uint32_t));
        }
    });

    return result;
}

#endif // JIT_ENABLE_GPU_BACKEND
