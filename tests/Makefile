# Makefile for standalone JIT test
# Requires: CHARMTYLES_HOME and MLIR/LLVM installed
#
# Usage:
#   make test_jit
#   ./test_jit

CXX       ?= clang++
CXXFLAGS  = -std=c++17 -O2 -g -DNDEBUG

# Include paths: charmtyles headers + example src headers (jit.hpp, opcodes.hpp, array_region.hpp)
# Auto-derive CHARMTYLES_HOME from this Makefile's location
CHARMTYLES_HOME ?= $(realpath $(dir $(lastword $(MAKEFILE_LIST)))../../..)

# Include paths: charmtyles headers + example src headers (jit.hpp, opcodes.hpp, array_region.hpp)
INCLUDES  = -I$(CHARMTYLES_HOME)/include \
            -I$(CHARMTYLES_HOME)/example/charmnumeric/src

# MLIR/LLVM flags
MLIR_CXXFLAGS := $(shell llvm-config --cxxflags 2>/dev/null)
MLIR_LDFLAGS  := $(shell llvm-config --ldflags --libs all --system-libs 2>/dev/null)

# MLIR libraries (adjust to your MLIR build)
MLIR_LIBS = -lMLIRLinalgTransforms -lMLIRLinalgDialect -lMLIRLinalgUtils \
            -lMLIRAffineTransforms -lMLIRAffineDialect -lMLIRAffineAnalysis -lMLIRAffineUtils \
            -lMLIRGPUDialect -lMLIRGPUTransforms \
            -lMLIRArithDialect -lMLIRArithUtils \
            -lMLIRMemRefDialect -lMLIRMemRefTransforms -lMLIRMemRefUtils \
            -lMLIRFuncDialect -lMLIRFuncTransforms \
            -lMLIRTensorDialect -lMLIRTensorUtils -lMLIRTensorTransforms \
            -lMLIRMathDialect \
            -lMLIRSCFDialect -lMLIRSCFTransforms -lMLIRSCFUtils \
            -lMLIRBufferizationDialect -lMLIRBufferizationTransforms \
            -lMLIRVectorDialect -lMLIRVectorTransforms -lMLIRVectorUtils \
            -lMLIRPass -lMLIRTransforms -lMLIRTransformUtils \
            -lMLIRSPIRVDialect -lMLIRSPIRVSerialization \
            -lMLIRLLVMDialect -lMLIRNVVMDialect -lMLIRROCDLDialect \
            -lMLIRLLVMToLLVMIRTranslation -lMLIRNVVMToLLVMIRTranslation \
            -lMLIRROCDLToLLVMIRTranslation \
            -lMLIRTargetLLVMIRExport \
            -lMLIRAnalysis -lMLIRSideEffectInterfaces -lMLIRControlFlowInterfaces \
            -lMLIRLoopLikeInterface -lMLIRViewLikeInterface \
            -lMLIRDestinationStyleOpInterface -lMLIRInferTypeOpInterface \
            -lMLIRDialectUtils -lMLIRRewrite \
            -lMLIRIR -lMLIRSupport -lMLIRParser

all: test_jit

test_jit: test_jit.cpp ../src/jit.hpp ../src/opcodes.hpp ../src/array_region.hpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MLIR_CXXFLAGS) \
		test_jit.cpp \
		$(MLIR_LDFLAGS) $(MLIR_LIBS) \
		-o $@

run: test_jit
	./test_jit

clean:
	rm -f test_jit

.PHONY: all run clean
