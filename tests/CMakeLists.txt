cmake_minimum_required(VERSION 3.20)
project(test_jit LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(DEFINED ENV{CHARM_HOME} AND NOT DEFINED CHARM_HOME)
    set(CHARM_HOME "$ENV{CHARM_HOME}")
endif()
if(NOT DEFINED CHARM_HOME)
    message(FATAL_ERROR "CHARM_HOME is not set. Point it to your Charm++ installation.")
endif()

# ---------- Find MLIR (and LLVM transitively) ----------
find_package(MLIR REQUIRED CONFIG)
find_package(LLVM REQUIRED CONFIG)

message(STATUS "Using MLIRConfig.cmake in: ${MLIR_DIR}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")

list(APPEND CMAKE_MODULE_PATH "${MLIR_CMAKE_DIR}")
list(APPEND CMAKE_MODULE_PATH "${LLVM_CMAKE_DIR}")

include(TableGen)
include(AddLLVM)
include(AddMLIR)

include_directories(${LLVM_INCLUDE_DIRS})
include_directories(${MLIR_INCLUDE_DIRS})
include_directories(${CHARM_HOME}/include)

# ---------- Charmtyles / example includes ----------
set(CHARMTYLES_HOME "${CMAKE_CURRENT_SOURCE_DIR}/../../..")
include_directories(${CHARMTYLES_HOME}/include)
include_directories(${CHARMTYLES_HOME}/example/charmnumeric/src)

# ---------- Build the test ----------
add_executable(test_jit test_jit.cpp)
add_executable(test_compute_decompositions test_compute_decompositions.cpp)
target_compile_definitions(test_compute_decompositions PRIVATE
    CT_MIN_TILE_1D=64
    CT_MIN_TILE_2D=64
    CT_MIN_TILE_3D=64
)

# Use MLIR's cmake helper to resolve ALL transitive deps automatically
get_property(mlir_dialect_libs GLOBAL PROPERTY MLIR_DIALECT_LIBS)
get_property(mlir_conversion_libs GLOBAL PROPERTY MLIR_CONVERSION_LIBS)

target_link_libraries(test_jit
    PRIVATE
    ${mlir_dialect_libs}
    ${mlir_conversion_libs}
    MLIRPass
    MLIRTransforms
    MLIRTargetLLVMIRExport
    MLIRLLVMToLLVMIRTranslation
    MLIRNVVMToLLVMIRTranslation
    MLIRROCDLToLLVMIRTranslation
    MLIRIR
    MLIRSupport
    MLIRParser
    LLVMCore
    LLVMSupport
)

enable_testing()
add_test(NAME test_compute_decompositions COMMAND test_compute_decompositions)
