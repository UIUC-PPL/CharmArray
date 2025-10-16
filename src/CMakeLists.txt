cmake_minimum_required(VERSION 3.16)
project(charmTyles)

include(${CMAKE_SOURCE_DIR}/../config.cmake)

set(Kokkos_ROOT ${KOKKOS_DIR})
find_package(Kokkos 4.5 REQUIRED CONFIG)

if(Charm_ENABLE_GPU)
    message(STATUS "Building for a GPU backend")
    add_definitions(-DGPU_BACKEND)
    add_definitions(-DCUDA_DIR=\"${CUDA_DIR}\")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${GPU_OPTS} ${LD_OPTS}")
else()
    message(STATUS "Building for a CPU backend")
    set(CMAKE_CXX_COMPILER "${CHARMC}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CPU_OPTS} ${LD_OPTS}")
endif()

add_definitions(-DKOKKOS_DIR=\"${KOKKOS_DIR}\")

add_custom_command(
    OUTPUT  ${BASE_DIR}/charmtyles/backend/libcharmtyles.decl.h
    COMMAND ${CMAKE_COMMAND} -E chdir ${BASE_DIR}/charmtyles/backend
            ${CHARMC} ${BASE_DIR}/charmtyles/backend/charmtyles.ci
    DEPENDS ${BASE_DIR}/charmtyles/backend/charmtyles.ci
    COMMENT "Processing charmtyles ci files"
)

add_custom_command(
    OUTPUT  ${CMAKE_SOURCE_DIR}/server.decl.h
    COMMAND ${CMAKE_COMMAND} -E chdir ${CMAKE_SOURCE_DIR}
            ${CHARMC} ${CMAKE_SOURCE_DIR}/server.ci
    DEPENDS ${CMAKE_SOURCE_DIR}/server.ci
    COMMENT "Processing server ci files"
)

if(Charm_ENABLE_GPU)
    add_library(server OBJECT server.cpp ${BASE_DIR}/charmtyles/backend/libcharmtyles.decl.h ${CMAKE_SOURCE_DIR}/server.decl.h)
    target_include_directories(server PRIVATE ${BASE_DIR} ${BASE_DIR}/charmtyles/backend ${EIGEN_DIR}/include ${CHARM_DIR}/include)
    target_link_libraries(server Kokkos::kokkos)

    add_custom_command(
        OUTPUT "${CMAKE_BINARY_DIR}/server.out"
        COMMAND ${CHARMC} ${GPU_LINK_OPTS} $<TARGET_OBJECTS:server> -o ${CMAKE_BINARY_DIR}/server.out
        DEPENDS server
        COMMENT "Linking charm build against kokkos and cuda"
    )

    add_custom_target(result ALL
        DEPENDS "${CMAKE_BINARY_DIR}/server.out"
    )
else()
    add_executable(server.out server.cpp ${BASE_DIR}/charmtyles/backend/libcharmtyles.decl.h ${CMAKE_SOURCE_DIR}/server.decl.h)
    target_include_directories(server.out PRIVATE ${BASE_DIR} ${BASE_DIR}/charmtyles/backend ${EIGEN_DIR})
    target_link_libraries(server.out Kokkos::kokkos)
endif()
