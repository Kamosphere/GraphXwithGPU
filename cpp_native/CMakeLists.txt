#**************************************************************************************************
# global config of cMake
#**************************************************************************************************
cmake_minimum_required(VERSION 3.9)

project(PregelGPU)

set(CMAKE_CXX_STANDARD 14)
set(GCC_COVERAGE_COMPILE_FLAGS  "-fPIC")
add_definitions(${GCC_COVERAGE_COMPILE_FLAGS})

#**************************************************************************************************
# Preprocessor definitions
#**************************************************************************************************
if (CMAKE_CL_64)
    add_definitions(-DWIN64)
    set(PLATFORM x64)
else (CMAKE_CL_64)
    set(PLATFORM x86)
endif (CMAKE_CL_64)

if(WIN32)
    ADD_DEFINITIONS(-DIGTB_EXPORTS)
    ADD_DEFINITIONS(-D_WIN32_WINNT=0x0501)
endif(WIN32)

add_subdirectory(Graph_Algo)
include_directories(Graph_Algo)

add_subdirectory(util)
include_directories(util)

#use $ENV{JAVA_HOME} to include the jni header, JAVA_HOME need to be configurated
INCLUDE_DIRECTORIES($ENV{JAVA_HOME}/include)
if(WIN32)
    INCLUDE_DIRECTORIES($ENV{JAVA_HOME}/include/win32)
ELSEIF(APPLE)
    INCLUDE_DIRECTORIES($ENV{JAVA_HOME}/include/darwin)
ELSE()
    INCLUDE_DIRECTORIES($ENV{JAVA_HOME}/include/linux)
endif()


add_library(PregelGPU SHARED
        edu_ustc_nodb_PregelGPU_Algorithm_SSSP_GPUNative.h
        edu_ustc_nodb_PregelGPU_Algorithm_SSSP_GPUNative.cpp)

target_link_libraries(PregelGPU
        srv_UtilClient
        initProber
        JNIPlugin)

add_library(PregelGPUShm SHARED
        edu_ustc_nodb_PregelGPU_Algorithm_SSSPshm_GPUNativeShm.h
        edu_ustc_nodb_PregelGPU_Algorithm_SSSPshm_GPUNativeShm.cpp)

target_link_libraries(PregelGPUShm
        srv_UtilClient
        initProber
        JNIPlugin
        rt)

add_library(PregelGPUBatch SHARED
        edu_ustc_nodb_PregelGPU_Algorithm_SSSPBatch_GPUNativeBatch.h
        edu_ustc_nodb_PregelGPU_Algorithm_SSSPBatch_GPUNativeBatch.cpp)

target_link_libraries(PregelGPUBatch
        srv_cli_BatchClient_BellmanFord
        dataProber
        initProber
        JNIPlugin)
