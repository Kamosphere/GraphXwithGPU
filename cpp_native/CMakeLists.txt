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
        edu_ustc_nodb_PregelGPU_Algorithm_SSSP_GPUNative.cpp prober/initProber.cpp prober/initProber.h)


target_link_libraries(PregelGPU
        srv_UtilClient)

