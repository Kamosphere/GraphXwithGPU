#**************************************************************************************************
# cMake的公共配置
#**************************************************************************************************
cmake_minimum_required(VERSION 3.9)

project(GPUSSSP)

set(CMAKE_CXX_STANDARD 14)
set(GCC_COVERAGE_COMPILE_FLAGS  "-fPIC")
add_definitions(${GCC_COVERAGE_COMPILE_FLAGS})

#**************************************************************************************************
# Preprocessor definitions 一些预定义设置
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

#用$ENV{JAVA_HOME}获取 用于加载jni头文件 前提配置好JAVA_HOME
INCLUDE_DIRECTORIES($ENV{JAVA_HOME}/include)
if(WIN32)
    INCLUDE_DIRECTORIES($ENV{JAVA_HOME}/include/win32)
ELSEIF(APPLE)
    INCLUDE_DIRECTORIES($ENV{JAVA_HOME}/include/darwin)
ELSE()
    INCLUDE_DIRECTORIES($ENV{JAVA_HOME}/include/linux)
endif()

add_library(GPUSSSP SHARED
        edu_ustc_nodb_SSSP_GPUNative.h
        edu_ustc_nodb_SSSP_GPUNative.cpp)


target_link_libraries(GPUSSSP algo_BellmanFord algo_BellmanFordGPU)

