cmake_minimum_required(VERSION 3.9)
project(prober)

set(CMAKE_CXX_STANDARD 14)

add_library(dataProber
        dataProber.cpp
        dataProber.h)

add_library(initProber
        initProber.cpp
        initProber.h)

target_link_libraries(dataProber
        srv_UNIX_msg
        srv_UNIX_shm)

target_link_libraries(initProber
        srv_UNIX_msg)