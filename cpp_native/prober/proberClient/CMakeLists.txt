cmake_minimum_required(VERSION 3.9)
project(prober)

set(CMAKE_CXX_STANDARD 14)

add_library(srv_cli_BatchClient_BellmanFord
        BellmanFord/pBellmanFordClient.cpp
        BellmanFord/pBellmanFordClient.h)

target_link_libraries(srv_cli_BatchClient_BellmanFord
        srv_UNIX_shm
        srv_UNIX_msg
        srv_UtilServer
        srv_UtilClient
        algo_BellmanFord
        algo_BellmanFordGPU)

add_executable(algo_pBellmanFordBatchClientTest
        pBellmanFordClientTest.cpp)

target_link_libraries(algo_pBellmanFordBatchClientTest
        srv_cli_BatchClient_BellmanFord)