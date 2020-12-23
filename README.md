# GraphXwithGPU

This project is a demo of spark-based graph processing system. 

The purpose of this project is to fully utilize the computing ability of emerging hardware and to accelerate heavy computation tasks such graph processing for distributed system, as well as to achieve well-behaved workload balancing and task scheduling for GPU processing kernel by using managers built in distributed system.

## System Requirement

A suitable C++14 compiler is necessary (such as clang, g++-5 and so on)

Also, you still need a cmake with version newer than 3.9 to compile it easily, but it's no matter whether you use cmake or not.

CUDA is needed for GPU version

Scala 2.11.12 is recommended for capability.

Please use spark 2.4.8-USTC-NODB version in order to support some function 

In order to running demo,  extra c++ library is required at https://github.com/thoh-testarossa/Graph_Algo,
and please use spark 2.4.8-USTC-NODB version https://github.com/Kamosphere/spark-GPUGraphX

## Config

This project includes a simple script for UNIX-like system to compile automatically. Just run the autoBuild.sh in {project_path}

./cpp_native/autoBuild.sh

before running application

## Usage

Take ShortestPath algorithm for example:

After compiling cpp code, deploy the jar file and cpp code and run like this:

 spark-submit --class edu.ustc.nodb.GPUGraphX.example.SSSP.{SSSPGPUTest, SSSPSparkTest} \
 --master "{master-url}" \
 --conf "{other config}" \
 --driver-library-path="{cpp code path}" \
 "{jar file path}" "{partition amount or GPU amount}"
 
 If you run the application in this method, be sure that the graph file is stored in the path written in the source code.
 
 To change the graph file you can use this entrance like：
 
 spark-submit --class edu.ustc.nodb.GPUGraphX.Base \
 --master "{master-url}" \
 --conf "{other config}" \
 --driver-library-path="{cpp code path}" \
 "{jar file path}" \
 "--master "{master-url}" --graphfile "{graph file path}" --npartitions "{amount of partitions}" --defaultalgo "{algorithm that want to execute}""

see more information at https://github.com/thoh-testarossa/Graph_Algo

## Todo List

Executing cost reduce

Cost model analysis

## Thanks

The project is developed by the following members:

@thoh-testarossa Thoh Testarossa

@Kamosphere Kamosphere

@cave-g-f cave-g-f

Thanks Prof.Xike Xie for the guidance.
