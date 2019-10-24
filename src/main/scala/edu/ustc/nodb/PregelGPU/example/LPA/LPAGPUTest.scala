package edu.ustc.nodb.PregelGPU.example.LPA

import edu.ustc.nodb.PregelGPU.algorithm.LPA.pregel_LPAShm
import edu.ustc.nodb.PregelGPU.algorithm.SSSP.pregel_SSSP
import edu.ustc.nodb.PregelGPU.plugin.graphGenerator
import edu.ustc.nodb.PregelGPU.{PregelGPU, PregelGPUShm, PregelGPUSkipping, envControl}
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.PartitionStrategy.{EdgePartition2D, RandomVertexCut}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn

object LPAGPUTest{

  // scalastyle:on println

  def main(args: Array[String]) {

    // environment setting
    val conf = new SparkConf().setAppName("Pregel_LPA_GPU")
    if(envControl.controller != 0){
      conf.setMaster("local[4]")
    }
    val sc = new SparkContext(conf)
    if(envControl.controller != 0){
      sc.setLogLevel("ERROR")
    }

    // part the graph shall be divided
    var parts = Some(args(0).toInt)
    if(parts.isEmpty) parts = Some(4)

    val definedGraphVertices = envControl.allTestGraphVertices

    val preDefinedGraphVertices = definedGraphVertices / 4

    // load graph from file
    var sourceFile = "testGraph"+definedGraphVertices+".txt"
    if(envControl.controller == 0) {
      sourceFile = "/usr/local/sourcegraph/" + sourceFile
    }

    envControl.skippingPartSize = preDefinedGraphVertices

    val graph = graphGenerator.readFile(sc, sourceFile)(parts.get)
      .partitionBy(RandomVertexCut)

    // running SSSP

    println("-------------------------")

    val shmIdentifier = new Array[String](4)
    shmIdentifier(0) = "ID"
    shmIdentifier(1) = "Active"
    shmIdentifier(2) = "PairSource1"
    shmIdentifier(3) = "PairSource2"

    // the quantity of vertices in the whole graph
    val vertexSum = graph.vertices.count()
    val edgeSum = graph.edges.count()

    val algorithm = new pregel_LPAShm(shmIdentifier, vertexSum, edgeSum, parts.get)

    val lpaGraph = graph.mapVertices { case (vid, _) => (vid, 0L) }

    val startNew = System.nanoTime()
    val GPUResult = PregelGPUShm.run(lpaGraph, EdgeDirection.Out, 50)(algorithm)
    // val q = ssspGPUResult.vertices.count()
    println(GPUResult.vertices.collect().mkString("\n"))
    val endNew = System.nanoTime()

    println("-------------------------")

    println(endNew - startNew)

    PregelGPUShm.close(GPUResult, algorithm)

    val k = StdIn.readInt()
    println(k)

  }

  // scalastyle:on println
}
