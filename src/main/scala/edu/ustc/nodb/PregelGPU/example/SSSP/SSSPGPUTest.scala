package edu.ustc.nodb.PregelGPU.example.SSSP

import edu.ustc.nodb.PregelGPU.algorithm.SSSP.pregel_SSSP
import edu.ustc.nodb.PregelGPU.plugin.graphGenerator
import edu.ustc.nodb.PregelGPU.plugin.partitionStrategy.EdgePartitionNumHookedTest
import edu.ustc.nodb.PregelGPU.{PregelGPU, PregelGPUSkipping, envControl}
import org.apache.spark.graphx.PartitionStrategy.{EdgePartition2D, RandomVertexCut}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn

object SSSPGPUTest{

  // scalastyle:on println

  def main(args: Array[String]) {

    // environment setting
    val conf = new SparkConf().setAppName("Pregel_SSSP")
    if(envControl.controller != 0){
      conf.setMaster("local[4]")
    }
    val sc = new SparkContext(conf)

    // part the graph shall be divided
    var parts = Some(args(0).toInt)
    if(parts.isEmpty) parts = Some(4)

    val definedGraphVertices = 1000000

    val preDefinedGraphVertices = definedGraphVertices / 4

    // load graph from file
    var sourceFile = "testGraph"+definedGraphVertices+".txt"
    if(envControl.controller == 0) {
      sourceFile = "/usr/local/ssspexample/" + sourceFile
    }

    envControl.skippingPartSize = preDefinedGraphVertices

    val graph = graphGenerator.readFile(sc, sourceFile)(parts.get)
      .partitionBy(EdgePartition2D)

    // running SSSP

    println("-------------------------")

    val sourceList = ArrayBuffer(1L, preDefinedGraphVertices * 1 + 2L,
      preDefinedGraphVertices * 2 + 4L,
      preDefinedGraphVertices * 3 + 7L).distinct.sorted

    val allSourceList = sc.broadcast(sourceList)

    // the quantity of vertices in the whole graph
    val vertexSum = graph.vertices.count()
    val edgeSum = graph.edges.count()

    if (! envControl.runningInSkip) {

      val startNew = System.nanoTime()
      val algorithm = new pregel_SSSP(allSourceList, vertexSum, edgeSum, parts.get)
      val GPUResult = PregelGPU.run(graph)(algorithm)
      // val q = ssspGPUResult.vertices.count()
      println(GPUResult.vertices.collect().mkString("\n"))
      val endNew = System.nanoTime()

      println("-------------------------")

      println(endNew - startNew)

      PregelGPU.close(GPUResult, algorithm)

    }
    else {

      val startNew = System.nanoTime()
      val algorithm = new pregel_SSSP(allSourceList, vertexSum, edgeSum, parts.get)
      val GPUResult = PregelGPUSkipping.run(graph)(algorithm)
      // val q = ssspGPUResult.vertices.count()
      println(GPUResult.vertices.collect().mkString("\n"))
      val endNew = System.nanoTime()

      println("-------------------------")

      println(endNew - startNew)

      PregelGPUSkipping.close(GPUResult, algorithm)
    }

    val k = StdIn.readInt()
    println(k)

  }

  // scalastyle:on println
}
