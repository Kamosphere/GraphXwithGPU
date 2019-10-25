package edu.ustc.nodb.PregelGPU.example.SSSP

import edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.pregel_SSSPShm
import edu.ustc.nodb.PregelGPU.plugin.graphGenerator
import edu.ustc.nodb.PregelGPU.plugin.partitionStrategy.EdgePartitionNumHookedTest
import edu.ustc.nodb.PregelGPU.{PregelGPUShm, envControl}
import org.apache.spark.graphx.PartitionStrategy.{EdgePartition2D, RandomVertexCut}
import org.apache.spark.graphx.VertexId
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object SSSPGPUShmTest{

  // scalastyle:on println
  def makeMap(x: (VertexId, Double)*): Map[VertexId, Double] = Map(x: _*)

  def main(args: Array[String]) {

    // environment setting
    val conf = new SparkConf().setAppName("Pregel_SSSP_GPU")
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
      .partitionBy(EdgePartition2D)

    // running SSSP

    println("-------------------------")

    val sourceList = ArrayBuffer(1L, preDefinedGraphVertices * 1 + 2L,
      preDefinedGraphVertices * 2 + 4L,
      preDefinedGraphVertices * 3 + 7L).distinct.sorted

    val allSourceList = sc.broadcast(sourceList)

    val shmIdentifier = new Array[String](3)
    shmIdentifier(0) = "ID"
    shmIdentifier(1) = "active"
    shmIdentifier(2) = "Attr"

    // the quantity of vertices in the whole graph
    val vertexSum = graph.vertices.count()
    val edgeSum = graph.edges.count()

    val algorithm = new pregel_SSSPShm(allSourceList, shmIdentifier, vertexSum, edgeSum, parts.get)

    val startNew = System.nanoTime()

    val spGraph = graph.mapVertices { (vid, attr) =>
      if (allSourceList.value.contains(vid)) makeMap(vid -> 0) else makeMap()
    }.cache()
    val GPUResult = PregelGPUShm.run(spGraph)(algorithm)
    // val q = ssspGPUResult.vertices.count()
    println(GPUResult.vertices.collect().mkString("\n"))
    val endNew = System.nanoTime()

    println("-------------------------")

    println(endNew - startNew)

    PregelGPUShm.close(GPUResult, algorithm)

    //val k = StdIn.readInt()
    //println(k)

  }

  // scalastyle:on println
}
