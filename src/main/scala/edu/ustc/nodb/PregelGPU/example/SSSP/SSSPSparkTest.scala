package edu.ustc.nodb.PregelGPU.example.SSSP

import edu.ustc.nodb.PregelGPU.plugin.graphGenerator
import edu.ustc.nodb.PregelGPU.envControl
import edu.ustc.nodb.PregelGPU.plugin.partitionStrategy.EdgePartitionNumHookedTest
import org.apache.spark.graphx.PartitionStrategy.{EdgePartition2D, RandomVertexCut}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object SSSPSparkTest{

  // scalastyle:on println

  def main(args: Array[String]) {

    // environment setting
    val conf = new SparkConf().setAppName("Pregel_SSSP")
    if(envControl.controller != 0){
      conf.setMaster("local[4]")
    }
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // part the graph shall be divided
    var parts = Some(args(0).toInt)
    if(parts.isEmpty) parts = Some(4)

    val definedGraphVertices = 40000

    val preDefinedGraphVertices = definedGraphVertices / 4

    // load graph from file
    var sourceFile = "testGraph"+preDefinedGraphVertices+"x4.txt"
    if(envControl.controller == 0) {
      sourceFile = "/usr/local/ssspexample/" + sourceFile
    }

    envControl.skippingPartSize = preDefinedGraphVertices

    val graph = graphGenerator.readFile(sc, sourceFile)(parts.get)
      .partitionBy(EdgePartitionNumHookedTest)

    // running SSSP

    println("-------------------------")

    val sourceList = ArrayBuffer(1L, preDefinedGraphVertices * 1 + 2L,
      preDefinedGraphVertices * 2 + 4L,
      preDefinedGraphVertices * 3 + 7L).distinct.sorted

    val allSourceList = sc.broadcast(sourceList)

    // the quantity of vertices in the whole graph
    val vertexSum = graph.vertices.count()
    val edgeSum = graph.edges.count()

    val startNormal = System.nanoTime()
    val ssspTest = new PregelSparkSSSP(graph, allSourceList)
    val ssspResult = ssspTest.run()
    // val d = ssspResult.vertices.count()
    val endNormal = System.nanoTime()
    println(ssspResult.vertices.collect().mkString("\n"))

    println("-------------------------")

    println(endNormal - startNormal)

  }

  // scalastyle:on println
}

