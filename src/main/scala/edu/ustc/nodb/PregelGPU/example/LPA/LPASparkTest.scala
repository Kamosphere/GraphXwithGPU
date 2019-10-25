package edu.ustc.nodb.PregelGPU.example.LPA

import edu.ustc.nodb.PregelGPU.envControl
import edu.ustc.nodb.PregelGPU.plugin.graphGenerator
import org.apache.spark.graphx.PartitionStrategy.{EdgePartition2D, RandomVertexCut}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object LPASparkTest{

  // scalastyle:on println

  def main(args: Array[String]) {

    // environment setting
    val conf = new SparkConf().setAppName("Pregel_LPA")
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

    // running LPA

    println("-------------------------")

    // the quantity of vertices in the whole graph
    val vertexSum = graph.vertices.count()
    val edgeSum = graph.edges.count()

    val LPATest = new PregelSparkLPA(graph)

    val startNormal = System.nanoTime()
    val LPAResult = LPATest.run(50)
    // val d = ssspResult.vertices.count()
    val endNormal = System.nanoTime()
    println(LPAResult.vertices.collect().mkString("\n"))

    println("-------------------------")

    println(endNormal - startNormal)

  }

  // scalastyle:on println
}

