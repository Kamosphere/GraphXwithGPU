package edu.ustc.nodb.PregelGPU.example.SSSP

import edu.ustc.nodb.PregelGPU.plugin.graphGenerator
import edu.ustc.nodb.PregelGPU.envControl
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object SSSPSparkTest{

  // scalastyle:on println

  def main(args: Array[String]) {

    // environment setting
    val conf = new SparkConf().setAppName("Pregel_SSSP").setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // part the graph shall be divided
    var parts = Some(args(0).toInt)
    if(parts.isEmpty) parts = Some(4)

    // load graph from file
    var sourceFile = ""
    if(envControl.controller == 0) {
      sourceFile = "/usr/local/ssspexample/testGraph.txt"
    }
    else sourceFile = "testGraph.txt"

    val graph = graphGenerator.readFile(sc, sourceFile)(parts.get)

    // running two versions of SSSP

    println("-------------------------")

    val sourceList = ArrayBuffer(1L, 2L, 4L, 7L).distinct.sorted

    val allSourceList = sc.broadcast(sourceList)

    // the quantity of vertices in the whole graph
    val vertexSum = graph.vertices.count()
    val edgeSum = graph.edges.count()

    envControl.skippingPartSize = (vertexSum / parts.get).toInt

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

