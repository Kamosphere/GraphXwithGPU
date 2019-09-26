package edu.ustc.nodb.PregelGPU.example.SSSP

import edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.pregelSSSPShm
import edu.ustc.nodb.PregelGPU.plugin.graphGenerator
import edu.ustc.nodb.PregelGPU.{PregelGPU, envControl}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn

object SSSPGPUTest{

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

    val startNew = System.nanoTime()
    val ssspAlgo = new pregelSSSPShm(allSourceList, vertexSum, edgeSum, parts.get)
    val ssspGPUResult = PregelGPU.run(graph)(ssspAlgo)
    // val q = ssspGPUResult.vertices.count()
    println(ssspGPUResult.vertices.collect().mkString("\n"))
    val endNew = System.nanoTime()

    println("-------------------------")

    println(endNew - startNew)

    PregelGPU.close(ssspGPUResult, ssspAlgo)

    //val k = StdIn.readInt()
    //println(k)

  }

  // scalastyle:on println
}