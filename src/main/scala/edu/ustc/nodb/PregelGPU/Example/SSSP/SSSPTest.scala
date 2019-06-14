package edu.ustc.nodb.PregelGPU.Example.SSSP

import edu.ustc.nodb.PregelGPU.Algorithm.SSSP.pregelSSSP
import edu.ustc.nodb.PregelGPU.Plugin.partitionStrategy.EdgePartitionPreSearch
import edu.ustc.nodb.PregelGPU.{PregelInGPU, envControl}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn

object SSSPTest{

  def main(args: Array[String]) {

    // environment setting
    val conf = new SparkConf().setAppName("Pregel_SSSP").setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // part the graph shall be divided
    var parts = Some(args(0).toInt)
    if(parts.isEmpty) parts =Some(4)

    /*
    // generate the graph randomly

    val random = new Random()
    val listRandom = new ListBuffer[Double]()
    val numbersVertex = 10000
    for (i <- 0 to numbersVertex){
      listRandom+=(random.nextDouble()*100/9).toInt.toDouble
    }
    listRandom.toList
    val graph: Graph[Long, Double] =
      GraphGenerators.logNormalGraph(sc, numVertices = numbersVertex).mapEdges(e => e.attr.toDouble+listRandom(e.srcId.toInt))

    println(graph.edges.count())

    */

    // load graph from file

    var sourceFile = ""
    if(envControl.controller == 0){
      sourceFile = "/usr/local/ssspexample/testGraph.txt"
    }
    else sourceFile = "testGraph.txt"

    val vertex: RDD[(VertexId, VertexId)] = sc.textFile(sourceFile).map{
      lines =>{
        val para = lines.split(" ")
        (para(0).toLong, para(0).toLong)
      }
    }.repartition(parts.get)
    val edge: RDD[Edge[Double]] = sc.textFile(sourceFile).map{
      lines =>{
        val para = lines.split(" ")
        val q = para(2).toDouble
        Edge(para(0).toLong, para(1).toLong, q)
      }
    }.repartition(parts.get)

    val graph = Graph(vertex, edge)
    println(graph.edges.count())

    //running two versions of SSSP

    println("-------------------------")

    val sourceList = ArrayBuffer(1L, 2L, 4L, 7L).distinct.sorted

    val allSourceList = sc.broadcast(sourceList)

    // the quantity of vertices in the whole graph
    val vertexSum = graph.vertices.count()
    val edgeSum = graph.edges.count()
/*
    val startNormal = System.nanoTime()
    val ssspTest = new PregelSparkSSSP(graph, allSourceList)
    val ssspResult = ssspTest.run()
    //val d = ssspResult.vertices.count()
    //println(ssspResult.vertices.collect.mkString("\n"))
    val endNormal = System.nanoTime()
*/
    println("-------------------------")

    val startNew = System.nanoTime()
    val ssspAlgo = new pregelSSSP(allSourceList, vertexSum, edgeSum, parts.get)
    val ssspGPUResult = PregelInGPU.run(graph)(ssspAlgo)
    val q = ssspGPUResult.vertices.count()
    //println(ssspGPUResult.vertices.collect.mkString("\n"))
    val endNew = System.nanoTime()

    println("-------------------------")

    //println(endNormal - startNormal)

    println(endNew - startNew)

    PregelInGPU.close(ssspGPUResult)
    val k = StdIn.readInt()
    println(k)

  }
}
