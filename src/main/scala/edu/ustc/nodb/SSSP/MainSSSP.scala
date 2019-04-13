package edu.ustc.nodb.SSSP

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.io.StdIn

object MainSSSP{

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

    val sourceFile = "testGraph.txt"
    //val sourceFile = "/usr/local/ssspexample/testGraph.txt"

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

    val sourceList = List(1L, 2L, 4L)

    val allSourceList = sc.broadcast(sourceList)

    // the quantity of vertices in the whole graph
    val vertexNumbers = graph.vertices.count()
    val edgeNumbers = graph.edges.count()

    val startNormal = System.nanoTime()
    val ssspTest = new PregelSparkSSSP(graph, allSourceList)
    val ssspResult = ssspTest.run()
    val d = ssspResult.vertices.count()
    //println(ssspResult.vertices.collect.mkString("\n"))
    val endNormal = System.nanoTime()

    println("-------------------------")

    val startNew = System.nanoTime()
    val ssspGPUResult = SSSPinGPU.run(graph, allSourceList, vertexNumbers, edgeNumbers, parts.get)
    val q = ssspGPUResult.vertices.count()
    //println(ssspGPUResult.vertices.collect.mkString("\n"))
    val endNew = System.nanoTime()

    println("-------------------------")

    println(endNormal - startNormal)

    println(endNew - startNew)

    SSSPinGPU.close(ssspGPUResult)
    //val k = StdIn.readInt()
    //println(k)

  }
}
