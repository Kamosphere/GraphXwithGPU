package edu.ustc.nodb.GPUGraphX.plugin

import org.apache.spark.SparkContext
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.util.Random

object graphGenerator {

  // scalastyle:off println

  // generate log graph
  def logNormalGraph(sc: SparkContext, numbersVertex: Int): Graph[Long, Double] = {

    val random = new Random()
    val listRandom = new ListBuffer[Double]()

    for (i <- 0 to numbersVertex) {
      listRandom += (random.nextDouble() * 100 / 9)
    }

    val graph: Graph[Long, Double] = GraphGenerators
      .logNormalGraph(sc, numbersVertex)
      .mapEdges(e => e.attr.toDouble + listRandom(e.srcId.toInt))

    // Trigger of spark RDD
    println(graph.edges.count())

    graph
  }

  // read graph file
  def readFile(sc: SparkContext, sourceFile: String)
              (implicit parts: Int = 4): Graph[Long, Double] = {

    val edge: RDD[Edge[Double]] = sc.textFile(sourceFile).map{
      lines => {
        val para = lines.split("\\s+")
        var q = 1.0
        if(para.length > 2) {
          q = para(2).toDouble
        }
        Edge(para(0).toLong, para(1).toLong, q)
      }
    }.repartition(parts)

    val maxNode = edge.flatMap(e => Iterator(math.max(e.srcId, e.dstId))).reduce((x, y) => math.max(x, y))

    val vertex: RDD[(VertexId, VertexId)] = sc.parallelize(0L to maxNode, parts).map( v => {
      (v, v)
    }).repartition(parts)

    val graph = Graph(vertex, edge)

    // trigger of spark RDD
    println(graph.vertices.count())
    println(graph.edges.count())

    graph
  }

  // scalastyle:on println
}
