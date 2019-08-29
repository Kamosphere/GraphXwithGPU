package edu.ustc.nodb.PregelGPU.Plugin

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.util.Random

object graphGenerator {

  def logNormalGraph(sc: SparkContext, numbersVertex: Int): Graph[Long, Double] = {

    val random = new Random()
    val listRandom = new ListBuffer[Double]()

    for (i <- 0 to numbersVertex){
      listRandom += (random.nextDouble() * 100 / 9)
    }

    val graph: Graph[Long, Double] = GraphGenerators
      .logNormalGraph(sc, numbersVertex)
      .mapEdges(e => e.attr.toDouble + listRandom(e.srcId.toInt))

    // Trigger of spark RDD
    println(graph.edges.count())

    graph
  }

  def readFile(sc: SparkContext, sourceFile: String)(implicit parts: Int = 4): Graph[Long, Double] ={

    val vertex: RDD[(VertexId, VertexId)] = sc.textFile(sourceFile).map{
      lines =>{
        val para = lines.split(" ")
        (para(0).toLong, para(0).toLong)
      }
    }.repartition(parts)
    val edge: RDD[Edge[Double]] = sc.textFile(sourceFile).map{
      lines =>{
        val para = lines.split(" ")
        val q = para(2).toDouble
        Edge(para(0).toLong, para(1).toLong, q)
      }
    }.repartition(parts)

    val graph = Graph(vertex, edge)

    //trigger of spark RDD
    println(graph.edges.count())

    graph
  }
}
