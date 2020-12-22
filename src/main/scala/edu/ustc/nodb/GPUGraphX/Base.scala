package edu.ustc.nodb.GPUGraphX


import edu.ustc.nodb.GPUGraphX.algorithm.shm.CC.pregel_CCShm
import edu.ustc.nodb.GPUGraphX.algorithm.shm.LP.pregel_LPShm
import edu.ustc.nodb.GPUGraphX.algorithm.shm.PageRank.pregel_PRShm
import edu.ustc.nodb.GPUGraphX.algorithm.shm.SSSP.pregel_SSSPShm
import edu.ustc.nodb.GPUGraphX.algorithm.shm.algoShmTemplete
import edu.ustc.nodb.GPUGraphX.plugin.graphGenerator
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object Base {

  case class Config(master: String = "", graphFile: String = "",
                    nPartitions: Int = -1, defaultAlgo: String = "")

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Config]("GPUGraphX") {
      opt[String]("master") valueName "<master>" action {
        (x, c) => c.copy(master = x)
      } text "sc.setMaster"

      opt[String]("graphfile") valueName "<graphFile>" action {
        (x, c) => c.copy(graphFile = x)
      } text "input graph file"

      opt[Int]("npartitions") valueName "<nPartitions>" action {
        (x, c) => c.copy(nPartitions = x)
      } text "split partitions"

      opt[String]("defaultalgo") valueName "<defaultAlgo>" action {
        (x, c) => c.copy(defaultAlgo = x)
      } text "algorithms including sssp/lp/pr/cc"
    }

    val conf = new SparkConf().setAppName("GPUGraphX").setMaster("local")

    var graphFileDir : String = ""
    var partitionCount : Int = 4
    var defaultAlgo : String = ""

    parser.parse(args, Config()) match {
      case Some(config) =>
        if(! config.master.isEmpty) conf.setMaster(config.master)
        graphFileDir = config.graphFile
        partitionCount = if (config.nPartitions > 0) config.nPartitions else 4
        defaultAlgo = config.defaultAlgo

      case None =>
    }

    val sc = new SparkContext(conf)

    val graph = graphGenerator.readFile(sc, graphFileDir)(partitionCount)
      .partitionBy(RandomVertexCut)

    val sourceList = ArrayBuffer[Long]()
    val allSourceList = sc.broadcast(sourceList)

    // the quantity of vertices in the whole graph
    val vertexSum = graph.vertices.count()
    val edgeSum = graph.edges.count()

    var processedGraph : Graph[_,_] = graph

    val algo : algoShmTemplete[_,_,_] = defaultAlgo match {
      case "sssp" =>
        def makeMap(x: (VertexId, Double)*): Map[VertexId, Double] = Map(x: _*)
        val definedGraphVertices = vertexSum
        val preDefinedGraphVertices = definedGraphVertices / 4
        sourceList += (1L, preDefinedGraphVertices * 1 + 1L,
          preDefinedGraphVertices * 2 + 1L,
          preDefinedGraphVertices * 3 + 1L)
        processedGraph = graph.mapVertices { (vid, attr) =>
          if (allSourceList.value.contains(vid)) makeMap(vid -> 0) else makeMap()
        }
        new pregel_SSSPShm(allSourceList, vertexSum, edgeSum, partitionCount)

      case "lp" =>
        processedGraph = graph.mapVertices { case (vid, _) => (vid, 0L) }
        new pregel_LPShm(vertexSum, edgeSum, partitionCount)

      case "pr" =>
        processedGraph = graphInitPR(graph, 1)
        new pregel_PRShm(vertexSum, edgeSum, 0.001, 0.15, partitionCount)

      case "cc" =>
        processedGraph = graph.mapVertices { case (vid, _) => vid }
        new pregel_CCShm(vertexSum, edgeSum, partitionCount)
    }

    execution(processedGraph.asInstanceOf[Graph[Any, Any]],
      algo.asInstanceOf[algoShmTemplete[Any, Any, _]])

  }

  def execution[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (graph: Graph[VD, ED], algo: algoShmTemplete[VD, ED, A]): Unit ={

    val startNew = System.nanoTime()

    val GPUResult = PregelGPUShm.run(graph, algo)
    // val q = ssspGPUResult.vertices.count()
    println(GPUResult.vertices.take(100000).mkString("\n"))
    val endNew = System.nanoTime()

    println("-------------------------")

    println(endNew - startNew)

    PregelGPUShm.close(GPUResult, algo)
  }


  def graphInitPR[VD: ClassTag, ED: ClassTag]
  (graph: Graph[VD, ED], initSrc: Int): Graph[(Double, Double), Double] = {
    graph.outerJoinVertices(graph.outDegrees) {
      (vid, vdata, deg) => deg.getOrElse(0)
    }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr )
      // Set the vertex attributes to (initialPR, delta = 0)
      .mapVertices { (id, attr) =>
        if (id == initSrc) (0.0, Double.NegativeInfinity) else (0.0, 0.0)
      }
      .persist()
  }
}
