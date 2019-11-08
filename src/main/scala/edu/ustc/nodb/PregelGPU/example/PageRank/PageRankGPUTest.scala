package edu.ustc.nodb.PregelGPU.example.PageRank

import edu.ustc.nodb.PregelGPU.algorithm.LPA.pregel_LPAShm
import edu.ustc.nodb.PregelGPU.algorithm.PageRank.pregel_PRShm
import edu.ustc.nodb.PregelGPU.algorithm.SSSP.pregel_SSSP
import edu.ustc.nodb.PregelGPU.plugin.graphGenerator
import edu.ustc.nodb.PregelGPU.{PregelGPU, PregelGPUShm, PregelGPUSkipping, envControl}
import org.apache.spark.graphx.{EdgeDirection, Graph, GraphXUtils}
import org.apache.spark.graphx.PartitionStrategy.{EdgePartition2D, RandomVertexCut}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn

object PageRankGPUTest{

  // scalastyle:on println

  def main(args: Array[String]) {

    // environment setting
    val conf = new SparkConf().setAppName("Pregel_PR_GPU")
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
      conf.set("fs.defaultFS", "hdfs://192.168.1.10:9000")
      sourceFile = "hdfs://192.168.1.10:9000/sourcegraph/" + sourceFile
    }

    envControl.skippingPartSize = preDefinedGraphVertices

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    GraphXUtils.registerKryoClasses(conf)

    val graph = graphGenerator.readFile(sc, sourceFile)(parts.get)
      .partitionBy(RandomVertexCut)

    // running PR

    println("-------------------------")

    val sourceList = 1L

    val shmIdentifier = new Array[String](4)
    shmIdentifier(0) = "ID"
    shmIdentifier(1) = "Active"
    shmIdentifier(2) = "PairSource1"
    shmIdentifier(3) = "PairSource2"

    // the quantity of vertices in the whole graph
    val vertexSum = graph.vertices.count()
    val edgeSum = graph.edges.count()

    val algorithm = new pregel_PRShm(shmIdentifier,
      vertexSum, edgeSum, 0.001, 0.15, parts.get)

    val startNew = System.nanoTime()

    val testGraph = algorithm.graphInit(graph)
    val GPUResult = PregelGPUShm.run(testGraph, EdgeDirection.Either)(algorithm)
    val GPUNormalize = GPUResult.mapVertices((vid, attr) => attr._1)
    val normalizedResult = normalizeRankSum(GPUNormalize, algorithm.personalized)
    // val q = ssspGPUResult.vertices.count()
    println(normalizedResult.vertices.take(100000).mkString("\n"))
    val endNew = System.nanoTime()

    println("-------------------------")

    println(endNew - startNew)

    PregelGPUShm.close(GPUResult, algorithm)

    //val k = StdIn.readInt()
    //println(k)

  }

  // Normalizes the sum of ranks to n (or 1 if personalized)
  def normalizeRankSum(rankGraph: Graph[Double, Double], personalized: Boolean): Graph[Double, Double] = {
    val rankSum = rankGraph.vertices.values.sum()
    if (personalized) {
      rankGraph.mapVertices((id, rank) => rank / rankSum)
    } else {
      val numVertices = rankGraph.numVertices
      val correctionFactor = numVertices.toDouble / rankSum
      rankGraph.mapVertices((id, rank) => rank * correctionFactor)
    }
  }

  // scalastyle:on println
}
