package edu.ustc.nodb.GPUGraphX.example.PageRank

import edu.ustc.nodb.GPUGraphX.algorithm.shm.PageRank.pregel_PRShm
import edu.ustc.nodb.GPUGraphX.plugin.graphGenerator
import edu.ustc.nodb.GPUGraphX.{PregelGPUShm, envControl}
import org.apache.spark.graphx.{EdgeDirection, Graph}
import org.apache.spark.graphx.PartitionStrategy._
import org.apache.spark.{SparkConf, SparkContext}


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


    //------for different type of graph

    var sourceFile: String = "/usr/local/sourcegraph/"

    val sourceFileName = envControl.datasetType match {
      case 0 =>
        val definedGraphVertices = envControl.allTestGraphVertices
        val preDefinedGraphVertices = definedGraphVertices / 4
        envControl.skippingPartSize = preDefinedGraphVertices
        "testGraph"+definedGraphVertices+".txt"
      case 1 =>
        "testGraph_road-road-usa.mtx.txt"
      case 2 =>
        "testGraph_com-orkut.ungraph.txt.txt"
      case 3 =>
        "testGraph_soc-LiveJournal1.txt.txt"
      case 4 =>
        "testGraph_wiki-topcats.txt.txt"
      case 5 =>
        "twitter.txt"
      case 6 =>
        "uk-200705graph.txt"
    }

    if(envControl.controller != 0){
      sourceFile = sourceFileName
    } else {
      sourceFile = sourceFile + sourceFileName
    }

    /*
    if(envControl.controller == 0) {
      conf.set("fs.defaultFS", "hdfs://192.168.1.2:9000")
      sourceFile = "hdfs://192.168.1.2:9000/sourcegraph/" + sourceFile
    }

     */

    val graph = graphGenerator.readFile(sc, sourceFile)(parts.get)
      .partitionBy(RandomVertexCut)

    // running PR

    println("-------------------------")

    val sourceList = 1L

    // the quantity of vertices in the whole graph
    val vertexSum = graph.vertices.count()
    val edgeSum = graph.edges.count()

    val algorithm = new pregel_PRShm(vertexSum, edgeSum,
      0.001, 0.15, parts.get)

    val startNew = System.nanoTime()

    val testGraph = algorithm.graphInit(graph)
    val GPUResult = PregelGPUShm.run(testGraph, algorithm)
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
