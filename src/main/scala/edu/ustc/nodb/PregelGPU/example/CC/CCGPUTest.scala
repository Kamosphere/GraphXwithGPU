package edu.ustc.nodb.PregelGPU.example.CC

import edu.ustc.nodb.PregelGPU.algorithm.CC.pregel_CCShm
import edu.ustc.nodb.PregelGPU.plugin.graphGenerator
import edu.ustc.nodb.PregelGPU.{PregelGPUShm, envControl}
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.PartitionStrategy._
import org.apache.spark.{SparkConf, SparkContext}

object CCGPUTest{

  // scalastyle:on println

  def main(args: Array[String]) {

    // environment setting
    val conf = new SparkConf().setAppName("Pregel_CC_GPU")
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

    var sourceFile: String = ""

    sourceFile = envControl.datasetType match {
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
    }

    if(envControl.controller == 0) {
      conf.set("fs.defaultFS", "hdfs://192.168.1.2:9000")
      sourceFile = "hdfs://192.168.1.2:9000/sourcegraph/" + sourceFile
    }

    val graph = graphGenerator.readFile(sc, sourceFile)(parts.get)
      .partitionBy(RandomVertexCut)

    // running CC

    println("-------------------------")

    val shmIdentifier = new Array[String](3)
    shmIdentifier(0) = "ID"
    shmIdentifier(1) = "Active"
    shmIdentifier(2) = "Attr"

    // the quantity of vertices in the whole graph
    val vertexSum = graph.vertices.count()
    val edgeSum = graph.edges.count()

    val algorithm = new pregel_CCShm(shmIdentifier, vertexSum, edgeSum, parts.get)

    val ccGraph = graph.mapVertices { case (vid, _) => vid }

    val startNew = System.nanoTime()
    val GPUResult = PregelGPUShm.run(ccGraph, EdgeDirection.Either, 20)(algorithm)
    // val q = ssspGPUResult.vertices.count()
    println(GPUResult.vertices.take(100000).mkString("\n"))
    val endNew = System.nanoTime()

    println("-------------------------")

    println(endNew - startNew)

    PregelGPUShm.close(GPUResult, algorithm)

    //val k = StdIn.readInt()
    //println(k)

  }

  // scalastyle:on println
}
