package edu.ustc.nodb.GPUGraphX.example.SSSP

import edu.ustc.nodb.GPUGraphX.algorithm.array.SSSP.pregel_SSSP
import edu.ustc.nodb.GPUGraphX.plugin.graphGenerator
import edu.ustc.nodb.GPUGraphX.plugin.partitionStrategy._
import edu.ustc.nodb.GPUGraphX.{PregelGPUSkipping, envControl}
import org.apache.spark.graphx.PartitionStrategy._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object SSSPGPUTest{

  // scalastyle:on println

  def main(args: Array[String]) {

    // environment setting
    val conf = new SparkConf().setAppName("Pregel_SSSP_GPU_Array")
    if(envControl.controller != 0){
      conf.setMaster("local[4]").set("spark.executor.memory", "60g").set("spark.driver.memory", "60g")
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

    val sourceList = ArrayBuffer[Long]()

    val sourceFileName = envControl.datasetType match {
      case 0 =>
        val definedGraphVertices = envControl.allTestGraphVertices
        val preDefinedGraphVertices = definedGraphVertices / 4
        envControl.skippingPartSize = preDefinedGraphVertices
        sourceList += (1L, preDefinedGraphVertices * 1 + 2L,
          preDefinedGraphVertices * 2 + 4L,
          preDefinedGraphVertices * 3 + 7L)
        "testGraph"+definedGraphVertices+".txt"
      case 1 =>
        sourceList += (828192L, 9808777L,
          13425140L,
          22675645L)
        "testGraph_road-road-usa.mtx.txt"
      case 2 =>
        sourceList += (435368L, 1052416L,
          1700856L,
          2425532L)
        "testGraph_com-orkut.ungraph.txt.txt"
      case 3 =>
        sourceList += (101295L, 1117363L,
          2030267L,
          3202807L)
        "soc-relabel-graph.txt"
      case 4 =>
        sourceList += (377335L, 584383L,
          1101295L,
          1400923L)
        "wiki-relabel-washed.txt"
      case 5 =>
        sourceList += (0L, 3640680L,
          18450777L)
        "twitter.txt"
      case 6 =>
        sourceList += (337151L, 22138482L,
          35528773L)
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

    // WRNASIA 3019787
    /*
    val sourceList = ArrayBuffer(91820L, 3716785L,
      7053267L,
      11122108L).distinct.sorted
     */

    val allSourceList = sc.broadcast(sourceList)

    // the quantity of vertices in the whole graph
    val vertexSum = graph.vertices.count()
    val edgeSum = graph.edges.count()

    val startNew = System.nanoTime()
    val algorithm = new pregel_SSSP(allSourceList, vertexSum, edgeSum, parts.get)
    val GPUResult = PregelGPUSkipping.run(graph)(algorithm)
    // val q = ssspGPUResult.vertices.count()
    println(GPUResult.vertices.take(100000).mkString("\n"))
    val endNew = System.nanoTime()

    println("-------------------------")

    println(endNew - startNew)

    PregelGPUSkipping.close(GPUResult, algorithm)

    /*
    if (! envControl.runningInSkip) {

      val startNew = System.nanoTime()
      val algorithm = new pregel_SSSP(allSourceList, vertexSum, edgeSum, parts.get)
      val GPUResult = PregelGPU.run(graph)(algorithm)
      // val q = ssspGPUResult.vertices.count()
      println(GPUResult.vertices.take(100).mkString("\n"))
      val endNew = System.nanoTime()

      println("-------------------------")

      println(endNew - startNew)

      PregelGPU.close(GPUResult, algorithm)

    }
    else {

      val startNew = System.nanoTime()
      val algorithm = new pregel_SSSP(allSourceList, vertexSum, edgeSum, parts.get)
      val GPUResult = PregelGPUSkipping.run(graph)(algorithm)
      // val q = ssspGPUResult.vertices.count()
      println(GPUResult.vertices.take(100000).mkString("\n"))
      val endNew = System.nanoTime()

      println("-------------------------")

      println(endNew - startNew)

      PregelGPUSkipping.close(GPUResult, algorithm)
    }

     */

    //val k = StdIn.readInt()
    //println(k)

  }

  // scalastyle:on println
}
