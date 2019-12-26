package edu.ustc.nodb.PregelGPU.example.SSSP

import edu.ustc.nodb.PregelGPU.plugin.graphGenerator
import edu.ustc.nodb.PregelGPU.envControl
import edu.ustc.nodb.PregelGPU.plugin.partitionStrategy._
import org.apache.spark.graphx.PartitionStrategy._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object SSSPSparkTest{

  // scalastyle:on println

  def main(args: Array[String]) {

    // environment setting
    val conf = new SparkConf().setAppName("Pregel_SSSP")
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

    val sourceList = ArrayBuffer[Long]()

    sourceFile = envControl.datasetType match {
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
        sourceList += (1101295L, 2117363L,
          3430267L,
          4202807L)
        "testGraph_soc-LiveJournal1.txt.txt"
      case 4 =>
        sourceList += (337151L, 649031L,
          1041859L,
          1400923L)
        "testGraph_wiki-topcats.txt.txt"

      // for skipping
      /*
      case 3 =>
        sourceList += (101295L, 1117363L,
          2030267L,
          3202807L)
        "soc-relabel-graph.txt"
      case 4 =>
        sourceList += (377335L, 584383L,
          1101295L,
          1400923L)
        "wiki-relabel-graph.txt"
      */
    }


    if(envControl.controller == 0) {
      conf.set("fs.defaultFS", "hdfs://192.168.1.2:9000")
      sourceFile = "hdfs://192.168.1.2:9000/sourcegraph/" + sourceFile
    }

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

    val ssspTest = new PregelSparkSSSP(graph, allSourceList)

    val startNormal = System.nanoTime()
    val ssspResult = ssspTest.run()
    // val d = ssspResult.vertices.count()
    val endNormal = System.nanoTime()
    println(ssspResult.vertices.take(100000).mkString("\n"))

    println("-------------------------")

    println(endNormal - startNormal)

    //val k = StdIn.readInt()
    //println(k)
  }

  // scalastyle:on println
}

