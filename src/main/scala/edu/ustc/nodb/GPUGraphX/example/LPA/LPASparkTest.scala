package edu.ustc.nodb.GPUGraphX.example.LPA

import edu.ustc.nodb.GPUGraphX.envControl
import edu.ustc.nodb.GPUGraphX.plugin.graphGenerator
import org.apache.spark.graphx.PartitionStrategy._
import org.apache.spark.{SparkConf, SparkContext}

object LPASparkTest{

  // scalastyle:on println

  def main(args: Array[String]) {

    // environment setting
    val conf = new SparkConf().setAppName("Pregel_LPA")
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

    // running LPA

    println("-------------------------")

    // the quantity of vertices in the whole graph
    val vertexSum = graph.vertices.count()
    val edgeSum = graph.edges.count()

    val LPATest = new PregelSparkLPA(graph)

    val startNormal = System.nanoTime()
    val LPAResult = LPATest.run(15)
    // val d = ssspResult.vertices.count()
    val endNormal = System.nanoTime()
    println(LPAResult.vertices.take(100000).mkString("\n"))

    println("-------------------------")

    println(endNormal - startNormal)

  }

  // scalastyle:on println
}

