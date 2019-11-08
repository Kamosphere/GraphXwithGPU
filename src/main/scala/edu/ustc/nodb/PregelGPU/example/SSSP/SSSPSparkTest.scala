package edu.ustc.nodb.PregelGPU.example.SSSP

import edu.ustc.nodb.PregelGPU.plugin.graphGenerator
import edu.ustc.nodb.PregelGPU.envControl
import edu.ustc.nodb.PregelGPU.plugin.partitionStrategy.EdgePartitionNumHookedTest
import org.apache.spark.graphx.GraphXUtils
import org.apache.spark.graphx.PartitionStrategy.{EdgePartition2D, RandomVertexCut}
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

    // running SSSP

    println("-------------------------")

    val sourceList = ArrayBuffer(1L, preDefinedGraphVertices * 1 + 2L,
      preDefinedGraphVertices * 2 + 4L,
      preDefinedGraphVertices * 3 + 7L).distinct.sorted

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

  }

  // scalastyle:on println
}

