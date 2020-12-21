package edu.ustc.nodb.GPUGraphX.plugin

import edu.ustc.nodb.GPUGraphX.envControl
import org.apache.spark.{SparkConf, SparkContext}

object partitionCost {

  def main(args: Array[String]): Unit = {

    // environment setting
    val conf = new SparkConf().setAppName("partitionCost")
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

    // load graph from file
    //var sourceFile = "testGraph4000000.txt"
    //var sourceFile = "testGraph_road-road-usa.mtx.txt"
    //var sourceFile = "testGraph_com-orkut.ungraph.txt.txt"
    //var sourceFile = "testGraph_soc-LiveJournal1.txt.txt"
    var sourceFile = "wiki-relabel-washed.txt"
    if(envControl.controller == 0) {
      conf.set("fs.defaultFS", "hdfs://192.168.1.2:9000")
      sourceFile = "hdfs://192.168.1.2:9000/sourcegraph/" + sourceFile
    }

    //envControl.skippingPartSize = 1000000

    val graph = graphGenerator.readFile(sc, sourceFile)(parts.get)

    /*
    val graphwash = graph.groupEdges((e1, e2) => e1)

    graphwash.triplets.foreachPartition(iter => {
      val pid = TaskContext.getPartitionId()
      var temp : EdgeTriplet[Long, Double]  = null

      val writer = new PrintWriter(new File("/home/liqi/IdeaProjects/GraphXwithGPU/washWikiGraph/" +
        "EdgeLog_pid" + pid + ".txt"))
      while(iter.hasNext){
        temp = iter.next()
        var chars = ""
        chars = chars + temp.srcId + " "
        chars = chars + temp.dstId + " "
        chars = chars + temp.attr
        writer.write(chars + '\n')

      }
      writer.close()
    })

*/

    //println(graph.outDegrees.collect().mkString("\n"))


    val calculator = new partitionCostCalculator(graph)
    calculator.runCalculator()



  }
}
