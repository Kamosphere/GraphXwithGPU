package edu.ustc.nodb.GPUGraphX.plugin

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.internal.Logging

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class partitionCostCalculator[VD: ClassTag, ED: ClassTag] (graph: Graph[VD, ED]) extends Serializable with Logging{

  val countOutDegree: collection.Map[VertexId, Int] = graph.outDegrees.collectAsMap()

  val innerPartition: collection.Map[Int, (Int, Int)] = graph.innerVerticesEdgesCount().collectAsMap()

  def filteredVertexSearch
  (pid: Int, iter: Iterator[Edge[ED]])
  (countDegree: collection.Map[VertexId, Int]):
  Iterator[(Int, Array[VertexId])] = {

    // used to remove the abundant vertices and record outDegree
    val VertexNumList = new mutable.HashMap[Long, Int]

    var temp : Edge[ED] = null

    var EdgeCount = 0

    while(iter.hasNext) {
      temp = iter.next()

      EdgeCount = EdgeCount + 1

      // Out Degree count in partition
      if(! VertexNumList.contains(temp.srcId)) {
        VertexNumList.put(temp.srcId, 1)
      }
      else {
        val countTracker = VertexNumList.getOrElse(temp.srcId, 0)
        VertexNumList.update(temp.srcId, countTracker + 1)
      }

      if(! VertexNumList.contains(temp.dstId)) {
        VertexNumList.put(temp.dstId, 0)
      }

    }

    // Detect if a vertex could satisfy the skip condition
    val filteredVertex = new ArrayBuffer[Long]
    for (part <- VertexNumList) {
      if (countDegree.getOrElse(part._1, 0) == part._2) {
        filteredVertex. += (part._1)
      }
    }

    Iterator((pid, filteredVertex.toArray))
  }

  def cutEdgeSearch
  (pid: Int, iter: Iterator[EdgeTriplet[Int, ED]]):
  Iterator[(Int, Int)] = {

    // used to remove the abundant vertices and record outDegree
    // val CutEdgeList = new ArrayBuffer[Edge[ED]]
    var CutEdgeCount = 0

    var temp : EdgeTriplet[Int, ED] = null

    while(iter.hasNext) {
      temp = iter.next()
      if (temp.dstAttr == pid) {
        CutEdgeCount += 1
      }
    }

    Iterator((pid, CutEdgeCount))
  }

  def runCalculator():Unit = {

    graph.persist()

    val filteredVertex = graph.edges.
      mapPartitionsWithIndex((pid, iter) => filteredVertexSearch(pid, iter)(countOutDegree))
      .collectAsMap()

    val vertexFilterMap = new mutable.HashMap[VertexId, Int]
    for (elem <- filteredVertex) {
      for (key <- elem._2) {
        vertexFilterMap.+=((key, elem._1))
      }
    }

    val sc = SparkContext.getOrCreate()
    val updateRDD = sc.parallelize(vertexFilterMap.toSeq)

    val newGraph = graph.outerJoinVertices(updateRDD)((vid, v1, v2) => v2.getOrElse(-1))

    val edgeList = newGraph.triplets.mapPartitionsWithIndex((pid, iter) => cutEdgeSearch(pid, iter))
      .collectAsMap()
    /*
    newGraph.triplets.foreachPartition(iter => {
      val pid = TaskContext.getPartitionId()
      var temp : EdgeTriplet[Int, ED]  = null

      val writer = new PrintWriter(new File("/home/liqi/IdeaProjects/GraphXwithGPU/logWRN/" +
        "testNewEdgeLog_pid" + pid + ".txt"))
      while(iter.hasNext){
        temp = iter.next()
        var chars = ""
        chars = chars + " " + temp.srcId + " : " + temp.srcAttr
        chars = chars + " -> " + temp.dstId + " : " + temp.dstAttr
        chars = chars + " Edge attr: " + temp.attr
        writer.write(chars + '\n')

      }
      writer.close()
    })
*/
    println(innerPartition)
    for (elem <- filteredVertex) {
      val pid = elem._1
      val partEdgeList = edgeList.getOrElse(pid, 0)

      logInfo("In partition " + pid + " , filteredVertex amount: " + elem._2.length)
      println("In partition " + pid + " , filteredVertex amount: " + elem._2.length)

      logInfo("In partition " + pid + " , not cut edge amount: " + partEdgeList)
      println("In partition " + pid + " , not cut edge amount: " + partEdgeList)
    }

  }
}
