package edu.ustc.nodb.SSSP

import java.util

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

object SSSPinGPU{

  // Define the vertex attribute in GPU-based SSSP project
  // Boolean stands for the activeness of Vertex
  // mutable.map stored the pairs of nearest distance from landmark
  type SPMapWithActive = (Boolean, mutable.LinkedHashMap[VertexId, Double])

  // run the SSSP based on Pregel structure
  def run(graph: Graph[VertexId, Double],
          allSource: Broadcast[ArrayBuffer[VertexId]],
          vertexNumbers: Long,
          edgeNumbers: Long,
          parts: Int,
          maxIterations: Int = Int.MaxValue)
  : Graph[SPMapWithActive, Double] = {

    // vertexNumbers stands for the quantity of vertices in the whole graph
    // Assuming the graph is in equal distribution, preSize and preMap is used to avoid allocation arraycopy
    val preMap : Int = allSource.value.length

    // initiate the graph
    // use the EdgePartitionDReverse so that vertices only updated in single partition
    // if modified to another partition method, ReduceByKey is needed
    val spGraph = graph.partitionBy(EdgePartition1DReverse)
    .mapVertices ( (vid, attr) => {
      var partitionInit: mutable.LinkedHashMap[VertexId, Double] = mutable.LinkedHashMap()
      var ifSource = false
      for (sid <- allSource.value.sorted) {
        if (vid == sid) {
          ifSource = true
          partitionInit += (sid -> 0)
        }
        else {
          partitionInit += (sid -> Double.PositiveInfinity)
        }

      }
      (ifSource, partitionInit)
    }
    ).cache()

    var g = spGraph

    val partitionSplit = g.triplets.mapPartitionsWithIndex((pid, iter) => {
      var EdgeNum = 0
      var VertexNum = 0
      val VertexNumList = new util.HashSet[Long]
      var temp : EdgeTriplet[SPMapWithActive,Double] = null

      while(iter.hasNext){
        temp = iter.next()
        EdgeNum = EdgeNum + 1
        if(! VertexNumList.contains(temp.srcId)){
          VertexNumList.add(temp.srcId)
          VertexNum = VertexNum + 1
        }
        if(! VertexNumList.contains(temp.dstId)){
          VertexNumList.add(temp.dstId)
          VertexNum = VertexNum + 1
        }
      }
      Iterator((pid, (VertexNum, EdgeNum)))
    }).collectAsMap()

    // run the first main SSSP process
    // algorithm should run at least once
    var modifiedSubGraph : RDD[(VertexId, SPMapWithActive)] = g.triplets.mapPartitionsWithIndex((pid, iter) => {

      // collect the VertexSet and EdgeSet
      //val partitionVertexTemp = new util.ArrayList[VertexSet](preVertexSize)
      val preParameter = partitionSplit.get(pid)
      val preVertexLength = preParameter.get._1
      val preEdgeLength = preParameter.get._2

      val pVertexIDTemp = new Array[Long](preVertexLength)
      val pVertexActiveTemp = new Array[Boolean](preVertexLength)
      val pVertexAttrTemp = new Array[Double](preVertexLength * preMap)

      val pEdgeSrcIDTemp = new Array[Long](preEdgeLength)
      val pEdgeDstIDTemp = new Array[Long](preEdgeLength)
      val pEdgeAttrTemp = new Array[Double](preEdgeLength)

      //val partitionEdgeTemp = new util.ArrayList[EdgeSet](preEdgeSize)
      val sourceList = allSource.value

      // used to remove the abundant vertices
      val VertexNumList = new util.HashSet[Long]

      var temp : EdgeTriplet[SPMapWithActive,Double] = null
      var VertexIndex = 0
      var EdgeIndex = 0

      while(iter.hasNext){

        temp = iter.next()

        pEdgeSrcIDTemp(EdgeIndex)=temp.srcId
        pEdgeDstIDTemp(EdgeIndex)=temp.dstId
        pEdgeAttrTemp(EdgeIndex)=temp.attr

        EdgeIndex = EdgeIndex + 1

        if(! VertexNumList.contains(temp.srcId)){
          pVertexIDTemp(VertexIndex)=temp.srcId
          pVertexActiveTemp(VertexIndex)=temp.srcAttr._1
          VertexNumList.add(temp.srcId)

          // guard the order of sourceList in array
          var index = 0
          for(part <- temp.srcAttr._2.values){
            pVertexAttrTemp(VertexIndex * preMap + index) = part
            index = index + 1
          }
          VertexIndex = VertexIndex + 1
        }
        if(! VertexNumList.contains(temp.dstId)){
          pVertexIDTemp(VertexIndex)=temp.dstId
          pVertexActiveTemp(VertexIndex)=temp.dstAttr._1
          VertexNumList.add(temp.dstId)

          // guard the order of sourceList in array
          var index = 0
          for(part <- temp.dstAttr._2.values){
            pVertexAttrTemp(VertexIndex * preMap + index) = part
            index = index + 1
          }
          VertexIndex = VertexIndex + 1
        }

      }

      val Process = new GPUNative
      var envInit : Boolean = false

      // loop until server started
      while(! envInit){
        envInit = Process.GPUInit(vertexNumbers.toInt,
          pEdgeSrcIDTemp, pEdgeDstIDTemp, pEdgeAttrTemp, sourceList, pid)
      }

      val results : ArrayBuffer[(VertexId, SPMapWithActive)] = Process.GPUProcess(
        pVertexIDTemp, pVertexActiveTemp, pVertexAttrTemp, vertexNumbers, pEdgeSrcIDTemp.length, sourceList, pid)
      results.iterator

    }).cache()

    /*
    // combine the vertex messages through partitions
    var vertexModified = modifiedSubgraph.reduceByKey((v1,v2)=>{
      val b = v1._1 | v2._1
      val result = (v1._2/:v2._2){
        case (map,(k,v)) => map + (k->math.min(v,map.getOrElse(k, Double.PositiveInfinity)))
      }
      (b,result)
    }).cache()
    */

    // get the amount of active vertices
    var activeMessages = modifiedSubGraph.count()

    //val endNew = System.nanoTime()
    //println("-------------------------")
    //println("~~~~~time in running"+(endNew - startNew)+"~~~~~")

    var iterTimes = 1
    var prevG : Graph[SPMapWithActive, Double] = null

    //loop
    while(activeMessages > 0 && iterTimes < maxIterations){

      val startTime = System.nanoTime()
      prevG = g

      //combine the messages with graph
      g = g.mapVertices((vid,attr) => {
        (false, attr._2)
      }).joinVertices(modifiedSubGraph)((vid, v1, v2) => {
        val b = v2._1
        val result : mutable.LinkedHashMap[Long, Double] = v1._2++v2._2.map{
          case (k,r) => k->math.min(r,v1._2.getOrElse(k, Double.PositiveInfinity))
        }
        (b,result)
      }).cache()

      val oldVertexModified = modifiedSubGraph
      //run the main SSSP process

      modifiedSubGraph = g.triplets.mapPartitionsWithIndex((pid, iter) => {

        val startTimeA = System.nanoTime()

        val preParameter = partitionSplit.get(pid)
        val preVertexLength = preParameter.get._1

        // collect the VertexSet
        val pVertexIDTemp = new Array[Long](preVertexLength)
        val pVertexActiveTemp = new Array[Boolean](preVertexLength)
        val pVertexAttrTemp = new Array[Double](preVertexLength * preMap)
        val sourceList = allSource.value

        // used to remove the abundant vertices
        val VertexNumList = new util.HashSet[Long]

        var temp : EdgeTriplet[SPMapWithActive,Double] = null
        var VertexIndex = 0
        var EdgeIndex = 0

        while(iter.hasNext){

          temp = iter.next()

          EdgeIndex = EdgeIndex + 1

          if(! VertexNumList.contains(temp.srcId)){
            pVertexIDTemp(VertexIndex)=temp.srcId
            pVertexActiveTemp(VertexIndex)=temp.srcAttr._1
            VertexNumList.add(temp.srcId)

            // guard the order of sourceList in array
            var index = 0
            for(part <- temp.srcAttr._2.values){
              pVertexAttrTemp(VertexIndex * preMap + index) = part
              index = index + 1
            }
            VertexIndex = VertexIndex + 1
          }
          if(! VertexNumList.contains(temp.dstId)){
            pVertexIDTemp(VertexIndex)=temp.dstId
            pVertexActiveTemp(VertexIndex)=temp.dstAttr._1
            VertexNumList.add(temp.dstId)

            // guard the order of sourceList in array
            var index = 0
            for(part <- temp.dstAttr._2.values){
              pVertexAttrTemp(VertexIndex * preMap + index) = part
              index = index + 1
            }
            VertexIndex = VertexIndex + 1
          }

        }

        val endTimeA = System.nanoTime()

        val startTimeB = System.nanoTime()

        val Process = new GPUNative
        val results : ArrayBuffer[(VertexId, SPMapWithActive)] = Process.GPUProcess(
          pVertexIDTemp, pVertexActiveTemp, pVertexAttrTemp, vertexNumbers, EdgeIndex, sourceList, pid)


        val result = results.iterator
        val endTimeB = System.nanoTime()

        println((endTimeA - startTimeA) + " with time of " + (endTimeB - startTimeB))

        result
      }).cache()

      /*
      // combine the vertex messages through partitions
      vertexModified = modifiedSubgraph.reduceByKey((v1,v2)=>{
        val b = v1._1 | v2._1 ;
        val result = (v1._2/:v2._2){
          case (map,(k,v)) => map + (k->math.min(v,map.getOrElse(k, Double.MaxValue)))
        }
        (b,result)
      }).cache()
      */

      iterTimes = iterTimes + 1

      // get the amount of active vertices
      activeMessages = modifiedSubGraph.count()

      val endTime = System.nanoTime()
      //val endNew = System.nanoTime()
      //println("-------------------------")
      println("~~~~~time in running"+(endTime - startTime)+"~~~~~")

      oldVertexModified.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
    }

    // the final combine
    g = g.mapVertices((vid,attr) => {
      (false, attr._2)
    }).joinVertices(modifiedSubGraph)((vid, v1, v2) => {
      val b = v2._1
      val result : mutable.LinkedHashMap[Long, Double] = v1._2++v2._2.map{
        case (k,r) => k->math.min(r,v1._2.getOrElse(k, Double.PositiveInfinity))
      }
      (b,result)
    })
    modifiedSubGraph.unpersist(blocking = false)
    g
  }

  // after running algorithm, close the server
  def close(Graph: Graph[SPMapWithActive, Double]):Unit = {
    Graph.vertices.foreachPartition(g=>{
      val Process = new GPUNative
      var envInit : Boolean = false
      val pid = TaskContext.getPartitionId()
      while(! envInit){
        envInit = Process.GPUShutdown(pid)
      }
    })
  }
}

