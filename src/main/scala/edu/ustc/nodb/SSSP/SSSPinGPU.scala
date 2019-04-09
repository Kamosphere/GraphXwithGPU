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
  type SPMapWithActive = (Boolean, mutable.Map[VertexId, Double])

  // run the SSSP based on Pregel structure
  def run(graph: Graph[VertexId,Double],
          allSource: Broadcast[List[VertexId]],
          vertexNumbers: Long,
          parts: Int,
          maxIterations: Int = Int.MaxValue)
  : Graph[SPMapWithActive, Double] = {

    // vertexNumbers stands for the quantity of vertices in the whole graph
    // Assuming the graph is in equal distribution, preSize and preMap is used to avoid allocation arraycopy
    val preSize : Int = (vertexNumbers.toInt/parts)+((vertexNumbers.toInt/parts)>>1)
    val preMap : Int = allSource.value.length+1

    // initiate the graph
    // use the EdgePartitionDReverse so that vertices only updated in single partition
    // if modified to another partition method, ReduceByKey is needed
    val spGraph = graph.mapVertices ( (vid, attr) => {
      var partitionInit: mutable.Map[VertexId, Double] = mutable.Map()
      var ifSource = false
      for (sid <- allSource.value) {
        if (vid == sid) {
          ifSource = true
          partitionInit += (sid -> 0)
        }
        else partitionInit += (sid -> Double.PositiveInfinity)
      }
      (ifSource, partitionInit)
    }
    ).partitionBy(EdgePartition1DReverse).cache()

    var g = spGraph

    //val startNew = System.nanoTime()

    // run the first main SSSP process
    // algorithm should run at least once
    var modifiedSubgraph : RDD[(VertexId, SPMapWithActive)] = g.triplets.mapPartitionsWithIndex((pid, iter) => {

      // collect the VertexSet and EdgeSet
      val partitionVertexTemp = new util.ArrayList[VertexSet](preSize)
      val partitionEdgeTemp = new util.ArrayList[EdgeSet](preSize)

      // used to remove the abundant vertices
      val VertexNumList = new util.HashSet[Long]

      var temp : EdgeTriplet[SPMapWithActive,Double] = null
      var tempSrcVertex : VertexSet = null
      var tempDstVertex : VertexSet = null
      var tempEdge : EdgeSet = null

      while(iter.hasNext){

        temp = iter.next()

        tempEdge = new EdgeSet(temp.srcId, temp.dstId, temp.attr)
        partitionEdgeTemp.add(tempEdge)

        if(! VertexNumList.contains(temp.srcId)){
          val intoJavaHashMap = new util.HashMap[Long, Double](preMap)
          for(part <- temp.srcAttr._2){
            intoJavaHashMap.put(part._1, part._2)
          }

          tempSrcVertex = new VertexSet(temp.srcId, temp.srcAttr._1, intoJavaHashMap)
          VertexNumList.add(temp.srcId)
          partitionVertexTemp.add(tempSrcVertex)
        }

        if(! VertexNumList.contains(temp.dstId)){
          val intoJavaHashMap = new util.HashMap[Long, Double](preMap)
          for(part <- temp.dstAttr._2){
            intoJavaHashMap.put(part._1, part._2)
          }

          tempDstVertex = new VertexSet(temp.dstId, temp.dstAttr._1, intoJavaHashMap)
          VertexNumList.add(temp.dstId)
          partitionVertexTemp.add(tempDstVertex)
        }

      }

      val Process = new GPUNative
      var envInit : Boolean = false

      while(! envInit){
        envInit = Process.GPUInit(vertexNumbers.toInt,
          partitionEdgeTemp.size(), allSource, pid)
      }

      val results : ArrayBuffer[(VertexId, SPMapWithActive)] = Process.GPUProcess(
        partitionVertexTemp, partitionEdgeTemp, allSource, vertexNumbers, pid)
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
    var activeMessages = modifiedSubgraph.filter(_._2._1==true).count()

    //val endNew = System.nanoTime()
    //println("-------------------------")
    //println("~~~~~time in running"+(endNew - startNew)+"~~~~~")

    var iterTimes = 1
    var prevG : Graph[SPMapWithActive, Double] = null

    //loop
    while(activeMessages > 0 && iterTimes < maxIterations){

      prevG = g

      //combine the messages with graph
      g = g.mapVertices((vid,attr) => {
        (false, attr._2)
      }).joinVertices(modifiedSubgraph)((vid, v1, v2) => {
        val b = v2._1
        val result = (v1._2/:v2._2){
          case (map,(k,r)) => map + (k->math.min(r,map.getOrElse(k, Double.PositiveInfinity)))
        }
        (b,result)
      }).cache()

      val oldVertexModified = modifiedSubgraph
      //run the main SSSP process

      //val startNew = System.nanoTime()
      modifiedSubgraph = g.triplets.mapPartitionsWithIndex((pid, iter) => {

        // collect the VertexSet and EdgeSet
        val partitionVertexTemp = new util.ArrayList[VertexSet](preSize)
        val partitionEdgeTemp = new util.ArrayList[EdgeSet](preSize)

        // used to remove the abundant vertices
        val VertexNumList = new util.HashSet[Long]

        var temp : EdgeTriplet[SPMapWithActive,Double] = null
        var tempSrcVertex : VertexSet = null
        var tempDstVertex : VertexSet = null
        var tempEdge : EdgeSet = null

        while(iter.hasNext){

          temp = iter.next()

          tempEdge = new EdgeSet(temp.srcId, temp.dstId, temp.attr)
          partitionEdgeTemp.add(tempEdge)

          if(! VertexNumList.contains(temp.srcId)){
            val intoJavaHashMap = new util.HashMap[Long, Double](preMap)
            for(part <- temp.srcAttr._2){
              intoJavaHashMap.put(part._1, part._2)
            }

            tempSrcVertex = new VertexSet(temp.srcId, temp.srcAttr._1, intoJavaHashMap)
            VertexNumList.add(temp.srcId)
            partitionVertexTemp.add(tempSrcVertex)
          }

          if(! VertexNumList.contains(temp.dstId)){
            val intoJavaHashMap = new util.HashMap[Long, Double](preMap)
            for(part <- temp.dstAttr._2){
              intoJavaHashMap.put(part._1, part._2)
            }

            tempDstVertex = new VertexSet(temp.dstId, temp.dstAttr._1, intoJavaHashMap)
            VertexNumList.add(temp.dstId)
            partitionVertexTemp.add(tempDstVertex)
          }

        }

        val Process = new GPUNative
        val results : ArrayBuffer[(VertexId, SPMapWithActive)] = Process.GPUProcess(
          partitionVertexTemp, partitionEdgeTemp, allSource, vertexNumbers, pid)
        results.iterator
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
      activeMessages = modifiedSubgraph.filter(_._2._1==true).count()

      //val endNew = System.nanoTime()
      //println("-------------------------")
      //println("~~~~~time in running"+(endNew - startNew)+"~~~~~")

      oldVertexModified.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
    }

    // the final combine
    g = g.mapVertices((vid,attr) => {
      (false, attr._2)
    }).joinVertices(modifiedSubgraph)((vid, v1, v2) => {
      val b = v2._1
      val result = (v1._2/:v2._2){
        case (map,(k,v)) => map + (k->math.min(v,map.getOrElse(k, Double.MaxValue)))
      }
      (b,result)
    })
    modifiedSubgraph.unpersist(blocking = false)
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

