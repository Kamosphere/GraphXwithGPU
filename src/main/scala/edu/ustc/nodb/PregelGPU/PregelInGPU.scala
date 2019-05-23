package edu.ustc.nodb.PregelGPU

import java.util

import edu.ustc.nodb.PregelGPU.Plugin.GraphXModified
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.PartitionStrategy.EdgePartition1D
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

object PregelInGPU{

  // Define the vertex attribute in GPU-based SSSP project
  // Boolean stands for the activeness of Vertex
  // mutable.LinkedHashMap stored the pairs of nearest distance from landmark in order
  type SPMapWithActive = (Boolean, mutable.LinkedHashMap[VertexId, Double])

  // vertexNumbers stands for the quantity of vertices in the whole graph
  def run(graph: Graph[VertexId, Double],
          allSource: Broadcast[ArrayBuffer[VertexId]],
          vertexNumbers: Long,
          edgeNumbers: Long,
          parts: Int,
          maxIterations: Int = Int.MaxValue)
  : Graph[SPMapWithActive, Double] = {

    // initiate the graph
    // use the EdgePartition1DReverse so that vertices only updated in single partition
    // if changed to other partition method, ***ReduceByKey is needed***
    val spGraph = graph.mapVertices ((vid, attr) => {
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
    }).partitionBy(EdgePartition1D).cache()

    var g = spGraph

    val countOutDegree = g.outDegrees.collectAsMap()

    val sc = org.apache.spark.SparkContext.getOrCreate()

    val ifFilteredCounter = sc.longAccumulator("ifFilteredCounter")

    var beforeCounter = ifFilteredCounter.value

    var partitionSplit : collection.Map[Int,(Int, Int)] = null
    var modifiedSubGraph: RDD[(VertexId, SPMapWithActive)] = null
    //var tuples = (partitionSplit, modifiedSubGraph)
    val (partitionSplitTemp, modifiedSubGraphTemp) = partitionSplitAndPreIter(g, allSource, vertexNumbers, countOutDegree, ifFilteredCounter)
    // get the vertex number and edge number in every partition in order to avoid allocation arraycopy

    partitionSplit = partitionSplitTemp
    modifiedSubGraph = modifiedSubGraphTemp

    // combine the vertex messages through partitions
    var vertexModified = modifiedSubGraph.reduceByKey((v1, v2) =>
      lambdaReduceByKey(v1, v2)).cache()

    // get the amount of active vertices
    var activeMessages = vertexModified.count()

    var afterCounter = ifFilteredCounter.value

    var iterTimes = 1
    var prevG : Graph[SPMapWithActive, Double] = null

    val ifRepartition = false

    //loop
    while(activeMessages > 0 && iterTimes < maxIterations){

      val startTime = System.nanoTime()
      prevG = g
      val oldVertexModified = vertexModified
      ifFilteredCounter.reset()
      val tempBeforeCounter = ifFilteredCounter.sum

      if(ifRepartition){
        g = GraphXModified.joinVerticesDefault(g, vertexModified)((vid, v1, v2) =>
          lambdaJoinVerticesDefault(vid, v1, v2))(vAttr =>
          (false,vAttr._2))
          .cache()

        val (partitionSplitTemp, modifiedSubGraphTemp) = partitionSplitAndPreIter(g, allSource, vertexNumbers, countOutDegree, ifFilteredCounter)

        partitionSplit = partitionSplitTemp
        modifiedSubGraph = modifiedSubGraphTemp
      }
      else{
        if(afterCounter != beforeCounter){
          //combine the messages with graph
          g = GraphXModified.joinVerticesDefault(g, vertexModified)((vid, v1, v2) =>
            lambdaJoinVerticesDefault(vid, v1, v2))(vAttr =>
            (false,vAttr._2))
            .cache()

          //run the main SSSP process
          modifiedSubGraph = IterationWhileCombine(g, iterTimes, allSource, vertexNumbers, partitionSplit, ifFilteredCounter)

        }
        // skip merging to graph
        else{
          modifiedSubGraph = vertexModified.mapPartitionsWithIndex((pid, iter) =>{
            val preParameter = partitionSplit.get(pid)
            val preVertexLength = preParameter.get._1
            val preEdgeLength = preParameter.get._2
            val sourceList = allSource.value

            val Process = new GPUNative
            val (results, needCombine) : (ArrayBuffer[(VertexId, SPMapWithActive)],Boolean) = Process.GPUSkippedProcess(
              vertexNumbers, preVertexLength, preEdgeLength, sourceList, pid)
            if(needCombine){
              ifFilteredCounter.add(1)
            }
            val result = results.iterator
            result
          }).cache()
        }
      }

      // combine the vertex messages through partitions
      vertexModified = modifiedSubGraph.reduceByKey((v1, v2)=>
        lambdaReduceByKey(v1, v2)).cache()

      iterTimes = iterTimes + 1

      // get the amount of active vertices
      activeMessages = modifiedSubGraph.count()

      /*
      something while need to repartition
       */

      val endTime = System.nanoTime()
      //val endNew = System.nanoTime()
      //println("-------------------------")
      println("Whole iteration time: " + (endTime - startTime))

      oldVertexModified.unpersist(blocking = false)
      if(afterCounter != beforeCounter){
        prevG.unpersistVertices(blocking = false)
        prevG.edges.unpersist(blocking = false)
      }
      beforeCounter = tempBeforeCounter
      afterCounter = ifFilteredCounter.sum
    }

    g = GraphXModified.joinVerticesDefault(g, vertexModified)((vid, v1, v2) =>
      lambdaJoinVerticesDefault(vid, v1, v2))(vAttr =>
      (false,vAttr._2))
      .cache()

    // extract the remained data
    modifiedSubGraph = vertexModified.mapPartitionsWithIndex((pid, iter) =>{

      val preParameter = partitionSplit.get(pid)
      val preVertexLength = preParameter.get._1
      val preEdgeLength = preParameter.get._2
      val sourceList = allSource.value

      val Process = new GPUNative
      val results : ArrayBuffer[(VertexId, SPMapWithActive)] = Process.GPUAllProcess(
        vertexNumbers, preVertexLength, preEdgeLength, sourceList, pid)
      val result = results.iterator
      result
    })

    vertexModified = modifiedSubGraph.reduceByKey((v1, v2)=>
      lambdaReduceByKey(v1, v2)).cache()

    // the final combine
    g = GraphXModified.joinVerticesDefault(g, vertexModified)((vid, v1, v2) =>
      lambdaJoinVerticesDefault(vid, v1, v2))(vAttr =>
      (false,vAttr._2))
      .cache()
    vertexModified.unpersist(blocking = false)
    g
  }

  def lambdaJoinVerticesDefault(vid: VertexId,
                                v1: SPMapWithActive,
                                v2: SPMapWithActive):
  SPMapWithActive ={

    val b = v2._1
    val result : mutable.LinkedHashMap[Long, Double] = v1._2++v2._2.map{
      case (k,r) => k->math.min(r,v1._2.getOrElse(k, Double.PositiveInfinity))
    }
    (b,result)

  }

  def lambdaReduceByKey(v1: SPMapWithActive,
                        v2: SPMapWithActive):
  SPMapWithActive = {

    val b = v1._1 | v2._1
    val result : mutable.LinkedHashMap[Long, Double] = v1._2++v2._2.map{
      case (k,r) => k->math.min(r,v1._2.getOrElse(k, Double.PositiveInfinity))
    }
    (b,result)

  }

  def partitionSplitAndPreIter(g: Graph[SPMapWithActive, Double],
                               allSource: Broadcast[ArrayBuffer[VertexId]],
                               vertexNumbers: Long,
                               countOutDegree: collection.Map[VertexId, Int],
                               counter: LongAccumulator):
  (collection.Map[Int,(Int, Int)], RDD[(VertexId, SPMapWithActive)]) = {

    // get the vertex number and edge number in every partition in order to avoid allocation arraycopy
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

    def lambda_ModifiedSubGraph_MPBI_afterPartition(pid: Int,
                                                    iter: Iterator[EdgeTriplet[SPMapWithActive,Double]]):
    Iterator[(VertexId, SPMapWithActive)] = {

      val startTimeA = System.nanoTime()

      val sourceList = allSource.value
      val preMap = sourceList.length
      // pre allocate size
      val preParameter = partitionSplit.get(pid)
      val preVertexLength = preParameter.get._1
      val preEdgeLength = preParameter.get._2
      // vertex data and edge data array
      val pVertexIDTemp = new Array[Long](preVertexLength)
      val pVertexActiveTemp = new Array[Boolean](preVertexLength)
      val pVertexAttrTemp = new Array[Double](preVertexLength * preMap)
      val pEdgeSrcIDTemp = new Array[Long](preEdgeLength)
      val pEdgeDstIDTemp = new Array[Long](preEdgeLength)
      val pEdgeAttrTemp = new Array[Double](preEdgeLength)
      // used to remove the abundant vertices and record outDegree
      val VertexNumList = new mutable.HashMap[Long, Int]

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
          VertexNumList.put(temp.srcId, 1)
          // need to guard the order of sourceList in array
          var index = 0
          for(part <- temp.srcAttr._2.values){
            pVertexAttrTemp(VertexIndex * preMap + index) = part
            index = index + 1
          }
          VertexIndex = VertexIndex + 1
        }
        else {
          val countTracker = VertexNumList.getOrElse(temp.srcId, 0)
          VertexNumList.update(temp.srcId, countTracker + 1)
        }

        if(! VertexNumList.contains(temp.dstId)){
          pVertexIDTemp(VertexIndex)=temp.dstId
          pVertexActiveTemp(VertexIndex)=temp.dstAttr._1
          VertexNumList.put(temp.dstId, 0)
          // need to guard the order of sourceList in array
          var index = 0
          for(part <- temp.dstAttr._2.values){
            pVertexAttrTemp(VertexIndex * preMap + index) = part
            index = index + 1
          }
          VertexIndex = VertexIndex + 1
        }
      }

      val filteredVertex = new ArrayBuffer[Long]
      for(part <- VertexNumList){
        if(countOutDegree.getOrElse(part._1, -1) == part._2){
          filteredVertex.+=(part._1)
        }
      }

      val endTimeA = System.nanoTime()

      val startTimeB = System.nanoTime()

      val Process = new GPUNative
      var envInit : Boolean = false
      // loop until server started
      while(! envInit){
        envInit = Process.GPUInit(vertexNumbers.toInt,filteredVertex.toArray,
          pEdgeSrcIDTemp, pEdgeDstIDTemp, pEdgeAttrTemp, sourceList, pid)
      }
      val (results, needCombine) : (ArrayBuffer[(VertexId, SPMapWithActive)], Boolean) = Process.GPUProcess(
        pVertexIDTemp, pVertexActiveTemp, pVertexAttrTemp, vertexNumbers, VertexIndex, pEdgeSrcIDTemp.length, sourceList, pid)
      val result = results.iterator
      if(needCombine){
        counter.add(1)
      }

      val endTimeB = System.nanoTime()

      println("In iter 0 of part" + pid + ", Collecting data time: " + (endTimeA - startTimeA) + " Processing time: " + (endTimeB - startTimeB))
      result
    }

    // run the first main SSSP process
    // algorithm should run at least once
    val modifiedSubGraph : RDD[(VertexId, SPMapWithActive)] = g.triplets.mapPartitionsWithIndex((pid, iter) =>
      lambda_ModifiedSubGraph_MPBI_afterPartition(pid, iter)).cache()

    (partitionSplit, modifiedSubGraph)
  }

  def IterationWhileCombine(g: Graph[SPMapWithActive, Double],
                            iterTimes:Int,
                            allSource: Broadcast[ArrayBuffer[VertexId]],
                            vertexNumbers: Long,
                            partitionSplit: collection.Map[Int,(Int, Int)],
                            counter: LongAccumulator):
  RDD[(VertexId, SPMapWithActive)] ={

    def lambda_ModifiedSubGraph_MPBI_IterWithoutPartition(pid: Int,
                                                          iter: Iterator[EdgeTriplet[SPMapWithActive,Double]]):
    Iterator[(VertexId, SPMapWithActive)] = {

      val startTimeA = System.nanoTime()

      val sourceList = allSource.value
      val preMap = sourceList.length
      // pre allocate size
      val preParameter = partitionSplit.get(pid)
      val preVertexLength = preParameter.get._1
      val preEdgeLength = preParameter.get._2
      // vertex data array
      val pVertexIDTemp = new Array[Long](preVertexLength)
      val pVertexActiveTemp = new Array[Boolean](preVertexLength)
      val pVertexAttrTemp = new Array[Double](preVertexLength * preMap)
      // used to remove the abundant vertices
      val VertexNumList = new util.HashSet[Long]

      var temp : EdgeTriplet[SPMapWithActive,Double] = null
      var VertexIndex = 0
      var EdgeIndex = 0

      while(iter.hasNext){
        temp = iter.next()
        if(temp.srcAttr._1){
          EdgeIndex = EdgeIndex + 1

          if(! VertexNumList.contains(temp.srcId)){
            pVertexIDTemp(VertexIndex)=temp.srcId
            pVertexActiveTemp(VertexIndex)=temp.srcAttr._1
            VertexNumList.add(temp.srcId)
            // need to guard the order of sourceList in array
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
            // need to guard the order of sourceList in array
            var index = 0
            for(part <- temp.dstAttr._2.values){
              pVertexAttrTemp(VertexIndex * preMap + index) = part
              index = index + 1
            }
            VertexIndex = VertexIndex + 1
          }
        }
      }
      val endTimeA = System.nanoTime()

      val startTimeB = System.nanoTime()

      val Process = new GPUNative
      val (results, needCombine) : (ArrayBuffer[(VertexId, SPMapWithActive)], Boolean) = Process.GPUProcess(
        pVertexIDTemp, pVertexActiveTemp, pVertexAttrTemp, vertexNumbers, VertexIndex, preEdgeLength, sourceList, pid)
      if(needCombine){
        counter.add(1)
      }
      val result = results.iterator

      val endTimeB = System.nanoTime()

      println("In iter "+ iterTimes + " of part" + pid + ", Collecting data time: " + (endTimeA - startTimeA) + " Processing time: " + (endTimeB - startTimeB))
      result
    }

    val modifiedSubGraph = g.triplets.mapPartitionsWithIndex((pid, iter)
    => lambda_ModifiedSubGraph_MPBI_IterWithoutPartition(pid, iter)).cache()

    modifiedSubGraph
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

