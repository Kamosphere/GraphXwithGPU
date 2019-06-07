package edu.ustc.nodb.PregelGPU.Algorithm.SSSP

import java.util

import edu.ustc.nodb.PregelGPU.Algorithm.{SPMapWithActive, lambdaTemplete}
import edu.ustc.nodb.PregelGPU.Plugin.partitionStrategy.EdgePartitionPreSearch
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.PartitionStrategy.EdgePartition1D
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class pregelSSSP (allSource: Broadcast[ArrayBuffer[VertexId]],
                  vertexSum: Long,
                  edgeSum: Long,
                  parts: Int) extends lambdaTemplete[SPMapWithActive, Double]{

  override def repartition(g: Graph[SPMapWithActive, Double]): Graph[SPMapWithActive, Double] = {

    val partitionMethod = new EdgePartitionPreSearch(g, allSource.value)
    val afterG = partitionMethod.generateMappedGraph()
    println(partitionMethod.landMarkVertexIndexed)
    println(partitionMethod.landMarkPartitionID)
    afterG.triplets.foreachPartition(v => {
      val pid = TaskContext.getPartitionId()
      var count = 0
      while(v.hasNext){
        val temp = v.next()
        count = count + 1
        println(temp.srcId + " to " + temp.dstId + " is in " + pid.toString)
      }
      println(count + " for after graph")
    })
    afterG
  }

  override def lambda_initGraph(vid: VertexId, attr: VertexId): SPMapWithActive = {

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

  override def lambda_JoinVerticesDefaultFirst(vid: VertexId,
                                 v1: SPMapWithActive,
                                 v2: SPMapWithActive):
  SPMapWithActive = {

    val b = v2._1
    val result : mutable.LinkedHashMap[Long, Double] = v1._2++v2._2.map{
      case (k,r) => k->math.min(r,v1._2.getOrElse(k, Double.PositiveInfinity))
    }
    (b,result)

  }

  override def lambda_JoinVerticesDefaultSecond(v1: SPMapWithActive):
  SPMapWithActive = (false,v1._2)

  override def lambda_ReduceByKey(v1: SPMapWithActive,
                                  v2: SPMapWithActive):
  SPMapWithActive = {

    val b = v1._1 | v2._1
    val result : mutable.LinkedHashMap[Long, Double] = v1._2++v2._2.map{
      case (k,r) => k->math.min(r,v1._2.getOrElse(k, Double.PositiveInfinity))
    }
    (b,result)

  }

  override def lambda_partitionSplit(pid: Int,
                                     iter: Iterator[EdgeTriplet[SPMapWithActive,Double]]):
  Iterator[(Int, (Int, Int))] = {

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

  }

  override def lambda_ModifiedSubGraph_MPBI_afterPartition(pid: Int,
                                                           iter: Iterator[EdgeTriplet[SPMapWithActive,Double]])
                                                          (iterTimes:Int,
                                                           countOutDegree: collection.Map[VertexId, Int],
                                                           partitionSplit: collection.Map[Int,(Int, Int)],
                                                           counter: LongAccumulator):
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
    var VertexCount = 0
    var EdgeCount = 0

    while(iter.hasNext){
      temp = iter.next()
      pEdgeSrcIDTemp(EdgeCount)=temp.srcId
      pEdgeDstIDTemp(EdgeCount)=temp.dstId
      pEdgeAttrTemp(EdgeCount)=temp.attr
      EdgeCount = EdgeCount + 1

      if(! VertexNumList.contains(temp.srcId)){
        pVertexIDTemp(VertexCount)=temp.srcId
        pVertexActiveTemp(VertexCount)=temp.srcAttr._1
        VertexNumList.put(temp.srcId, 1)
        // need to guard the order of sourceList in array
        var index = 0
        for(part <- temp.srcAttr._2.values){
          pVertexAttrTemp(VertexCount * preMap + index) = part
          index = index + 1
        }
        VertexCount = VertexCount + 1
      }
      else {
        val countTracker = VertexNumList.getOrElse(temp.srcId, 0)
        VertexNumList.update(temp.srcId, countTracker + 1)
      }

      if(! VertexNumList.contains(temp.dstId)){
        pVertexIDTemp(VertexCount)=temp.dstId
        pVertexActiveTemp(VertexCount)=temp.dstAttr._1
        VertexNumList.put(temp.dstId, 0)
        // need to guard the order of sourceList in array
        var index = 0
        for(part <- temp.dstAttr._2.values){
          pVertexAttrTemp(VertexCount * preMap + index) = part
          index = index + 1
        }
        VertexCount = VertexCount + 1
      }
    }

    val filteredVertex = new ArrayBuffer[Long]
    for(part <- VertexNumList){
      if(countOutDegree.getOrElse(part._1, -1) == part._2){
        filteredVertex.+=(part._1)
      }
    }

    println(filteredVertex)

    val endTimeA = System.nanoTime()

    val startTimeB = System.nanoTime()

    val Process = new GPUNative
    var envInit : Boolean = false
    // loop until server started
    while(! envInit){
      envInit = Process.GPUInit(vertexSum.toInt, filteredVertex.toArray,
        pEdgeSrcIDTemp, pEdgeDstIDTemp, pEdgeAttrTemp, sourceList, pid)
    }
    val (results, needCombine) : (ArrayBuffer[(VertexId, SPMapWithActive)], Boolean) = Process.GPUProcess(
      pVertexIDTemp, pVertexActiveTemp, pVertexAttrTemp, vertexSum, VertexCount, pEdgeSrcIDTemp.length, sourceList, pid)
    val result = results.iterator
    if(needCombine){
      counter.add(1)
    }

    val endTimeB = System.nanoTime()

    println("In iter 0 of part" + pid + ", Collecting data time: " + (endTimeA - startTimeA) + " Processing time: " + (endTimeB - startTimeB)
      + needCombine + " " + results)
    result
  }

  override def lambda_ModifiedSubGraph_MPBI_IterWithoutPartition(pid: Int,
                                                                 iter: Iterator[EdgeTriplet[SPMapWithActive,Double]])
                                                                (iterTimes:Int,
                                                                 partitionSplit: collection.Map[Int,(Int, Int)],
                                                                 counter: LongAccumulator):
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
      pVertexIDTemp, pVertexActiveTemp, pVertexAttrTemp, vertexSum, VertexIndex, preEdgeLength, sourceList, pid)
    if(needCombine){
      counter.add(1)
    }

    val result = results.iterator

    val endTimeB = System.nanoTime()

    println("In iter "+ iterTimes + " of part" + pid + ", Collecting data time: " + (endTimeA - startTimeA) + " Processing time: " + (endTimeB - startTimeB)
     + needCombine + " " + results)
    result
  }

  override def lambda_ModifiedSubGraph_MPBI_skipStep(pid: Int,
                                                     iter: Iterator[EdgeTriplet[SPMapWithActive, Double]])
                                                    (iterTimes:Int,
                                                     partitionSplit: collection.Map[Int,(Int, Int)],
                                                     counter: LongAccumulator):
  Iterator[(VertexId, SPMapWithActive)] = {

    val preParameter = partitionSplit.get(pid)
    val preVertexLength = preParameter.get._1
    val preEdgeLength = preParameter.get._2
    val sourceList = allSource.value

    val Process = new GPUNative
    val (results, needCombine): (ArrayBuffer[(VertexId, SPMapWithActive)], Boolean) = Process.GPUSkippedProcess(
      vertexSum, preVertexLength, preEdgeLength, sourceList, pid)
    if (needCombine) {
      counter.add(1)
    }
    val result = results.iterator
    result
  }

  override def lambda_ModifiedSubGraph_MPBI_All(pid: Int,
                                                iter: Iterator[EdgeTriplet[SPMapWithActive, Double]])
                                               (iterTimes:Int,
                                                partitionSplit: collection.Map[Int,(Int, Int)],
                                                counter: LongAccumulator):
  Iterator[(VertexId, SPMapWithActive)] = {

    val preParameter = partitionSplit.get(pid)
    val preVertexLength = preParameter.get._1
    val preEdgeLength = preParameter.get._2
    val sourceList = allSource.value

    val Process = new GPUNative
    val results : ArrayBuffer[(VertexId, SPMapWithActive)] = Process.GPUAllProcess(
      vertexSum, preVertexLength, preEdgeLength, sourceList, pid)
    val result = results.iterator
    result
  }

}
