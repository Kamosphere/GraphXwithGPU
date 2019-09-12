package edu.ustc.nodb.PregelGPU.algorithm.SSSP

import java.util

import edu.ustc.nodb.PregelGPU.algorithm.{SPMap, lambdaTemplate}
import edu.ustc.nodb.PregelGPU.plugin.partitionStrategy.EdgePartitionPreSearch
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class pregelSSSP (allSource: Broadcast[ArrayBuffer[VertexId]],
                  vertexSum: Long,
                  edgeSum: Long,
                  parts: Int) extends lambdaTemplate[SPMap, Double]{

  // scalastyle:off println

  override def repartition
  (g: Graph[(Boolean, SPMap), Double]):
  Graph[(Boolean, SPMap), Double] = {

    val partitionMethod = new EdgePartitionPreSearch(g, allSource.value)
    partitionMethod.generateMappedGraph()
  }

  override def lambda_initGraph
  (vid: VertexId, attr: VertexId):
  (Boolean, SPMap) = {

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

  override def lambda_JoinVerticesDefault
  (vid: VertexId,
   v1: (Boolean, SPMap),
   v2: (Boolean, SPMap)):
  (Boolean, SPMap) = {

    val b = v2._1
    val result : mutable.LinkedHashMap[Long, Double] = v1._2++v2._2.map{
      case (k, r) => k -> math.min(r, v1._2.getOrElse(k, Double.PositiveInfinity))
    }
    (b, result)
  }

  override def lambda_ReduceByKey
  (v1: (Boolean, SPMap),
   v2: (Boolean, SPMap)):
  (Boolean, SPMap) = {

    val b = v1._1 | v2._1
    val result : mutable.LinkedHashMap[Long, Double] = v1._2++v2._2.map{
      case (k, r) => k -> math.min(r, v1._2.getOrElse(k, Double.PositiveInfinity))
    }
    (b, result)

  }

  override def lambda_partitionSplit
  (pid: Int,
   iter: Iterator[EdgeTriplet[(Boolean, SPMap), Double]]):
  Iterator[(Int, (Int, Int))] = {

    var EdgeNum = 0
    var VertexNum = 0
    val VertexNumList = new util.HashSet[Long]
    var temp : EdgeTriplet[(Boolean, SPMap), Double] = null

    while(iter.hasNext) {
      temp = iter.next()
      EdgeNum = EdgeNum + 1
      if(! VertexNumList.contains(temp.srcId)) {
        VertexNumList.add(temp.srcId)
        VertexNum = VertexNum + 1
      }
      if(! VertexNumList.contains(temp.dstId)) {
        VertexNumList.add(temp.dstId)
        VertexNum = VertexNum + 1
      }
    }

    Iterator((pid, (VertexNum, EdgeNum)))

  }

  override def lambda_ModifiedSubGraph_repartitionIter
  (pid: Int,
   iter: Iterator[EdgeTriplet[(Boolean, SPMap), Double]])
  (iterTimes: Int,
   countOutDegree: collection.Map[VertexId, Int],
   partitionSplit: collection.Map[Int, (Int, Int)],
   counter: LongAccumulator):
  Iterator[(VertexId, (Boolean, SPMap))] = {

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

    var temp : EdgeTriplet[(Boolean, SPMap), Double] = null
    var VertexCount = 0
    var EdgeCount = 0

    while(iter.hasNext) {
      temp = iter.next()

      pEdgeSrcIDTemp(EdgeCount) = temp.srcId
      pEdgeDstIDTemp(EdgeCount) = temp.dstId
      pEdgeAttrTemp(EdgeCount) = temp.attr
      EdgeCount = EdgeCount + 1

      if(! VertexNumList.contains(temp.srcId)) {
        pVertexIDTemp(VertexCount) = temp.srcId
        pVertexActiveTemp(VertexCount) = temp.srcAttr._1
        VertexNumList.put(temp.srcId, 1)
        // the order of sourceList in array is guarded by linkedHashMap
        var index = 0
        for(part <- temp.srcAttr._2.values) {
          pVertexAttrTemp(VertexCount * preMap + index) = part
          index = index + 1
        }
        VertexCount = VertexCount + 1
      }
      else {
        val countTracker = VertexNumList.getOrElse(temp.srcId, 0)
        VertexNumList.update(temp.srcId, countTracker + 1)
      }

      if(! VertexNumList.contains(temp.dstId)) {
        pVertexIDTemp(VertexCount) = temp.dstId
        pVertexActiveTemp(VertexCount) = temp.dstAttr._1
        VertexNumList.put(temp.dstId, 0)
        // the order of sourceList in array is guarded by linkedHashMap
        var index = 0
        for(part <- temp.dstAttr._2.values) {
          pVertexAttrTemp(VertexCount * preMap + index) = part
          index = index + 1
        }
        VertexCount = VertexCount + 1
      }
    }

    val filteredVertex = new ArrayBuffer[Long]
    for (part <- VertexNumList) {
      if (countOutDegree.getOrElse(part._1, -1) == part._2) {
        filteredVertex. += (part._1)
      }
    }

    val endTimeA = System.nanoTime()

    val startTimeB = System.nanoTime()

    val Process = new GPUController(vertexSum, EdgeCount, sourceList, pid)
    Process.GPUEnvEdgeInit(filteredVertex.toArray, EdgeCount,
      pEdgeSrcIDTemp, pEdgeDstIDTemp, pEdgeAttrTemp)

    val (results, needCombine) = Process.GPUMsgExecute(
      pVertexIDTemp, pVertexActiveTemp, pVertexAttrTemp, VertexCount)
    val result = results.iterator
    if(needCombine) {
      counter.add(1)
    }

    val endTimeB = System.nanoTime()

    println("In iter 0 of part" + pid + ", Collecting data time: "
      + (endTimeA - startTimeA) + " Processing time: "
      + (endTimeB - startTimeB))
    result
  }

  override def lambda_ModifiedSubGraph_normalIter
  (pid: Int,
   iter: Iterator[EdgeTriplet[(Boolean, SPMap), Double]])
  (iterTimes: Int,
   partitionSplit: collection.Map[Int, (Int, Int)],
   counter: LongAccumulator):
  Iterator[(VertexId, (Boolean, SPMap))] = {

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
    val VertexNumList = new util.HashSet[Long](preVertexLength)

    var temp : EdgeTriplet[(Boolean, SPMap), Double] = null
    var VertexCount = 0
    var EdgeCount = 0

    while(iter.hasNext) {
      temp = iter.next()

      if(temp.srcAttr._1) {
        EdgeCount = EdgeCount + 1

        if(! VertexNumList.contains(temp.srcId)) {
          pVertexIDTemp(VertexCount) = temp.srcId
          pVertexActiveTemp(VertexCount) = temp.srcAttr._1
          VertexNumList.add(temp.srcId)
          // the order of sourceList in array is guarded by linkedHashMap
          var index = 0
          for(part <- temp.srcAttr._2.values) {
            pVertexAttrTemp(VertexCount * preMap + index) = part
            index = index + 1
          }
          VertexCount = VertexCount + 1
        }
        if(! VertexNumList.contains(temp.dstId)) {
          pVertexIDTemp(VertexCount) = temp.dstId
          pVertexActiveTemp(VertexCount) = temp.dstAttr._1
          VertexNumList.add(temp.dstId)
          // the order of sourceList in array is guarded by linkedHashMap
          var index = 0
          for(part <- temp.dstAttr._2.values) {
            pVertexAttrTemp(VertexCount * preMap + index) = part
            index = index + 1
          }
          VertexCount = VertexCount + 1
        }
      }
    }
    val endTimeA = System.nanoTime()

    val startTimeB = System.nanoTime()

    val Process = new GPUController(vertexSum, preEdgeLength, sourceList, pid)
    val (results, needCombine) = Process.GPUMsgExecute(
      pVertexIDTemp, pVertexActiveTemp, pVertexAttrTemp, VertexCount)
    if(needCombine) {
      counter.add(1)
    }

    val result = results.iterator

    val endTimeB = System.nanoTime()

    println("In iter " + iterTimes + " of part" + pid + ", Collecting data time: "
      + (endTimeA - startTimeA) + " Processing time: "
      + (endTimeB - startTimeB))
    result
  }

  override def lambda_modifiedSubGraph_skipStep
  (pid: Int,
   iter: Iterator[EdgeTriplet[(Boolean, SPMap), Double]])
  (iterTimes: Int,
   partitionSplit: collection.Map[Int, (Int, Int)],
   counter: LongAccumulator):
  Iterator[(VertexId, (Boolean, SPMap))] = {

    val preParameter = partitionSplit.get(pid)
    val preVertexLength = preParameter.get._1
    val preEdgeLength = preParameter.get._2
    val sourceList = allSource.value

    val Process = new GPUController(vertexSum, preEdgeLength, sourceList, pid)
    val (results, needCombine) = Process.GPUIterSkipCollect(preVertexLength)
    if (needCombine) {
      counter.add(1)
    }
    val result = results.iterator
    result
  }

  override def lambda_modifiedSubGraph_collectAll
  (pid: Int,
   iter: Iterator[EdgeTriplet[(Boolean, SPMap), Double]])
  (iterTimes: Int,
   partitionSplit: collection.Map[Int, (Int, Int)],
   counter: LongAccumulator):
  Iterator[(VertexId, (Boolean, SPMap))] = {

    val preParameter = partitionSplit.get(pid)
    val preVertexLength = preParameter.get._1
    val preEdgeLength = preParameter.get._2
    val sourceList = allSource.value

    val Process = new GPUController(vertexSum, preEdgeLength, sourceList, pid)
    val results = Process.GPUFinalCollect(preVertexLength)
    val result = results.iterator
    result
  }

  override def lambda_shutDown
  (pid: Int,
   iter: Iterator[(VertexId, (Boolean, SPMap))]):
  Unit = {

    val Process = new GPUController(pid)
    var envInit : Boolean = false

    while(! envInit) {
      envInit = Process.GPUShutdown()
    }
  }

  // scalastyle:on println
}
