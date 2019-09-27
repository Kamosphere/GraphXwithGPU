package edu.ustc.nodb.PregelGPU.algorithm.SSSP

import edu.ustc.nodb.PregelGPU.algorithm.SPMap
import edu.ustc.nodb.PregelGPU.plugin.partitionStrategy.EdgePartitionPreSearch
import edu.ustc.nodb.PregelGPU.template.lambdaTemplete
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class pregel_SSSP(allSource: Broadcast[ArrayBuffer[VertexId]],
                  vertexSum: Long,
                  edgeSum: Long,
                  parts: Int) extends lambdaTemplete[SPMap, Double, SPMap] {

  var controller: GPUController = _

  override var partitionInnerData : collection.Map[Int,(Int,Int)] = _

  override def fillPartitionInnerData(newMap: collection.Map[Int,(Int,Int)]) : Unit = {
    partitionInnerData = newMap
  }

  override def repartition
  (g: Graph[SPMap, Double]): Graph[SPMap, Double] = {

    val partitionMethod = new EdgePartitionPreSearch(g, allSource.value)
    partitionMethod.generateMappedGraph()
  }

  override def lambda_initGraph
  (vid: VertexId, attr: VertexId): SPMap = {

    var partitionInit: mutable.LinkedHashMap[VertexId, Double] = mutable.LinkedHashMap()
    for (sid <- allSource.value.sorted) {
      if (vid == sid) {
        partitionInit += (sid -> 0)
      }
      else {
        partitionInit += (sid -> Double.PositiveInfinity)
      }
    }
    partitionInit
  }

  override def lambda_globalVertexFunc
  (v1: VertexId, v2: SPMap, v3: SPMap) : SPMap = {
    val result : mutable.LinkedHashMap[Long, Double] = v2 ++ v3.map{
      case (k, r) => k -> math.min(r, v2.getOrElse(k, Double.PositiveInfinity))
    }
    result
  }

  override def lambda_globalReduceFunc
  (v1: SPMap, v2: SPMap): SPMap = {
    val result : mutable.LinkedHashMap[Long, Double] = v1 ++ v2.map{
      case (k, r) => k -> math.min(r, v1.getOrElse(k, Double.PositiveInfinity))
    }
    result
  }

  override def lambda_edgeImport
  (pid: Int, iter: Iterator[Edge[Double]])
  (iterTimes: Int,
   countOutDegree: collection.Map[VertexId, Int],
   counter: LongAccumulator):
  Unit = {

    val startTimeA = System.nanoTime()

    val sourceList = allSource.value

    // pre allocate size
    val preParameter = partitionInnerData.get(pid)
    val preVertexLength = preParameter.get._1
    val preEdgeLength = preParameter.get._2

    // edge data array
    val pEdgeSrcIDTemp = new Array[Long](preEdgeLength)
    val pEdgeDstIDTemp = new Array[Long](preEdgeLength)
    val pEdgeAttrTemp = new Array[Double](preEdgeLength)
    // used to remove the abundant vertices and record outDegree
    val VertexNumList = new mutable.HashMap[Long, Int]

    var temp : Edge[Double] = null

    var EdgeCount = 0

    while(iter.hasNext) {
      temp = iter.next()

      pEdgeSrcIDTemp(EdgeCount) = temp.srcId
      pEdgeDstIDTemp(EdgeCount) = temp.dstId
      pEdgeAttrTemp(EdgeCount) = temp.attr
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
      if (countOutDegree.getOrElse(part._1, -1) == part._2) {
        filteredVertex. += (part._1)
      }
    }

    val endTimeA = System.nanoTime()

    val startTimeB = System.nanoTime()

    controller = new GPUController(vertexSum, preVertexLength, EdgeCount, sourceList, pid)

    controller.GPUEnvEdgeInit(filteredVertex.toArray,
      pEdgeSrcIDTemp, pEdgeDstIDTemp, pEdgeAttrTemp)

    val endTimeB = System.nanoTime()

    println("In iter " + iterTimes + " of part" + pid + ", Collecting data time: "
      + (endTimeA - startTimeA) + " Processing time: "
      + (endTimeB - startTimeB))

  }

  override def lambda_GPUExecute
  (pid: Int, idArr: Array[VertexId], activeArr: Array[Boolean], attrArr: Array[SPMap]):
  (Array[VertexId], Array[SPMap], Boolean) = {

    val vertexCount = partitionInnerData(pid)._1
    val edgeCount = partitionInnerData(pid)._2

    val sourceArray = allSource.value
    controller = new GPUController(vertexSum, vertexCount, edgeCount, sourceArray, pid)

    val mapSize = sourceArray.length

    val vertexID = new Array[VertexId](vertexCount)
    val vertexActive = new Array[Boolean](vertexCount)
    val vertexAttr = new Array[Double](vertexCount * mapSize)

    var i = 0
    for(index <- vertexID.indices){
      if(idArr(index) == -1){
      }
      else{
        vertexID(i) = idArr(index)
        vertexActive(i) = activeArr(index)
        for(mapIndex <- allSource.value.indices){
          vertexAttr(i * mapSize + mapIndex) = attrArr(index)(sourceArray(mapIndex))
        }
        i += 1
      }
    }

    controller.GPUMsgExecute(vertexID, vertexActive, vertexAttr, vertexCount)

  }

  override def lambda_GPUExecute_skipStep
  (pid: Int): (Array[VertexId], Array[SPMap], Boolean) = {

    val vertexCount = partitionInnerData(pid)._1
    val edgeCount = partitionInnerData(pid)._2

    val sourceArray = allSource.value
    controller = new GPUController(vertexSum, vertexCount, edgeCount, sourceArray, pid)

    controller.GPUIterSkipCollect(vertexCount)
  }

  override def lambda_GPUExecute_finalCollect
  (pid: Int): (Array[VertexId], Array[SPMap], Boolean) = {

    val vertexCount = partitionInnerData(pid)._1
    val edgeCount = partitionInnerData(pid)._2

    val sourceArray = allSource.value
    controller = new GPUController(vertexSum, vertexCount, edgeCount, sourceArray, pid)

    controller.GPUFinalCollect(vertexCount)
  }

  override def lambda_shutDown
  (pid: Int, iter: Iterator[(VertexId, SPMap)]):
  Unit = {

    val Process = new GPUController(pid)
    var envInit : Boolean = false

    while(! envInit) {
      envInit = Process.GPUShutdown()
    }
  }
}
