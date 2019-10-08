package edu.ustc.nodb.PregelGPU.algorithm.SSSPshm

import edu.ustc.nodb.PregelGPU.algorithm.SPMap
import edu.ustc.nodb.PregelGPU.plugin.partitionStrategy.EdgePartitionPreSearch
import edu.ustc.nodb.PregelGPU.template.lambdaShmTemplete
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.graphx.util.collection.shmManager.shmArrayWriter
import org.apache.spark.graphx.util.collection.shmManager.shmArrayWriterImpl.{shmArrayWriterBoolean, shmArrayWriterDouble, shmArrayWriterLong}
import org.apache.spark.graphx.util.collection.shmManager.shmNamePackager.shmWriterPackager
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.util.collection.BitSet

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class pregel_SSSPShm(allSource: Broadcast[ArrayBuffer[VertexId]],
                     shmIdentifier: Array[String],
                     vertexSum: Long,
                     edgeSum: Long,
                     parts: Int) extends lambdaShmTemplete[SPMap, Double, SPMap] {

  var controller: GPUControllerShm = _

  private def makeMap(x: (VertexId, Double)*) = Map(x: _*)

  override var initSource: Array[VertexId] = allSource.value.toArray

  override var partitionInnerData : collection.Map[Int, (Int, Int)] = _

  override var identifier: Array[String] = shmIdentifier

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

    var partitionInit = makeMap()
    for (sid <- allSource.value.sorted) {
      if (vid == sid) {
        partitionInit += (sid -> 0)
      }
    }
    partitionInit
  }

  override def lambda_initMessage(v1: VertexId): SPMap = {
    if (initSource.contains(v1)) {
      makeMap(v1 -> 0)
    }
    else makeMap()
  }

  override def lambda_globalVertexFunc
  (v1: VertexId, v2: SPMap, v3: SPMap) : SPMap = {
    (v2.keySet ++ v3.keySet).map {
      k => k -> math.min(v2.getOrElse(k, Double.MaxValue), v3.getOrElse(k, Double.MaxValue))
    }(collection.breakOut)
  }

  override def lambda_globalReduceFunc
  (v1: SPMap, v2: SPMap): SPMap = {
    (v1.keySet ++ v2.keySet).map {
      k => k -> math.min(v1.getOrElse(k, Double.MaxValue), v2.getOrElse(k, Double.MaxValue))
    }(collection.breakOut)
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
    val pEdgeSrcIDShm = new shmArrayWriterLong(pid, preEdgeLength, "EdgeSrc")
    val pEdgeDstIDShm = new shmArrayWriterLong(pid, preEdgeLength, "EdgeDst")
    val pEdgeAttrShm = new shmArrayWriterDouble(pid, preEdgeLength, "EdgeAttr")
    // used to remove the abundant vertices and record outDegree
    val VertexNumList = new mutable.HashMap[Long, Int]

    var temp : Edge[Double] = null

    var EdgeCount = 0

    while(iter.hasNext) {
      temp = iter.next()

      pEdgeSrcIDShm.shmArrayWriterSet(temp.srcId)
      pEdgeDstIDShm.shmArrayWriterSet(temp.dstId)
      pEdgeAttrShm.shmArrayWriterSet(temp.attr)
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

    controller = new GPUControllerShm(vertexSum, preVertexLength, EdgeCount, sourceList, pid)

    controller.GPUEnvEdgeInit(filteredVertex.toArray,
      pEdgeSrcIDShm.shmWriterClose(),
      pEdgeDstIDShm.shmWriterClose(),
      pEdgeAttrShm.shmWriterClose())

    val endTimeB = System.nanoTime()

    println("In iter " + iterTimes + " of part" + pid + ", Collecting data time: "
      + (endTimeA - startTimeA) + " Processing time: "
      + (endTimeB - startTimeB))

  }

  override def lambda_GPUExecute
  (pid: Int, writer: shmWriterPackager, modifiedVertexAmount: Int, global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int]):
  (BitSet, Array[SPMap], Boolean) = {

    val vertexCount = partitionInnerData(pid)._1
    val edgeCount = partitionInnerData(pid)._2

    val sourceArray = allSource.value
    controller = new GPUControllerShm(vertexSum, vertexCount, edgeCount, sourceArray, pid)

    val vertexIdIdentifier = writer.getNameByIndex(0)
    val vertexActiveIdentifier = writer.getNameByIndex(1)
    val vertexAttrIdentifier = writer.getNameByIndex(2)

    controller.GPUMsgExecute(vertexIdIdentifier, vertexActiveIdentifier, vertexAttrIdentifier,
      modifiedVertexAmount, global2local)

  }

  override def lambda_shmInit(ident: Array[String], pid: Int):
  (Array[shmArrayWriter], shmWriterPackager) = {

    val vertexCount = partitionInnerData(pid)._1
    val preMap = allSource.value.length

    val t = new Array[shmArrayWriter](3)
    t(0) = new shmArrayWriterLong(pid, vertexCount, ident(0))
    t(1) = new shmArrayWriterBoolean(pid, vertexCount, ident(1))
    t(2) = new shmArrayWriterDouble(pid, vertexCount * preMap, ident(2))

    val d = new shmWriterPackager(3)
    d.addName(t(0).shmName, vertexCount)
    d.addName(t(1).shmName, vertexCount)
    d.addName(t(2).shmName, vertexCount * preMap)

    (t, d)
  }

  override def lambda_shmWrite(vid: VertexId, activeness: Boolean, vertexAttr: SPMap, writer: Array[shmArrayWriter])
  : Unit = {

    writer(0) match {
      case longWriter: shmArrayWriterLong => longWriter.shmArrayWriterSet(vid)
    }
    writer(1) match {
      case booleanWriter: shmArrayWriterBoolean => booleanWriter.shmArrayWriterSet(activeness)
    }
    writer(2) match {
      case doubleWriter: shmArrayWriterDouble => {
        for (elem <- allSource.value){
          val attr = vertexAttr.getOrElse(elem, Int.MaxValue.toDouble)
          doubleWriter.shmArrayWriterSet(attr)
        }
      }
    }

  }

  override def lambda_GPUExecute_skipStep
  (pid: Int, global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int]):
  (BitSet, Array[SPMap], Boolean) = {

    val vertexCount = partitionInnerData(pid)._1
    val edgeCount = partitionInnerData(pid)._2

    val sourceArray = allSource.value
    controller = new GPUControllerShm(vertexSum, vertexCount, edgeCount, sourceArray, pid)

    controller.GPUIterSkipCollect(global2local)
  }

  override def lambda_GPUExecute_finalCollect
  (pid: Int, global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int]):
  (BitSet, Array[SPMap], Boolean) = {

    val vertexCount = partitionInnerData(pid)._1
    val edgeCount = partitionInnerData(pid)._2

    val sourceArray = allSource.value
    controller = new GPUControllerShm(vertexSum, vertexCount, edgeCount, sourceArray, pid)

    controller.GPUFinalCollect(global2local)
  }

  override def lambda_shutDown
  (pid: Int, iter: Iterator[(VertexId, SPMap)]):
  Unit = {

    val Process = new GPUControllerShm(pid)
    var envInit : Boolean = false

    while(! envInit) {
      envInit = Process.GPUShutdown(1)
    }
  }
}
