package edu.ustc.nodb.PregelGPU.algoTemplate

import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.graphx.util.collection.shmManager.shmArrayWriter
import org.apache.spark.graphx.util.collection.shmManager.shmNamePackager.shmWriterPackager
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.util.collection.BitSet

import scala.reflect.ClassTag

abstract class lambdaShmTemplete[VD: ClassTag, ED: ClassTag, A: ClassTag] extends Serializable {

  var partitionInnerData : collection.Map[Int, (Int, Int)]

  var initSource : Array[VertexId]

  var identifier : Array[String]

  def fillPartitionInnerData(newMap: collection.Map[Int,(Int,Int)]) : Unit

  def repartition
  (g: Graph[VD, ED]): Graph[VD, ED]

  def lambda_initMessage: A

  def lambda_globalVertexMergeFunc
  (v1: VertexId, v2: VD, v3: A) : VD

  def lambda_globalReduceFunc
  (v1: A, v2: A): A

  def lambda_edgeImport
  (pid: Int, iter: Iterator[Edge[ED]])
  (iterTimes: Int,
   countOutDegree: collection.Map[VertexId, Int],
   counter: LongAccumulator):
  Unit

  def lambda_GPUExecute(pid: Int, writer: shmWriterPackager, modifiedVertexAmount: Int,
                        global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int]):
  (BitSet, Array[A], Boolean)

  def lambda_shmInit(identifier: Array[String], pid: Int):
  (Array[shmArrayWriter], shmWriterPackager)

  def lambda_shmWrite(vid: VertexId, activeness: Boolean, vertexAttr: VD,
                      writer: Array[shmArrayWriter]): Unit

  def lambda_GPUExecute_skipStep
  (pid: Int, global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int]):
  (BitSet, Array[A], Boolean)

  def lambda_GPUExecute_finalCollect
  (pid: Int, global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int]):
  (BitSet, Array[A], Boolean)

  def lambda_shutDown
  (pid: Int, iter: Iterator[(VertexId, VD)]):
  Unit

}
