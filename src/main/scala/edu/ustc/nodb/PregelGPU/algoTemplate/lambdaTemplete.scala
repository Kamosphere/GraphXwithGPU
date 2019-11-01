package edu.ustc.nodb.PregelGPU.algoTemplate

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.util.LongAccumulator

import scala.reflect.ClassTag

abstract class lambdaTemplete[VD: ClassTag, ED: ClassTag, A: ClassTag] extends Serializable {

  var partitionInnerData : collection.Map[Int, (Int, Int)]

  var initSource : Array[VertexId]

  def fillPartitionInnerData(newMap: collection.Map[Int,(Int,Int)]) : Unit

  def repartition
  (g: Graph[VD, ED]): Graph[VD, ED]

  def lambda_initGraph
  (v1: VertexId, v2: VertexId): VD

  def lambda_initMessage
  (v1: VertexId): A

  def lambda_globalVertexFunc
  (v1: VertexId, v2: VD, v3: A) : VD

  def lambda_globalReduceFunc
  (v1: A, v2: A): A

  def lambda_edgeImport
  (pid: Int, iter: Iterator[Edge[ED]])
  (iterTimes: Int,
   countOutDegree: collection.Map[VertexId, Int],
   counter: LongAccumulator):
  Unit

  // Old init

  def lambda_GPUExecute
  (pid: Int, idArr: Array[VertexId], activeArr: Array[Boolean], vertexAttr: Array[VD]):
  (Array[VertexId], Array[A], Boolean)

  def lambda_GPUExecute_skipStep
  (pid: Int): (Array[VertexId], Array[A], Boolean)

  def lambda_GPUExecute_finalCollect
  (pid: Int): (Array[VertexId], Array[A], Boolean)

  def lambda_shutDown
  (pid: Int, iter: Iterator[(VertexId, VD)]):
  Unit

  // New init

  def lambda_VertexIntoGPU
  (pid: Int, idArr: Array[VertexId], activeArr: Array[Boolean], vertexAttr: Array[VD]):
  (Boolean, Int)

  def lambda_getMessages
  (pid: Int): (Array[VertexId], Array[A])

  def lambda_getOldMessages
  (pid: Int): (Array[VertexId], Array[Boolean], Array[Int], Array[A])

  def lambda_SkipVertexIntoGPU
  (pid: Int, iterTimes: Int): (Boolean, Int)

  def lambda_globalOldMsgReduceFunc
  (v1: (Boolean, Int, A), v2: (Boolean, Int, A)): (Boolean, Int, A)

}
