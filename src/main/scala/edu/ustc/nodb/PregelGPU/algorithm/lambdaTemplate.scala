package edu.ustc.nodb.PregelGPU.algorithm

import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}
import org.apache.spark.util.LongAccumulator

trait lambdaTemplate[VD, ED] extends Serializable {

  def repartition
  (g: Graph[(Boolean, VD), ED]):
  Graph[(Boolean, VD), ED]

  def lambda_initGraph
  (v1: VertexId,
   v2: VertexId):
  (Boolean, VD)

  def lambda_JoinVerticesDefault
  (vid: VertexId,
   v1: (Boolean, VD),
   v2: (Boolean, VD)):
  (Boolean, VD)

  def lambda_ReduceByKey
  (v1: (Boolean, VD),
   v2: (Boolean, VD)):
  (Boolean, VD)

  def lambda_partitionSplit
  (pid: Int,
   iter: Iterator[EdgeTriplet[(Boolean, VD), ED]]):
  Iterator[(Int, (Int, Int))]

  def lambda_ModifiedSubGraph_repartitionIter
  (pid: Int, iter: Iterator[EdgeTriplet[(Boolean, VD), ED]])
  (iterTimes: Int,
   countOutDegree: collection.Map[VertexId, Int],
   partitionSplit: collection.Map[Int, (Int, Int)],
   counter: LongAccumulator):
  Iterator[(VertexId, (Boolean, VD))]

  def lambda_ModifiedSubGraph_normalIter
  (pid: Int, iter: Iterator[EdgeTriplet[(Boolean, VD), ED]])
  (iterTimes: Int,
   partitionSplit: collection.Map[Int, (Int, Int)],
   counter: LongAccumulator):
  Iterator[(VertexId, (Boolean, VD))]

  def lambda_modifiedSubGraph_skipStep
  (pid: Int, iter: Iterator[EdgeTriplet[(Boolean, VD), ED]])
  (iterTimes: Int,
   partitionSplit: collection.Map[Int, (Int, Int)],
   counter: LongAccumulator):
  Iterator[(VertexId, (Boolean, VD))]

  def lambda_modifiedSubGraph_collectAll
  (pid: Int, iter: Iterator[EdgeTriplet[(Boolean, VD), ED]])
  (iterTimes: Int,
   partitionSplit: collection.Map[Int, (Int, Int)],
   counter: LongAccumulator):
  Iterator[(VertexId, (Boolean, VD))]

  def lambda_shutDown
  (pid: Int,
   iter: Iterator[(VertexId, (Boolean, VD))]):
  Unit

}
