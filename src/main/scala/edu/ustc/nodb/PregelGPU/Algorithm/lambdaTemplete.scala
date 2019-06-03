package edu.ustc.nodb.PregelGPU.Algorithm

import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}
import org.apache.spark.util.LongAccumulator

trait lambdaTemplete[VD, ED] extends Serializable {

  def repartition(g: Graph[VD, ED]) : Graph[VD, ED]

  def lambda_initGraph(v1: VertexId, v2: VertexId) : VD

  def lambda_JoinVerticesDefaultFirst(vid: VertexId, v1: VD, v2: VD): VD

  def lambda_JoinVerticesDefaultSecond(v1: VD): VD

  def lambda_ReduceByKey(v1: VD,
                         v2: VD): VD

  def lambda_partitionSplit(pid: Int,
                            iter: Iterator[EdgeTriplet[VD, ED]]):
  Iterator[(Int, (Int, Int))]

  def lambda_ModifiedSubGraph_MPBI_afterPartition
  (pid: Int, iter: Iterator[EdgeTriplet[VD, ED]])
  (iterTimes: Int,
   countOutDegree: collection.Map[VertexId, Int],
   partitionSplit: collection.Map[Int,(Int, Int)],
   counter: LongAccumulator):
  Iterator[(VertexId, VD)]

  def lambda_ModifiedSubGraph_MPBI_IterWithoutPartition
  (pid: Int, iter: Iterator[EdgeTriplet[VD, ED]])
  (iterTimes: Int,
   partitionSplit: collection.Map[Int,(Int, Int)],
   counter: LongAccumulator):
  Iterator[(VertexId, VD)]

  def lambda_ModifiedSubGraph_MPBI_skipStep
  (pid: Int, iter: Iterator[EdgeTriplet[VD, ED]])
  (iterTimes: Int,
   partitionSplit: collection.Map[Int,(Int, Int)],
   counter: LongAccumulator):
  Iterator[(VertexId, VD)]

  def lambda_ModifiedSubGraph_MPBI_All
  (pid: Int, iter: Iterator[EdgeTriplet[VD, ED]])
  (iterTimes:Int,
   partitionSplit: collection.Map[Int,(Int, Int)],
   counter: LongAccumulator):
  Iterator[(VertexId, VD)]

}
