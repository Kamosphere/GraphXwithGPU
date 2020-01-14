package edu.ustc.nodb.GPUGraphX.algorithm.array

import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

abstract class algoTemplete[VD: ClassTag, ED: ClassTag, A: ClassTag] extends Serializable
with packagerTemplete[VD, ED, A] with pregelTemplete[VD, ED, A] with skipStepTemplete[VD, ED, A] {

  var partitionInnerData : collection.Map[Int, (Int, Int)]

  var initSource : Array[VertexId]

  def fillPartitionInnerData(newMap: collection.Map[Int,(Int,Int)]) : Unit

  def repartition
  (g: Graph[VD, ED]): Graph[VD, ED]

}
