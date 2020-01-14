package edu.ustc.nodb.GPUGraphX.algorithm.shm

import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

abstract class algoShmTemplete[VD: ClassTag, ED: ClassTag, A: ClassTag] extends Serializable
with packagerShmTemplete[VD, ED, A] with pregelShmTemplete[VD, ED, A] {

  var partitionInnerData : collection.Map[Int, (Int, Int)]

  var initSource : Array[VertexId]

  var identifier : Array[String]

  def fillPartitionInnerData(newMap: collection.Map[Int,(Int,Int)]) : Unit

  def repartition
  (g: Graph[VD, ED]): Graph[VD, ED]

}
