package edu.ustc.nodb.PregelGPU.algorithm.SSSP

import edu.ustc.nodb.PregelGPU.algorithm.SPMap

import scala.collection.mutable

class VertexSet (vid: Long,
                 activeness: Boolean,
                 attr: mutable.LinkedHashMap[Long, Double])
  extends Serializable {

  // another construction for VertexSet
  // make it convenience to add vertices attribute
  def this(vid: Long, activeness: Boolean)
  = this(vid, activeness, new mutable.LinkedHashMap[Long, Double]())

  // that's it
  def addAttr(id: Long, distance : Double):
  Unit = {
    attr.put(id, distance)
  }

  // pair of data structure, used in JNI
  def TupleReturn():
  (Long, (Boolean, SPMap)) = {
    (vid, (activeness, attr))
  }

}
