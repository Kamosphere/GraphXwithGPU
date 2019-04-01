package edu.ustc.nodb.SSSP

import java.util

class EdgeSet (inSrc : Long, inDst : Long, inAttr : Double) extends Serializable {

  private[this] var _SrcId: Long = inSrc

  def SrcId(): Long = _SrcId

  def SrcId_=(value: Long): Unit = {
    _SrcId = value
  }

  private[this] var _DstId: Long = inDst

  def DstId(): Long = _DstId

  def DstId_=(value: Long): Unit = {
    _DstId = value
  }

  private[this] var _Attr: Double = inAttr

  def Attr(): Double = _Attr

  def Attr_=(value: Double): Unit = {
    _Attr = value
  }

}

class VertexSet (vid: Long, activeness: Boolean, attr: util.HashMap[Long, Double])extends Serializable {

  // another construction for VertexSet to make it convenience to add vertices attribute
  def this(vid: Long, activeness: Boolean) = this(vid, activeness, new util.HashMap[Long, Double])

  // that's it
  def addAttr(id: Long, distance : Double): Unit ={
    _Attr.put(id, distance)
  }

  private[this] var _ifActive: Boolean = activeness

  def ifActive(): Boolean = _ifActive

  def ifActive_=(value: Boolean): Unit = {
    _ifActive = value
  }

  private[this] var _VertexId: Long = vid

  def VertexId(): Long = _VertexId

  def VertexId_=(value: Long): Unit = {
    _VertexId = value
  }

  private[this] var _Attr: util.HashMap[Long, Double] = attr

  def Attr(): util.HashMap[Long, Double] = _Attr

  def Attr_=(value: util.HashMap[Long, Double]): Unit = {
    _Attr = value
  }

}