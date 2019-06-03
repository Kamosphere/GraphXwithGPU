package edu.ustc.nodb.PregelGPU.Plugin.partitionStrategy

import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object NeighborCollect {

  type VMap = Map[VertexId, Int]

  def vProg(vid: VertexId, vdata: VMap, message: VMap)
  : Map[VertexId, Int] = addMaps(vdata, message)

  def sendMsg(e: EdgeTriplet[VMap, _]): Iterator[(VertexId, Map[VertexId, Int])] = {
    val srcMap = (e.dstAttr.keySet -- e.srcAttr.keySet).map { k => k -> (e.dstAttr(k) - 1) }.toMap
    val dstMap = (e.srcAttr.keySet -- e.dstAttr.keySet).map { k => k -> (e.srcAttr(k) - 1) }.toMap
    if (srcMap.isEmpty && dstMap.isEmpty)
      Iterator.empty
    else
      Iterator((e.dstId, dstMap), (e.srcId, srcMap))
  }

  def addMaps(spmap1: VMap, spmap2: VMap): VMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
    }.toMap

  def run[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED], jumps: Int, landMark: ArrayBuffer[VertexId]): collection.Map[VertexId, Map[VertexId, Int]] = {
    val newG = g.mapVertices((vid, _) => {
      if(landMark.contains(vid)) Map[VertexId, Int](vid -> jumps)
      else Map[VertexId, Int]()
    })
      .pregel(Map[VertexId, Int](), jumps, EdgeDirection.Out)(vProg, sendMsg, addMaps)

    val JumpFriends = newG.vertices
      .mapValues(_.filter(_._2 == 0)).filter( x => x._2 != Map()).collectAsMap()

    JumpFriends
  }

}
