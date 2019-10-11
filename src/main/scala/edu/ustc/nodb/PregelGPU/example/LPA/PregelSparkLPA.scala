package edu.ustc.nodb.PregelGPU.example.LPA

import scala.reflect.ClassTag

import org.apache.spark.graphx._

class PregelSparkLPA [VD, ED: ClassTag](graph: Graph[VD, ED]){

  def sendMessage(e: EdgeTriplet[VertexId, ED]):
  Iterator[(VertexId, Map[VertexId, Long])] = {

    Iterator((e.srcId, Map(e.dstAttr -> 1L)), (e.dstId, Map(e.srcAttr -> 1L)))
  }

  def mergeMessage(count1: Map[VertexId, Long], count2: Map[VertexId, Long])
  : Map[VertexId, Long] = {
    (count1.keySet ++ count2.keySet).map { i =>
      val count1Val = count1.getOrElse(i, 0L)
      val count2Val = count2.getOrElse(i, 0L)
      i -> (count1Val + count2Val)
    }(collection.breakOut) // more efficient alternative to [[collection.Traversable.toMap]]
  }

  def vertexProgram(vid: VertexId, attr: Long, message: Map[VertexId, Long]):
  VertexId = {

    if (message.isEmpty) attr else message.maxBy(_._2)._1
  }

  val initialMessage = Map[VertexId, Long]()

  def run(maxSteps: Int): Graph[VertexId, ED] = {

    require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got $maxSteps")

    val lpaGraph = graph.mapVertices { case (vid, _) => vid }

    Pregel(lpaGraph, initialMessage, maxIterations = maxSteps)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
  }
}
