package edu.ustc.nodb.GPUGraphX.example.LPA

import scala.reflect.ClassTag

import org.apache.spark.graphx._

class PregelSparkLPA [VD, ED: ClassTag](graph: Graph[VD, ED]) extends Serializable {

  def sendMessage(e: EdgeTriplet[(VertexId, Long), ED]):
  Iterator[(VertexId, Map[VertexId, Long])] = {

    Iterator((e.dstId, Map(e.srcAttr._1 -> 1L)))
  }

  def mergeMessage(count1: Map[VertexId, Long], count2: Map[VertexId, Long])
  : Map[VertexId, Long] = {
    (count1.keySet ++ count2.keySet).map { i =>
      val count1Val = count1.getOrElse(i, 0L)
      val count2Val = count2.getOrElse(i, 0L)
      i -> (count1Val + count2Val)
    }(collection.breakOut) // more efficient alternative to [[collection.Traversable.toMap]]
  }

  def vertexProgram(vid: VertexId, attr: (VertexId, Long), message: Map[VertexId, Long]):
  (VertexId, Long) = {

    if (message.isEmpty) attr else message.maxBy(_._2)
  }

  val initialMessage: Map[VertexId, VertexId] = Map[VertexId, Long]()

  def run(maxSteps: Int): Graph[(VertexId, Long), ED] = {

    require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got $maxSteps")

    val lpaGraph = graph.mapVertices { case (vid, _) => (vid, 0L) }

    Pregel(lpaGraph, initialMessage, maxIterations = maxSteps, EdgeDirection.Out)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
  }

  def runInContinue(prevGraph: Graph[(VertexId, Long), ED], maxSteps: Int): Graph[(VertexId, Long), ED] = {

    require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got $maxSteps")

    val lpaGraph = prevGraph

    Pregel(lpaGraph, initialMessage, maxIterations = maxSteps, EdgeDirection.Out)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
  }
}
